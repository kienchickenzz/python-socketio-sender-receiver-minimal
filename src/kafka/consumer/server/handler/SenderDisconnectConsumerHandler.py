"""
SenderDisconnectConsumerHandler - Consumer xử lý logic khi sender disconnect

Nhận message từ Kafka và thực hiện cleanup:
1. Tìm pair của sender
2. Set receiver về IDLE
3. Publish emit event thông báo sender disconnect về receiver
4. Cancel tất cả jobs của pair
5. Xóa pair
6. Xóa sender
"""
from typing import ClassVar
from pydantic import ValidationError

from src.kafka.consumer.server.dto.SenderDisconnectedConsumerDto import SenderDisconnectedConsumerDto

from src.kafka.consumer.shared.interface.IEventHandler import IEventHandler
from src.kafka.consumer.shared.interface.IDLQHandler import IDLQHandler
from src.kafka.consumer.shared.enum.KafkaTopic import KafkaTopic
from src.kafka.consumer.shared.enum.ConsumerGroup import ConsumerGroup
from src.kafka.consumer.server.enum.ServerTopic import ServerTopic
from src.kafka.consumer.server.enum.ServerGroup import ServerGroup
from src.kafka.consumer.shared.kafka_publisher.base.KafkaEmitPublisher import (
    KafkaEmitPublisher,
)
from src.kafka.consumer.server.kafka_publisher.dto.SenderDisconnectedEmitDto import (
    SenderDisconnectedEmitDto,
)

from src.socketio.socketio_server.main.manager.SenderManager import SenderManager
from src.socketio.socketio_server.main.manager.ReceiverManager import ReceiverManager
from src.socketio.socketio_server.main.manager.PairManager import PairManager
from src.socketio.socketio_server.main.manager.JobManager import JobManager
from src.socketio.socketio_server.main.enum.ReceiverStatus import ReceiverStatus
from src.socketio.socketio_server.main.enum.JobStatus import JobStatus


class SenderDisconnectConsumerHandler(IEventHandler, IDLQHandler):
    """
    Consumer handler xử lý logic cleanup khi sender disconnect.

    Flow xử lý:
        1. Parse SenderDisconnectedConsumerDto từ Kafka message
        2. Tìm pair của sender (nếu có)
        3. Nếu có pair:
           - Lấy receiver_id từ pair
           - Set receiver về trạng thái IDLE
           - Publish emit event thông báo sender disconnect về receiver
           - Tìm và cancel tất cả jobs của pair
           - Xóa bản ghi pair
        4. Xóa sender

    Managers và emit_publisher được inject từ ServerRegistry.
    """

    # Main consumer config
    topic: ClassVar[KafkaTopic] = ServerTopic.SENDER_DISCONNECTED
    group: ClassVar[ConsumerGroup] = ServerGroup.SENDER_DISCONNECT

    # DLQ consumer config
    dlq_topic: ClassVar[KafkaTopic] = ServerTopic.SENDER_DISCONNECTED_DLQ
    dlq_group: ClassVar[ConsumerGroup] = ServerGroup.SENDER_DISCONNECT_DLQ

    def __init__(
        self,
        emit_publisher: KafkaEmitPublisher,
        sender_manager: SenderManager,
        receiver_manager: ReceiverManager,
        pair_manager: PairManager,
        job_manager: JobManager,
    ):
        """
        Khởi tạo handler với các managers được inject từ bên ngoài.

        Args:
            emit_publisher (KafkaEmitPublisher): Publisher để publish emit events
            sender_manager (SenderManager): Manager quản lý senders
            receiver_manager (ReceiverManager): Manager quản lý receivers
            pair_manager (PairManager): Manager quản lý pairs
            job_manager (JobManager): Manager quản lý jobs
        """
        self._emit_publisher = emit_publisher
        self._sender_manager = sender_manager
        self._receiver_manager = receiver_manager
        self._pair_manager = pair_manager
        self._job_manager = job_manager

    def handle(self, data: dict):
        """
        Xử lý logic cleanup khi sender disconnect.

        Args:
            data: Message data từ Kafka chứa SenderDisconnectedDto

        Raises:
            ValidationError: Nếu data không đúng format → message vào DLQ
        """
        # 1. Parse DTO
        try:
            dto = SenderDisconnectedConsumerDto(**data)
        except ValidationError as e:
            print(f"[SenderDisconnectConsumer] Invalid data format: {e}")
            raise  # Raise để message vào DLQ

        sender_id = dto.sender_id

        print(f"\n{'='*60}")
        print(f"[SenderDisconnectConsumer] 🔄 PROCESSING SENDER DISCONNECT")
        print(f"[SenderDisconnectConsumer] Sender ID: {sender_id}")
        print(f"[SenderDisconnectConsumer] Timestamp: {dto.timestamp}")
        print(f"{'='*60}\n")

        # 2. Tìm pair của sender
        pair = self._pair_manager.get_pair_by_sender(sender_id)

        if pair:
            receiver_id = pair.receiver_id
            pair_id = pair.id

            print(f"[SenderDisconnectConsumer] Found pair: {pair_id}")
            print(f"[SenderDisconnectConsumer] Receiver ID: {receiver_id}")

            # 3. Set receiver về IDLE
            success = self._receiver_manager.update_status(receiver_id, ReceiverStatus.IDLE)
            if success:
                print(f"[SenderDisconnectConsumer] ✅ Set receiver {receiver_id} to IDLE")
            else:
                print(f"[SenderDisconnectConsumer] ⚠️ Failed to update receiver {receiver_id} status")

            # 3.5. Publish emit event thông báo sender disconnect về receiver
            emit_dto = SenderDisconnectedEmitDto(
                target_sid=receiver_id,
                sender_id=sender_id,
                pair_id=str(pair_id),
            )
            self._emit_publisher.publish(emit_dto)
            print(f"[SenderDisconnectConsumer] 📤 Published sender-disconnected emit to receiver {receiver_id}")

            # TODO: Có thể thêm bước notify worker để hủy job đang chạy (OPTIONAL)

            # 4. Tìm và cancel tất cả jobs của pair
            jobs = self._job_manager.get_jobs_by_pair(str(pair_id))
            cancelled_count = 0

            for job in jobs:
                self._job_manager.update_job_status(job.id, JobStatus.CANCEL)
                cancelled_count += 1

            print(f"[SenderDisconnectConsumer] ✅ Cancelled {cancelled_count} job(s) for pair {pair_id}")

            # 5. Xóa pair
            self._pair_manager.remove_pair(pair_id)
            print(f"[SenderDisconnectConsumer] ✅ Removed pair {pair_id}")
        else:
            print(f"[SenderDisconnectConsumer] No pair found for sender {sender_id}")

        # 6. Xóa sender
        success = self._sender_manager.remove_sender(sender_id)
        if success:
            print(f"[SenderDisconnectConsumer] ✅ Removed sender {sender_id}")
        else:
            print(f"[SenderDisconnectConsumer] ⚠️ Sender {sender_id} not found in manager")

        print(f"\n{'='*60}")
        print(f"[SenderDisconnectConsumer] ✅ SENDER DISCONNECT PROCESSED SUCCESSFULLY")
        print(f"{'='*60}\n")

    def handle_dlq(self, data: dict, error_info: dict) -> None:
        """
        Xử lý message failed từ DLQ.

        Args:
            data: Original message data
            error_info: Error context với keys:
                - error_message: Nội dung lỗi
                - error_type: Loại exception
                - timestamp: Thời điểm xảy ra lỗi
                - original_topic: Topic gốc
                - handler_name: Tên handler
                - retry_count: Số lần đã retry
        """
        print(f"\n{'='*60}")
        print(f"[SenderDisconnectConsumer] ⚠️ DLQ MESSAGE RECEIVED")
        print(f"[SenderDisconnectConsumer] Error Type: {error_info.get('error_type')}")
        print(f"[SenderDisconnectConsumer] Error Message: {error_info.get('error_message')}")
        print(f"[SenderDisconnectConsumer] Original Topic: {error_info.get('original_topic')}")
        print(f"[SenderDisconnectConsumer] Retry Count: {error_info.get('retry_count')}")
        print(f"[SenderDisconnectConsumer] Data: {data}")
        print(f"{'='*60}\n")

        # TODO: Implement retry logic hoặc alert
        # Có thể:
        # - Retry với exponential backoff
        # - Gửi alert đến monitoring system
        # - Log vào database để manual review
