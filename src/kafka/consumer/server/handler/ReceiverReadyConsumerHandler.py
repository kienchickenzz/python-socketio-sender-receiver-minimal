"""
ReceiverReadyConsumerHandler - Consumer xử lý logic khi receiver sẵn sàng

Nhận message từ Kafka và thực hiện:
1. Parse ReceiverReadyConsumerDto từ message
2. Thêm receiver vào pool với status IDLE
3. Log thông tin về số lượng receivers
"""
from typing import ClassVar
from pydantic import ValidationError

from src.kafka.consumer.server.dto.ReceiverReadyConsumerDto import ReceiverReadyConsumerDto

from src.kafka.consumer.shared.interface.IEventHandler import IEventHandler
from src.kafka.consumer.shared.interface.IDLQHandler import IDLQHandler
from src.kafka.consumer.shared.enum.KafkaTopic import KafkaTopic
from src.kafka.consumer.shared.enum.ConsumerGroup import ConsumerGroup
from src.kafka.consumer.server.enum.ServerTopic import ServerTopic
from src.kafka.consumer.server.enum.ServerGroup import ServerGroup

from src.socketio.socketio_server.main.manager.ReceiverManager import ReceiverManager
from src.socketio.socketio_server.main.enum.ReceiverStatus import ReceiverStatus


class ReceiverReadyConsumerHandler(IEventHandler, IDLQHandler):
    """
    Consumer handler xử lý logic khi receiver sẵn sàng.

    Flow xử lý:
        1. Parse ReceiverReadyConsumerDto từ Kafka message
        2. Thêm receiver vào ReceiverManager với status IDLE
        3. Log thông tin về số lượng receivers hiện có

    Managers được inject từ ServerRegistry để đảm bảo clear ownership.
    """

    # Main consumer config
    topic: ClassVar[KafkaTopic] = ServerTopic.RECEIVER_READY
    group: ClassVar[ConsumerGroup] = ServerGroup.RECEIVER_READY

    # DLQ consumer config
    dlq_topic: ClassVar[KafkaTopic] = ServerTopic.RECEIVER_READY_DLQ
    dlq_group: ClassVar[ConsumerGroup] = ServerGroup.RECEIVER_READY_DLQ

    def __init__(self, receiver_manager: ReceiverManager):
        """
        Khởi tạo handler với ReceiverManager được inject từ bên ngoài.

        Args:
            receiver_manager (ReceiverManager): Manager quản lý receivers
        """
        self._receiver_manager = receiver_manager

    def handle(self, data: dict) -> None:
        """
        Xử lý logic khi receiver sẵn sàng.

        Args:
            data (dict): Message data từ Kafka chứa ReceiverReadyDto
                - sessionId: ID của receiver (client_sid)

        Raises:
            ValidationError: Nếu data không đúng format → message vào DLQ
        """
        # 1. Parse DTO
        try:
            dto = ReceiverReadyConsumerDto(**data)
        except ValidationError as e:
            print(f"[ReceiverReadyConsumer] Invalid data format: {e}")
            raise  # Raise để message vào DLQ

        session_id = dto.session_id

        print(f"\n{'='*60}")
        print(f"[ReceiverReadyConsumer] 🔄 PROCESSING RECEIVER READY")
        print(f"[ReceiverReadyConsumer] Session ID: {session_id}")
        print(f"{'='*60}\n")

        # 2. Thêm receiver vào pool với status IDLE
        self._receiver_manager.add_receiver(session_id, ReceiverStatus.IDLE)

        print(f"[ReceiverReadyConsumer] ✅ Receiver {session_id} added to pool as IDLE")

        # 3. Log thông tin về số lượng receivers
        active_count = self._receiver_manager.count_by_status(ReceiverStatus.ACTIVE)
        idle_count = self._receiver_manager.count_by_status(ReceiverStatus.IDLE)
        total_count = self._receiver_manager.count()

        print(f"[ReceiverReadyConsumer] 📊 Receiver Statistics:")
        print(f"[ReceiverReadyConsumer]    - Total receivers: {total_count}")
        print(f"[ReceiverReadyConsumer]    - Active receivers: {active_count}")
        print(f"[ReceiverReadyConsumer]    - Idle receivers: {idle_count}")

        print(f"\n{'='*60}")
        print(f"[ReceiverReadyConsumer] ✅ RECEIVER READY PROCESSED SUCCESSFULLY")
        print(f"{'='*60}\n")

    def handle_dlq(self, data: dict, error_info: dict) -> None:
        """
        Xử lý message failed từ DLQ.

        Args:
            data (dict): Original message data
            error_info (dict): Error context với keys:
                - error_message: Nội dung lỗi
                - error_type: Loại exception
                - timestamp: Thời điểm xảy ra lỗi
                - original_topic: Topic gốc
                - handler_name: Tên handler
                - retry_count: Số lần đã retry
        """
        print(f"\n{'='*60}")
        print(f"[ReceiverReadyConsumer] ⚠️ DLQ MESSAGE RECEIVED")
        print(f"[ReceiverReadyConsumer] Error Type: {error_info.get('error_type')}")
        print(f"[ReceiverReadyConsumer] Error Message: {error_info.get('error_message')}")
        print(f"[ReceiverReadyConsumer] Original Topic: {error_info.get('original_topic')}")
        print(f"[ReceiverReadyConsumer] Retry Count: {error_info.get('retry_count')}")
        print(f"[ReceiverReadyConsumer] Data: {data}")
        print(f"{'='*60}\n")

        # TODO: Implement retry logic hoặc alert
        # Có thể:
        # - Retry với exponential backoff
        # - Gửi alert đến monitoring system
        # - Log vào database để manual review
