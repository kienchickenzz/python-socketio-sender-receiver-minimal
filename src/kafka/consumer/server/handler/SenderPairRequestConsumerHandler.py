"""
SenderPairRequestConsumerHandler - Consumer xử lý logic khi sender yêu cầu pair.

Nhận message từ Kafka và thực hiện:
1. Parse SenderPairRequestConsumerDto từ message
2. Thêm sender vào pool với status ACTIVE
3. Tìm receiver IDLE chưa được pair
4. Nếu có → tạo pair, publish emit success cho cả sender và receiver
5. Nếu không → publish emit failed cho sender
"""
from typing import ClassVar
from pydantic import ValidationError

from src.kafka.consumer.server.dto.SenderPairRequestConsumerDto import (
    SenderPairRequestConsumerDto,
)

from src.kafka.consumer.shared.interface.IEventHandler import IEventHandler
from src.kafka.consumer.shared.interface.IDLQHandler import IDLQHandler
from src.kafka.consumer.shared.enum.KafkaTopic import KafkaTopic
from src.kafka.consumer.shared.enum.ConsumerGroup import ConsumerGroup
from src.kafka.consumer.shared.kafka_publisher.base.KafkaEmitPublisher import (
    KafkaEmitPublisher,
)
from src.kafka.consumer.server.enum.ServerTopic import ServerTopic
from src.kafka.consumer.server.enum.ServerGroup import ServerGroup
from src.kafka.consumer.server.kafka_publisher.dto.PairRequestSuccessEmitDto import (
    PairRequestSuccessEmitDto,
)
from src.kafka.consumer.server.kafka_publisher.dto.PairRequestFailedEmitDto import (
    PairRequestFailedEmitDto,
)

from src.socketio.socketio_server.main.manager.SenderManager import SenderManager
from src.socketio.socketio_server.main.manager.ReceiverManager import ReceiverManager
from src.socketio.socketio_server.main.manager.PairManager import PairManager
from src.socketio.socketio_server.main.enum.SenderStatus import SenderStatus
from src.socketio.socketio_server.main.enum.ReceiverStatus import ReceiverStatus


class SenderPairRequestConsumerHandler(IEventHandler, IDLQHandler):
    """
    Consumer handler xử lý logic khi sender yêu cầu pair với receiver.

    Flow xử lý:
        1. Parse SenderPairRequestConsumerDto từ Kafka message
        2. Thêm sender vào SenderManager với status ACTIVE
        3. Tìm receiver IDLE chưa được pair
        4. Nếu có receiver available:
           - Tạo pair trong PairManager
           - Update receiver status sang ACTIVE
           - Publish emit success cho cả sender và receiver
        5. Nếu không có receiver:
           - Publish emit failed cho sender

    Managers và emit_publisher được inject từ ServerRegistry.
    """

    # Main consumer config
    topic: ClassVar[KafkaTopic] = ServerTopic.SENDER_PAIR_REQUEST
    group: ClassVar[ConsumerGroup] = ServerGroup.SENDER_PAIR_REQUEST

    # DLQ consumer config
    dlq_topic: ClassVar[KafkaTopic] = ServerTopic.SENDER_PAIR_REQUEST_DLQ
    dlq_group: ClassVar[ConsumerGroup] = ServerGroup.SENDER_PAIR_REQUEST_DLQ

    def __init__(
        self,
        emit_publisher: KafkaEmitPublisher,
        sender_manager: SenderManager,
        receiver_manager: ReceiverManager,
        pair_manager: PairManager,
    ):
        """
        Khởi tạo handler với dependencies được inject từ bên ngoài.

        Args:
            emit_publisher (KafkaEmitPublisher): Publisher để publish emit events
            sender_manager (SenderManager): Manager quản lý senders
            receiver_manager (ReceiverManager): Manager quản lý receivers
            pair_manager (PairManager): Manager quản lý pairs
        """
        self._emit_publisher = emit_publisher
        self._sender_manager = sender_manager
        self._receiver_manager = receiver_manager
        self._pair_manager = pair_manager

    def handle(self, data: dict) -> None:
        """
        Xử lý logic khi sender yêu cầu pair với receiver.

        Args:
            data (dict): Message data từ Kafka chứa SenderPairRequestDto
                - sessionId: ID của sender
                - clientSid: Socket ID của sender

        Raises:
            ValidationError: Nếu data không đúng format → message vào DLQ
        """
        # 1. Parse DTO
        try:
            dto = SenderPairRequestConsumerDto(**data)
        except ValidationError as e:
            print(f"[SenderPairRequestConsumer] Invalid data format: {e}")
            raise  # Raise để message vào DLQ

        session_id = dto.session_id
        client_sid = dto.client_sid

        print(f"\n{'='*60}")
        print(f"[SenderPairRequestConsumer] PROCESSING SENDER PAIR REQUEST")
        print(f"[SenderPairRequestConsumer] Session ID: {session_id}")
        print(f"[SenderPairRequestConsumer] Client SID: {client_sid}")
        print(f"{'='*60}\n")

        # 2. Thêm sender vào pool với status ACTIVE
        self._sender_manager.add_sender(session_id, SenderStatus.ACTIVE)
        print(f"[SenderPairRequestConsumer] Sender {session_id} added as ACTIVE")

        # 3. Tìm receiver IDLE chưa được pair
        idle_receivers = self._receiver_manager.get_receivers_by_status(
            ReceiverStatus.IDLE
        )

        available_receiver_id = None
        for receiver_id in idle_receivers.keys():
            if not self._pair_manager.is_receiver_paired(receiver_id):
                available_receiver_id = receiver_id
                break

        if available_receiver_id:
            # 4. Có receiver available → tạo pair
            pair_id = self._pair_manager.add_pair(session_id, available_receiver_id)

            # Update receiver status sang ACTIVE
            self._receiver_manager.update_status(
                available_receiver_id, ReceiverStatus.ACTIVE
            )

            print(
                f"[SenderPairRequestConsumer] Paired sender {session_id} "
                f"with receiver {available_receiver_id} (pair_id: {pair_id})"
            )

            # Publish emit success cho sender
            sender_emit_dto = PairRequestSuccessEmitDto(
                target_sid=client_sid,
                pair_id=str(pair_id),
                sender_id=session_id,
                receiver_id=available_receiver_id,
            )
            self._emit_publisher.publish(sender_emit_dto)

            # Publish emit success cho receiver
            receiver_emit_dto = PairRequestSuccessEmitDto(
                target_sid=available_receiver_id,
                pair_id=str(pair_id),
                sender_id=session_id,
                receiver_id=available_receiver_id,
            )
            self._emit_publisher.publish(receiver_emit_dto)

            print(f"[SenderPairRequestConsumer] Published emit success events")

        else:
            # 5. Không có receiver → publish emit failed
            print(
                f"[SenderPairRequestConsumer] No available receiver for sender {session_id}"
            )

            failed_emit_dto = PairRequestFailedEmitDto(
                target_sid=client_sid,
                sender_id=session_id,
                reason="No available receiver",
            )
            self._emit_publisher.publish(failed_emit_dto)

            print(f"[SenderPairRequestConsumer] Published emit failed event")

        print(f"\n{'='*60}")
        print(f"[SenderPairRequestConsumer] SENDER PAIR REQUEST PROCESSED")
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
        print(f"[SenderPairRequestConsumer] DLQ MESSAGE RECEIVED")
        print(f"[SenderPairRequestConsumer] Error Type: {error_info.get('error_type')}")
        print(
            f"[SenderPairRequestConsumer] Error Message: {error_info.get('error_message')}"
        )
        print(
            f"[SenderPairRequestConsumer] Original Topic: {error_info.get('original_topic')}"
        )
        print(f"[SenderPairRequestConsumer] Retry Count: {error_info.get('retry_count')}")
        print(f"[SenderPairRequestConsumer] Data: {data}")
        print(f"{'='*60}\n")
