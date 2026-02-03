"""
ReceiverDisconnectConsumerHandler - Consumer xử lý logic khi receiver disconnect

Nhận message từ Kafka và thực hiện cleanup:
1. Xóa receiver khỏi pool
"""
from typing import ClassVar
from pydantic import ValidationError

from src.kafka.consumer.server.dto.ReceiverDisconnectedConsumerDto import ReceiverDisconnectedConsumerDto

from src.kafka.consumer.shared.interface.IEventHandler import IEventHandler
from src.kafka.consumer.shared.interface.IDLQHandler import IDLQHandler
from src.kafka.consumer.shared.enum.KafkaTopic import KafkaTopic
from src.kafka.consumer.shared.enum.ConsumerGroup import ConsumerGroup
from src.kafka.consumer.server.enum.ServerTopic import ServerTopic
from src.kafka.consumer.server.enum.ServerGroup import ServerGroup

from src.socketio.socketio_server.main.manager.ReceiverManager import ReceiverManager


class ReceiverDisconnectConsumerHandler(IEventHandler, IDLQHandler):
    """
    Consumer handler xử lý logic cleanup khi receiver disconnect.

    Flow xử lý:
        1. Parse ReceiverDisconnectedConsumerDto từ Kafka message
        2. Xóa receiver khỏi pool

    ReceiverManager được inject từ ServerRegistry.
    """

    # Main consumer config
    topic: ClassVar[KafkaTopic] = ServerTopic.RECEIVER_DISCONNECTED
    group: ClassVar[ConsumerGroup] = ServerGroup.RECEIVER_DISCONNECT

    # DLQ consumer config
    dlq_topic: ClassVar[KafkaTopic] = ServerTopic.RECEIVER_DISCONNECTED_DLQ
    dlq_group: ClassVar[ConsumerGroup] = ServerGroup.RECEIVER_DISCONNECT_DLQ

    def __init__(self, receiver_manager: ReceiverManager):
        """
        Khởi tạo handler với ReceiverManager được inject từ bên ngoài.

        Args:
            receiver_manager (ReceiverManager): Manager quản lý receivers
        """
        self._receiver_manager = receiver_manager

    def handle(self, data: dict):
        """
        Xử lý logic cleanup khi receiver disconnect.

        Args:
            data: Message data từ Kafka chứa ReceiverDisconnectedDto

        Raises:
            ValidationError: Nếu data không đúng format → message vào DLQ
        """
        # 1. Parse DTO
        try:
            dto = ReceiverDisconnectedConsumerDto(**data)
        except ValidationError as e:
            print(f"[ReceiverDisconnectConsumer] Invalid data format: {e}")
            raise  # Raise để message vào DLQ

        receiver_id = dto.receiver_id

        print(f"\n{'='*60}")
        print(f"[ReceiverDisconnectConsumer] PROCESSING RECEIVER DISCONNECT")
        print(f"[ReceiverDisconnectConsumer] Receiver ID: {receiver_id}")
        print(f"[ReceiverDisconnectConsumer] Timestamp: {dto.timestamp}")
        print(f"{'='*60}\n")

        # 2. Xóa receiver khỏi pool
        success = self._receiver_manager.remove_receiver(receiver_id)
        if success:
            print(f"[ReceiverDisconnectConsumer] Removed receiver {receiver_id}")
        else:
            print(f"[ReceiverDisconnectConsumer] Receiver {receiver_id} not found in manager")

        print(f"\n{'='*60}")
        print(f"[ReceiverDisconnectConsumer] RECEIVER DISCONNECT PROCESSED SUCCESSFULLY")
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
        print(f"[ReceiverDisconnectConsumer] DLQ MESSAGE RECEIVED")
        print(f"[ReceiverDisconnectConsumer] Error Type: {error_info.get('error_type')}")
        print(f"[ReceiverDisconnectConsumer] Error Message: {error_info.get('error_message')}")
        print(f"[ReceiverDisconnectConsumer] Original Topic: {error_info.get('original_topic')}")
        print(f"[ReceiverDisconnectConsumer] Retry Count: {error_info.get('retry_count')}")
        print(f"[ReceiverDisconnectConsumer] Data: {data}")
        print(f"{'='*60}\n")
