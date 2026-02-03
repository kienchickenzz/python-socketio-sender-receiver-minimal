"""
SenderDisconnectedEmitHandler - Emit handler cho sender disconnected events.

Nhận message từ Kafka topic emit.sender-disconnected và emit
SocketIO event sender-disconnected về receiver.
"""
from typing import ClassVar
from pydantic import ValidationError

from socketio import AsyncServer

from src.socketio.shared.dto.connection import SenderDisconnectDto
from src.socketio.socketio_server.shared.kafka_consumer.interface.IEmitHandler import (
    IEmitHandler,
)
from src.socketio.socketio_server.shared.kafka_consumer.interface.IDLQHandler import (
    IDLQHandler,
)
from src.socketio.socketio_server.shared.kafka_consumer.enum.KafkaTopic import KafkaTopic
from src.socketio.socketio_server.shared.kafka_consumer.enum.ConsumerGroup import (
    ConsumerGroup,
)

from src.socketio.socketio_server.main.kafka_consumer.enum.EmitTopic import EmitTopic
from src.socketio.socketio_server.main.kafka_consumer.enum.EmitGroup import EmitGroup
from src.socketio.socketio_server.main.kafka_consumer.dto.SenderDisconnectedConsumerDto import (
    SenderDisconnectedConsumerDto,
)

from src.socketio.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio.socketio_server.main.enum.MainNamespace import MainNamespaces


class SenderDisconnectedEmitHandler(IEmitHandler, IDLQHandler):
    """
    Emit handler cho sender disconnected events.

    Flow xử lý:
        1. Nhận message từ Kafka topic emit.sender-disconnected
        2. Parse SenderDisconnectedConsumerDto từ message
        3. Emit SocketIO event sender-disconnected về receiver (target_sid)

    Handler này emit thông báo sender ngắt kết nối về receiver client.
    """

    # Main consumer config
    topic: ClassVar[KafkaTopic] = EmitTopic.SENDER_DISCONNECTED
    group: ClassVar[ConsumerGroup] = EmitGroup.SENDER_DISCONNECTED

    # DLQ consumer config
    dlq_topic: ClassVar[KafkaTopic] = EmitTopic.SENDER_DISCONNECTED_DLQ
    dlq_group: ClassVar[ConsumerGroup] = EmitGroup.SENDER_DISCONNECTED_DLQ

    async def handle(self, sio: AsyncServer, data: dict) -> None:
        """
        Emit SocketIO event sender-disconnected về receiver.

        Args:
            sio (AsyncServer): SocketIO AsyncServer instance để emit events
            data (dict): Message data từ Kafka chứa:
                - targetSid: Socket ID của receiver cần emit tới
                - senderId: ID của sender đã ngắt kết nối
                - pairId: ID của cặp sender-receiver

        Raises:
            ValidationError: Nếu data không đúng format → message vào DLQ
        """
        # 1. Parse DTO
        try:
            dto = SenderDisconnectedConsumerDto(**data)
        except ValidationError as e:
            print(f"[SenderDisconnectedEmitHandler] Invalid data format: {e}")
            raise  # Raise để message vào DLQ

        print(f"\n{'='*60}")
        print(f"[SenderDisconnectedEmitHandler] EMITTING SENDER DISCONNECTED")
        print(f"[SenderDisconnectedEmitHandler] Target SID: {dto.target_sid}")
        print(f"[SenderDisconnectedEmitHandler] Sender ID: {dto.sender_id}")
        print(f"[SenderDisconnectedEmitHandler] Pair ID: {dto.pair_id}")
        print(f"{'='*60}\n")

        # 2. Tạo payload cho SocketIO emit
        payload_dto = SenderDisconnectDto(
            sender_id=dto.sender_id,
            pair_id=dto.pair_id,
        )
        payload = payload_dto.model_dump(by_alias=True)

        # 3. Emit SocketIO event về receiver
        await sio.emit(
            MainEvents.SENDER_DISCONNECTED.value,
            payload,
            room=dto.target_sid,
            namespace=MainNamespaces.ROOT.value,
        )

        print(f"[SenderDisconnectedEmitHandler] ✅ Emitted to receiver {dto.target_sid}")

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
        print(f"[SenderDisconnectedEmitHandler] DLQ MESSAGE RECEIVED")
        print(f"[SenderDisconnectedEmitHandler] Error Type: {error_info.get('error_type')}")
        print(
            f"[SenderDisconnectedEmitHandler] Error Message: {error_info.get('error_message')}"
        )
        print(
            f"[SenderDisconnectedEmitHandler] Original Topic: {error_info.get('original_topic')}"
        )
        print(f"[SenderDisconnectedEmitHandler] Retry Count: {error_info.get('retry_count')}")
        print(f"[SenderDisconnectedEmitHandler] Data: {data}")
        print(f"{'='*60}\n")
