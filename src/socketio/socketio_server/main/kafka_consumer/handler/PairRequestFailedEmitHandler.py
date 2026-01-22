"""
PairRequestFailedEmitHandler - Emit handler cho pair request failed events.

Nhận message từ Kafka topic emit.pair-request-failed và emit
SocketIO event pair-request-failed về client.
"""
from typing import ClassVar
from pydantic import ValidationError

from socketio import AsyncServer

from src.socketio.socketio_server.shared.kafka_consumer.interface.IEmitHandler import (
    IEmitHandler,
)
from src.socketio.socketio_server.shared.kafka_consumer.interface.IDLQHandler import (
    IDLQHandler,
)
from src.socketio.shared.dto.pairing import PairRequestFailedDto
from src.socketio.socketio_server.shared.kafka_consumer.enum.KafkaTopic import KafkaTopic
from src.socketio.socketio_server.shared.kafka_consumer.enum.ConsumerGroup import (
    ConsumerGroup,
)

from src.socketio.socketio_server.main.kafka_consumer.enum.EmitTopic import EmitTopic
from src.socketio.socketio_server.main.kafka_consumer.enum.EmitGroup import EmitGroup
from src.socketio.socketio_server.main.kafka_consumer.dto.PairRequestFailedConsumerDto import (
    PairRequestFailedConsumerDto,
)

from src.socketio.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio.socketio_server.main.enum.MainNamespace import MainNamespaces


class PairRequestFailedEmitHandler(IEmitHandler, IDLQHandler):
    """
    Emit handler cho pair request failed events.

    Flow xử lý:
        1. Nhận message từ Kafka topic emit.pair-request-failed
        2. Parse PairRequestFailedConsumerDto từ message
        3. Emit SocketIO event pair-request-failed về client (target_sid)

    Handler này emit về client khi pairing thất bại (không có receiver available).
    """

    # Main consumer config
    topic: ClassVar[KafkaTopic] = EmitTopic.PAIR_REQUEST_FAILED
    group: ClassVar[ConsumerGroup] = EmitGroup.PAIR_REQUEST_FAILED

    # DLQ consumer config
    dlq_topic: ClassVar[KafkaTopic] = EmitTopic.PAIR_REQUEST_FAILED_DLQ
    dlq_group: ClassVar[ConsumerGroup] = EmitGroup.PAIR_REQUEST_FAILED_DLQ

    async def handle(self, sio: AsyncServer, data: dict) -> None:
        """
        Emit SocketIO event pair-request-failed về client.

        Args:
            sio (AsyncServer): SocketIO AsyncServer instance để emit events
            data (dict): Message data từ Kafka chứa:
                - targetSid: Socket ID của client cần emit tới
                - senderId: ID của sender
                - reason: Lý do pair thất bại

        Raises:
            ValidationError: Nếu data không đúng format → message vào DLQ
        """
        # 1. Parse DTO
        try:
            dto = PairRequestFailedConsumerDto(**data)
        except ValidationError as e:
            print(f"[PairRequestFailedEmitHandler] Invalid data format: {e}")
            raise  # Raise để message vào DLQ

        print(f"\n{'='*60}")
        print(f"[PairRequestFailedEmitHandler] EMITTING PAIR REQUEST FAILED")
        print(f"[PairRequestFailedEmitHandler] Target SID: {dto.target_sid}")
        print(f"[PairRequestFailedEmitHandler] Sender ID: {dto.sender_id}")
        print(f"[PairRequestFailedEmitHandler] Reason: {dto.reason}")
        print(f"{'='*60}\n")

        # 2. Tạo payload cho SocketIO emit
        payloadDto = PairRequestFailedDto(
            sender_id=dto.sender_id,
            reason=dto.reason,
        )
        payload = payloadDto.model_dump(by_alias=True)
        
        # 3. Emit SocketIO event về client
        await sio.emit(
            MainEvents.PAIR_REQUEST_FAILED.value,
            payload,
            room=dto.target_sid,
            namespace=MainNamespaces.ROOT.value,
        )

        print(f"[PairRequestFailedEmitHandler] Emitted to {dto.target_sid}")

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
        print(f"[PairRequestFailedEmitHandler] DLQ MESSAGE RECEIVED")
        print(f"[PairRequestFailedEmitHandler] Error Type: {error_info.get('error_type')}")
        print(
            f"[PairRequestFailedEmitHandler] Error Message: {error_info.get('error_message')}"
        )
        print(
            f"[PairRequestFailedEmitHandler] Original Topic: {error_info.get('original_topic')}"
        )
        print(f"[PairRequestFailedEmitHandler] Retry Count: {error_info.get('retry_count')}")
        print(f"[PairRequestFailedEmitHandler] Data: {data}")
        print(f"{'='*60}\n")
