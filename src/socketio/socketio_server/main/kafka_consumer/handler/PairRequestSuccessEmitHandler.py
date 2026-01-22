"""
PairRequestSuccessEmitHandler - Emit handler cho pair request success events.

Nhận message từ Kafka topic emit.pair-request-success và emit
SocketIO event pair-request-success về client.
"""
from typing import ClassVar
from pydantic import ValidationError

from socketio import AsyncServer

from src.socketio.shared.dto.pairing import PairRequestSuccessDto
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
from src.socketio.socketio_server.main.kafka_consumer.dto.PairRequestSuccessConsumerDto import (
    PairRequestSuccessConsumerDto,
)

from src.socketio.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio.socketio_server.main.enum.MainNamespace import MainNamespaces


class PairRequestSuccessEmitHandler(IEmitHandler, IDLQHandler):
    """
    Emit handler cho pair request success events.

    Flow xử lý:
        1. Nhận message từ Kafka topic emit.pair-request-success
        2. Parse PairRequestSuccessConsumerDto từ message
        3. Emit SocketIO event pair-request-success về client (target_sid)

    Handler này emit về client khi pairing thành công.
    """

    # Main consumer config
    topic: ClassVar[KafkaTopic] = EmitTopic.PAIR_REQUEST_SUCCESS
    group: ClassVar[ConsumerGroup] = EmitGroup.PAIR_REQUEST_SUCCESS

    # DLQ consumer config
    dlq_topic: ClassVar[KafkaTopic] = EmitTopic.PAIR_REQUEST_SUCCESS_DLQ
    dlq_group: ClassVar[ConsumerGroup] = EmitGroup.PAIR_REQUEST_SUCCESS_DLQ

    async def handle(self, sio: AsyncServer, data: dict) -> None:
        """
        Emit SocketIO event pair-request-success về client.

        Args:
            sio (AsyncServer): SocketIO AsyncServer instance để emit events
            data (dict): Message data từ Kafka chứa:
                - targetSid: Socket ID của client cần emit tới
                - pairId: ID của pair được tạo
                - senderId: ID của sender
                - receiverId: ID của receiver

        Raises:
            ValidationError: Nếu data không đúng format → message vào DLQ
        """
        # 1. Parse DTO
        try:
            dto = PairRequestSuccessConsumerDto(**data)
        except ValidationError as e:
            print(f"[PairRequestSuccessEmitHandler] Invalid data format: {e}")
            raise  # Raise để message vào DLQ

        print(f"\n{'='*60}")
        print(f"[PairRequestSuccessEmitHandler] EMITTING PAIR REQUEST SUCCESS")
        print(f"[PairRequestSuccessEmitHandler] Target SID: {dto.target_sid}")
        print(f"[PairRequestSuccessEmitHandler] Pair ID: {dto.pair_id}")
        print(f"{'='*60}\n")

        # 2. Tạo payload cho SocketIO emit
        payloadDto = PairRequestSuccessDto(
            pair_id=dto.pair_id,
            sender_id=dto.sender_id,
            receiver_id=dto.receiver_id,
        )
        payload = payloadDto.model_dump(by_alias=True)

        # 3. Emit SocketIO event về client
        await sio.emit(
            MainEvents.PAIR_REQUEST_SUCCESS.value,
            payload,
            room=dto.target_sid,
            namespace=MainNamespaces.ROOT.value,
        )

        print(f"[PairRequestSuccessEmitHandler] Emitted to {dto.target_sid}")

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
        print(f"[PairRequestSuccessEmitHandler] DLQ MESSAGE RECEIVED")
        print(f"[PairRequestSuccessEmitHandler] Error Type: {error_info.get('error_type')}")
        print(
            f"[PairRequestSuccessEmitHandler] Error Message: {error_info.get('error_message')}"
        )
        print(
            f"[PairRequestSuccessEmitHandler] Original Topic: {error_info.get('original_topic')}"
        )
        print(f"[PairRequestSuccessEmitHandler] Retry Count: {error_info.get('retry_count')}")
        print(f"[PairRequestSuccessEmitHandler] Data: {data}")
        print(f"{'='*60}\n")
