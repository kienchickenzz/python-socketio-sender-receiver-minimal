"""
ProcessingResultEmitHandler - Emit handler cho processing result events.

Nhận message từ Kafka topic emit.processing-result và emit
SocketIO event processing-result về receiver.
"""
from typing import ClassVar
from pydantic import ValidationError

from socketio import AsyncServer

from src.socketio.shared.dto.processing import ProcessingResultDto
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
from src.socketio.socketio_server.main.kafka_consumer.dto.ProcessingResultConsumerDto import (
    ProcessingResultConsumerDto,
)

from src.socketio.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio.socketio_server.main.enum.MainNamespace import MainNamespaces


class ProcessingResultEmitHandler(IEmitHandler, IDLQHandler):
    """
    Emit handler cho processing result events.

    Flow xử lý:
        1. Nhận message từ Kafka topic emit.processing-result
        2. Parse ProcessingResultConsumerDto từ message
        3. Emit SocketIO event processing-result về receiver (target_sid)

    Handler này emit kết quả xử lý về receiver client.
    """

    # Main consumer config
    topic: ClassVar[KafkaTopic] = EmitTopic.PROCESSING_RESULT
    group: ClassVar[ConsumerGroup] = EmitGroup.PROCESSING_RESULT

    # DLQ consumer config
    dlq_topic: ClassVar[KafkaTopic] = EmitTopic.PROCESSING_RESULT_DLQ
    dlq_group: ClassVar[ConsumerGroup] = EmitGroup.PROCESSING_RESULT_DLQ

    async def handle(self, sio: AsyncServer, data: dict) -> None:
        """
        Emit SocketIO event processing-result về receiver.

        Args:
            sio (AsyncServer): SocketIO AsyncServer instance để emit events
            data (dict): Message data từ Kafka chứa:
                - targetSid: Socket ID của receiver cần emit tới
                - jobId: ID của job đã hoàn thành
                - pairId: ID của cặp sender-receiver
                - senderId: ID của sender
                - workerId: ID của worker đã xử lý
                - originalData: Dữ liệu gốc
                - result: Kết quả đã xử lý

        Raises:
            ValidationError: Nếu data không đúng format → message vào DLQ
        """
        # 1. Parse DTO
        try:
            dto = ProcessingResultConsumerDto(**data)
        except ValidationError as e:
            print(f"[ProcessingResultEmitHandler] Invalid data format: {e}")
            raise  # Raise để message vào DLQ

        print(f"\n{'='*60}")
        print(f"[ProcessingResultEmitHandler] EMITTING PROCESSING RESULT")
        print(f"[ProcessingResultEmitHandler] Target SID: {dto.target_sid}")
        print(f"[ProcessingResultEmitHandler] Job ID: {dto.job_id}")
        print(f"[ProcessingResultEmitHandler] Pair ID: {dto.pair_id}")
        print(f"[ProcessingResultEmitHandler] Sender ID: {dto.sender_id}")
        print(f"[ProcessingResultEmitHandler] Worker ID: {dto.worker_id}")
        print(f"[ProcessingResultEmitHandler] Result length: {len(dto.result)}")
        print(f"{'='*60}\n")

        # 2. Tạo payload cho SocketIO emit
        payload_dto = ProcessingResultDto(
            job_id=dto.job_id,
            pair_id=dto.pair_id,
            sender_id=dto.sender_id,
            worker_id=dto.worker_id,
            original_data=dto.original_data,
            result=dto.result,
        )
        payload = payload_dto.model_dump(by_alias=True)

        # 3. Emit SocketIO event về receiver
        await sio.emit(
            MainEvents.PROCESSING_RESULT.value,
            payload,
            room=dto.target_sid,
            namespace=MainNamespaces.ROOT.value,
        )

        print(f"[ProcessingResultEmitHandler] ✅ Emitted to receiver {dto.target_sid}")

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
        print(f"[ProcessingResultEmitHandler] DLQ MESSAGE RECEIVED")
        print(f"[ProcessingResultEmitHandler] Error Type: {error_info.get('error_type')}")
        print(
            f"[ProcessingResultEmitHandler] Error Message: {error_info.get('error_message')}"
        )
        print(
            f"[ProcessingResultEmitHandler] Original Topic: {error_info.get('original_topic')}"
        )
        print(f"[ProcessingResultEmitHandler] Retry Count: {error_info.get('retry_count')}")
        print(f"[ProcessingResultEmitHandler] Data: {data}")
        print(f"{'='*60}\n")
