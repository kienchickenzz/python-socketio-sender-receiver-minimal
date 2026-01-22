"""
WorkerJobEmitHandler - Emit handler cho worker job events.

Nhận message từ Kafka topic emit.worker-job và emit
SocketIO event worker-job về worker.
"""
from typing import ClassVar
from pydantic import ValidationError

from socketio import AsyncServer

from src.socketio.shared.dto.processing import WorkerJobDto
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
from src.socketio.socketio_server.main.kafka_consumer.dto.WorkerJobConsumerDto import (
    WorkerJobConsumerDto,
)

from src.socketio.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio.socketio_server.main.enum.MainNamespace import MainNamespaces


class WorkerJobEmitHandler(IEmitHandler, IDLQHandler):
    """
    Emit handler cho worker job events.

    Flow xử lý:
        1. Nhận message từ Kafka topic emit.worker-job
        2. Parse WorkerJobConsumerDto từ message
        3. Emit SocketIO event worker-job về worker (target_sid)

    Handler này emit job tới worker để xử lý data.
    """

    # Main consumer config
    topic: ClassVar[KafkaTopic] = EmitTopic.WORKER_JOB
    group: ClassVar[ConsumerGroup] = EmitGroup.WORKER_JOB

    # DLQ consumer config
    dlq_topic: ClassVar[KafkaTopic] = EmitTopic.WORKER_JOB_DLQ
    dlq_group: ClassVar[ConsumerGroup] = EmitGroup.WORKER_JOB_DLQ

    async def handle(self, sio: AsyncServer, data: dict) -> None:
        """
        Emit SocketIO event worker-job về worker.

        Args:
            sio (AsyncServer): SocketIO AsyncServer instance để emit events
            data (dict): Message data từ Kafka chứa:
                - jobId: ID của job được tạo bởi JobManager
                - targetSid: Socket ID của worker cần emit tới
                - pairId: ID của cặp sender-receiver
                - senderId: ID của sender
                - receiverId: ID của receiver
                - workerId: ID của worker được chọn
                - data: Dữ liệu cần xử lý (list of numbers)

        Raises:
            ValidationError: Nếu data không đúng format → message vào DLQ
        """
        # 1. Parse DTO
        try:
            dto = WorkerJobConsumerDto(**data)
        except ValidationError as e:
            print(f"[WorkerJobEmitHandler] Invalid data format: {e}")
            raise  # Raise để message vào DLQ

        print(f"\n{'='*60}")
        print(f"[WorkerJobEmitHandler] EMITTING WORKER JOB")
        print(f"[WorkerJobEmitHandler] Job ID: {dto.job_id}")
        print(f"[WorkerJobEmitHandler] Target SID: {dto.target_sid}")
        print(f"[WorkerJobEmitHandler] Worker ID: {dto.worker_id}")
        print(f"[WorkerJobEmitHandler] Pair ID: {dto.pair_id}")
        print(f"[WorkerJobEmitHandler] Data length: {len(dto.data)}")
        print(f"{'='*60}\n")

        # 2. Tạo payload cho SocketIO emit
        payloadDto = WorkerJobDto(
            job_id=dto.job_id,
            pair_id=dto.pair_id,
            sender_id=dto.sender_id,
            receiver_id=dto.receiver_id,
            worker_id=dto.worker_id,
            data=dto.data,
        )
        payload = payloadDto.model_dump(by_alias=True)

        # 3. Emit SocketIO event về worker
        await sio.emit(
            MainEvents.WORKER_JOB.value,
            payload,
            room=dto.target_sid,
            namespace=MainNamespaces.ROOT.value,
        )

        print(f"[WorkerJobEmitHandler] Emitted to worker {dto.target_sid}")

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
        print(f"[WorkerJobEmitHandler] DLQ MESSAGE RECEIVED")
        print(f"[WorkerJobEmitHandler] Error Type: {error_info.get('error_type')}")
        print(
            f"[WorkerJobEmitHandler] Error Message: {error_info.get('error_message')}"
        )
        print(
            f"[WorkerJobEmitHandler] Original Topic: {error_info.get('original_topic')}"
        )
        print(f"[WorkerJobEmitHandler] Retry Count: {error_info.get('retry_count')}")
        print(f"[WorkerJobEmitHandler] Data: {data}")
        print(f"{'='*60}\n")
