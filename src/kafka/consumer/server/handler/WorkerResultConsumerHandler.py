"""
WorkerResultConsumerHandler - Consumer xử lý logic khi worker trả kết quả.

Nhận message từ Kafka và thực hiện:
1. Parse WorkerResultConsumerDto từ message
2. Tìm job theo sender_id và job_id
3. Update job output và status = COMPLETED
4. Xử lý cascade completed jobs (FIFO ordering)
5. Cập nhật worker status về ACTIVE
6. Publish emit events để gửi kết quả về receiver
"""
from typing import ClassVar
from pydantic import ValidationError

from src.kafka.consumer.server.dto.WorkerResultConsumerDto import (
    WorkerResultConsumerDto,
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
from src.kafka.consumer.server.kafka_publisher.dto.ProcessingResultEmitDto import (
    ProcessingResultEmitDto,
)

from src.socketio.socketio_server.main.manager.WorkerManager import WorkerManager
from src.socketio.socketio_server.main.manager.JobManager import JobManager
from src.socketio.socketio_server.main.enum.WorkerStatus import WorkerStatus
from src.socketio.socketio_server.main.enum.JobStatus import JobStatus


class WorkerResultConsumerHandler(IEventHandler, IDLQHandler):
    """
    Consumer handler xử lý logic khi worker trả kết quả.

    Flow xử lý:
        1. Parse WorkerResultConsumerDto từ Kafka message
        2. Tìm job theo sender_id và job_id (khoanh vùng tìm kiếm)
        3. Nếu không tìm thấy job:
           - Log warning và return
        4. Nếu tìm thấy job:
           - Update job output với result
           - Mark job status = COMPLETED
           - Xử lý cascade completed jobs (FIFO ordering)
           - Cập nhật worker status về ACTIVE
           - Publish emit events để gửi kết quả về receiver

    Managers và emit_publisher được inject từ ServerRegistry.
    """

    # Main consumer config
    topic: ClassVar[KafkaTopic] = ServerTopic.WORKER_RESULT
    group: ClassVar[ConsumerGroup] = ServerGroup.WORKER_RESULT

    # DLQ consumer config
    dlq_topic: ClassVar[KafkaTopic] = ServerTopic.WORKER_RESULT_DLQ
    dlq_group: ClassVar[ConsumerGroup] = ServerGroup.WORKER_RESULT_DLQ

    def __init__(
        self,
        emit_publisher: KafkaEmitPublisher,
        worker_manager: WorkerManager,
        job_manager: JobManager,
    ):
        """
        Khởi tạo handler với dependencies được inject từ bên ngoài.

        Args:
            emit_publisher (KafkaEmitPublisher): Publisher để publish emit events
            worker_manager (WorkerManager): Manager quản lý workers
            job_manager (JobManager): Manager quản lý jobs
        """
        self._emit_publisher = emit_publisher
        self._worker_manager = worker_manager
        self._job_manager = job_manager

    def handle(self, data: dict) -> None:
        """
        Xử lý logic khi worker trả kết quả.

        Args:
            data (dict): Message data từ Kafka chứa WorkerResultDto
                - clientSid: Socket ID của worker
                - jobId: ID của job
                - pairId: ID của cặp sender-receiver
                - senderId: ID của sender
                - receiverId: ID của receiver
                - workerId: ID của worker đã xử lý
                - originalData: Dữ liệu gốc
                - result: Kết quả đã xử lý

        Raises:
            ValidationError: Nếu data không đúng format → message vào DLQ
        """
        # 1. Parse DTO
        try:
            dto = WorkerResultConsumerDto(**data)
        except ValidationError as e:
            print(f"[WorkerResultConsumer] Invalid data format: {e}")
            raise  # Raise để message vào DLQ

        print(f"\n{'='*60}")
        print(f"[WorkerResultConsumer] PROCESSING WORKER RESULT")
        print(f"[WorkerResultConsumer] Job ID: {dto.job_id}")
        print(f"[WorkerResultConsumer] Pair ID: {dto.pair_id}")
        print(f"[WorkerResultConsumer] Sender ID: {dto.sender_id}")
        print(f"[WorkerResultConsumer] Receiver ID: {dto.receiver_id}")
        print(f"[WorkerResultConsumer] Worker ID: {dto.worker_id}")
        print(f"[WorkerResultConsumer] Original data: {dto.original_data}")
        print(f"[WorkerResultConsumer] Result: {dto.result}")
        print(f"{'='*60}\n")

        # 2. Tìm job theo sender_id (khoanh vùng) và job_id (định danh chính xác)
        job = self._job_manager.get_job_from_sender(dto.sender_id, dto.job_id)

        if not job:
            print(
                f"[WorkerResultConsumer] ⚠️ No matching job found for "
                f"sender {dto.sender_id} with job_id {dto.job_id}"
            )
            return

        # 3. Update job output và mark status = COMPLETED
        self._job_manager.update_job_output(job.id, dto.result)
        self._job_manager.update_job_status(job.id, JobStatus.COMPLETED)

        print(f"[WorkerResultConsumer] ✅ Marked job {job.id} as COMPLETED with output")

        # 4. Process completed jobs cascade - FIFO ordering
        jobs_to_emit = self._job_manager.process_completed_jobs(dto.sender_id, job.id)

        if not jobs_to_emit:
            print(
                f"[WorkerResultConsumer] ⏳ Job {job.id} is waiting for "
                f"previous jobs to complete"
            )
            print(f"[WorkerResultConsumer] Not emitting to receiver yet")
        else:
            # Emit tất cả jobs theo thứ tự FIFO
            print(
                f"[WorkerResultConsumer] 📤 {len(jobs_to_emit)} job(s) ready to emit "
                f"in FIFO order"
            )

            for completed_job in jobs_to_emit:
                # Publish emit event để gửi kết quả về receiver
                emit_dto = ProcessingResultEmitDto(
                    target_sid=dto.receiver_id,
                    job_id=completed_job.id,
                    pair_id=completed_job.pair_id,
                    sender_id=completed_job.sender_id,
                    worker_id=completed_job.worker_id,
                    original_data=completed_job.input,
                    result=completed_job.output or [],
                )
                self._emit_publisher.publish(emit_dto)

                print(
                    f"[WorkerResultConsumer] ✅ Published emit event for job "
                    f"{completed_job.id} to receiver {dto.receiver_id}"
                )

            print(
                f"[WorkerResultConsumer] Sender {dto.sender_id} queue size: "
                f"{self._job_manager.count_sender_jobs(dto.sender_id)}"
            )
            print(
                f"[WorkerResultConsumer] Total active jobs: "
                f"{self._job_manager.count_all_jobs()}"
            )

        # 5. Cập nhật trạng thái worker về ACTIVE
        if dto.worker_id:
            success = self._worker_manager.update_status(
                dto.worker_id, WorkerStatus.ACTIVE
            )
            if success:
                print(
                    f"[WorkerResultConsumer] 🔄 Updated worker {dto.worker_id} "
                    f"status back to ACTIVE"
                )
            else:
                print(
                    f"[WorkerResultConsumer] ⚠️ Failed to update worker "
                    f"{dto.worker_id} status"
                )

        print(f"\n{'='*60}")
        print(f"[WorkerResultConsumer] WORKER RESULT HANDLED")
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
        print(f"[WorkerResultConsumer] DLQ MESSAGE RECEIVED")
        print(f"[WorkerResultConsumer] Error Type: {error_info.get('error_type')}")
        print(
            f"[WorkerResultConsumer] Error Message: {error_info.get('error_message')}"
        )
        print(
            f"[WorkerResultConsumer] Original Topic: {error_info.get('original_topic')}"
        )
        print(f"[WorkerResultConsumer] Retry Count: {error_info.get('retry_count')}")
        print(f"[WorkerResultConsumer] Data: {data}")
        print(f"{'='*60}\n")
