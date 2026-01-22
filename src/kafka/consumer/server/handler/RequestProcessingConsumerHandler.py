"""
RequestProcessingConsumerHandler - Consumer xử lý logic khi sender gửi request xử lý.

Nhận message từ Kafka và thực hiện:
1. Parse RequestProcessingConsumerDto từ message
2. Lấy ACTIVE workers từ WorkerManager
3. Chọn ngẫu nhiên 1 worker
4. Publish emit event để gửi worker-job tới worker
5. Thêm job vào JobManager để tracking
"""
import random
from typing import ClassVar
from pydantic import ValidationError

from src.kafka.consumer.server.dto.RequestProcessingConsumerDto import (
    RequestProcessingConsumerDto,
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
from src.kafka.consumer.server.kafka_publisher.dto.WorkerJobEmitDto import (
    WorkerJobEmitDto,
)

from src.socketio.socketio_server.main.manager.WorkerManager import WorkerManager
from src.socketio.socketio_server.main.manager.JobManager import JobManager
from src.socketio.socketio_server.main.enum.WorkerStatus import WorkerStatus


class RequestProcessingConsumerHandler(IEventHandler, IDLQHandler):
    """
    Consumer handler xử lý logic khi sender gửi request xử lý data.

    Flow xử lý:
        1. Parse RequestProcessingConsumerDto từ Kafka message
        2. Lấy danh sách ACTIVE workers từ WorkerManager
        3. Nếu không có worker available:
           - TODO: Publish emit event thông báo lỗi về sender
        4. Nếu có workers:
           - Chọn ngẫu nhiên 1 worker
           - Publish emit event (worker-job) tới worker
           - Thêm job vào JobManager để tracking

    Managers và emit_publisher được inject từ ServerRegistry.
    """

    # Main consumer config
    topic: ClassVar[KafkaTopic] = ServerTopic.REQUEST_PROCESSING
    group: ClassVar[ConsumerGroup] = ServerGroup.REQUEST_PROCESSING

    # DLQ consumer config
    dlq_topic: ClassVar[KafkaTopic] = ServerTopic.REQUEST_PROCESSING_DLQ
    dlq_group: ClassVar[ConsumerGroup] = ServerGroup.REQUEST_PROCESSING_DLQ

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
        Xử lý logic khi sender gửi request xử lý data.

        Args:
            data (dict): Message data từ Kafka chứa RequestProcessingDto
                - clientSid: Socket ID của sender
                - pairId: ID của cặp sender-receiver
                - senderId: ID của sender
                - receiverId: ID của receiver
                - data: Dữ liệu cần xử lý (list of numbers)

        Raises:
            ValidationError: Nếu data không đúng format → message vào DLQ
        """
        # 1. Parse DTO
        try:
            dto = RequestProcessingConsumerDto(**data)
        except ValidationError as e:
            print(f"[RequestProcessingConsumer] Invalid data format: {e}")
            raise  # Raise để message vào DLQ

        print(f"\n{'='*60}")
        print(f"[RequestProcessingConsumer] PROCESSING REQUEST")
        print(f"[RequestProcessingConsumer] Pair ID: {dto.pair_id}")
        print(f"[RequestProcessingConsumer] Sender ID: {dto.sender_id}")
        print(f"[RequestProcessingConsumer] Receiver ID: {dto.receiver_id}")
        print(f"[RequestProcessingConsumer] Data length: {len(dto.data)}")
        print(f"{'='*60}\n")

        # 2. Lấy tất cả workers đang ACTIVE
        active_workers = self._worker_manager.get_workers_by_status(WorkerStatus.ACTIVE)

        if not active_workers:
            print(f"[RequestProcessingConsumer] No ACTIVE workers available")
            # TODO: Publish emit event thông báo lỗi về sender
            return

        # 3. Chọn ngẫu nhiên 1 worker từ danh sách ACTIVE
        worker_id = random.choice(list(active_workers.keys()))

        print(f"[RequestProcessingConsumer] Selected worker: {worker_id}")
        print(f"[RequestProcessingConsumer] Total ACTIVE workers: {len(active_workers)}")

        # 4. Thêm job vào JobManager để tracking (tạo job_id trước)
        job_id = self._job_manager.add_job(
            sender_id=dto.sender_id,
            worker_id=worker_id,
            pair_id=dto.pair_id,
            input_data=dto.data,
        )

        print(f"[RequestProcessingConsumer] Created job tracking: {job_id}")

        # 5. Publish emit event (worker-job) tới worker với job_id
        worker_job_emit_dto = WorkerJobEmitDto(
            job_id=job_id,
            target_sid=worker_id,
            pair_id=dto.pair_id,
            sender_id=dto.sender_id,
            receiver_id=dto.receiver_id,
            worker_id=worker_id,
            data=dto.data,
        )
        self._emit_publisher.publish(worker_job_emit_dto)

        print(f"[RequestProcessingConsumer] Published worker-job emit event to {worker_id}")
        print(
            f"[RequestProcessingConsumer] Sender {dto.sender_id} queue size: "
            f"{self._job_manager.count_sender_jobs(dto.sender_id)}"
        )
        print(
            f"[RequestProcessingConsumer] Total active jobs: "
            f"{self._job_manager.count_all_jobs()}"
        )

        print(f"\n{'='*60}")
        print(f"[RequestProcessingConsumer] REQUEST PROCESSING HANDLED")
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
        print(f"[RequestProcessingConsumer] DLQ MESSAGE RECEIVED")
        print(f"[RequestProcessingConsumer] Error Type: {error_info.get('error_type')}")
        print(
            f"[RequestProcessingConsumer] Error Message: {error_info.get('error_message')}"
        )
        print(
            f"[RequestProcessingConsumer] Original Topic: {error_info.get('original_topic')}"
        )
        print(f"[RequestProcessingConsumer] Retry Count: {error_info.get('retry_count')}")
        print(f"[RequestProcessingConsumer] Data: {data}")
        print(f"{'='*60}\n")
