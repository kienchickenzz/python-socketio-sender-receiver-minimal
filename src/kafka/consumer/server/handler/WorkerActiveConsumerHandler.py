"""
WorkerActiveConsumerHandler - Consumer xử lý logic khi worker báo active

Nhận message từ Kafka và thực hiện:
1. Parse WorkerActiveConsumerDto từ message
2. Nếu worker chưa có → thêm mới với status ACTIVE
3. Nếu worker đã có và IDLE → update sang ACTIVE
4. Nếu worker đã ACTIVE → không làm gì
"""
from typing import ClassVar
from pydantic import ValidationError

from src.kafka.consumer.server.dto.WorkerActiveConsumerDto import WorkerActiveConsumerDto

from src.kafka.consumer.shared.interface.IEventHandler import IEventHandler
from src.kafka.consumer.shared.interface.IDLQHandler import IDLQHandler
from src.kafka.consumer.shared.enum.KafkaTopic import KafkaTopic
from src.kafka.consumer.shared.enum.ConsumerGroup import ConsumerGroup
from src.kafka.consumer.server.enum.ServerTopic import ServerTopic
from src.kafka.consumer.server.enum.ServerGroup import ServerGroup

from src.socketio.socketio_server.main.manager.WorkerManager import WorkerManager
from src.socketio.socketio_server.main.enum.WorkerStatus import WorkerStatus


class WorkerActiveConsumerHandler(IEventHandler, IDLQHandler):
    """
    Consumer handler xử lý logic khi worker báo active.

    Flow xử lý:
        1. Parse WorkerActiveConsumerDto từ Kafka message
        2. Check xem worker đã tồn tại trong manager chưa
        3. Nếu chưa có → thêm mới với status ACTIVE
        4. Nếu đã có và IDLE → update sang ACTIVE
        5. Nếu đã ACTIVE → không làm gì

    Managers được inject từ ServerRegistry để đảm bảo clear ownership.
    """

    # Main consumer config
    topic: ClassVar[KafkaTopic] = ServerTopic.WORKER_ACTIVE
    group: ClassVar[ConsumerGroup] = ServerGroup.WORKER_ACTIVE

    # DLQ consumer config
    dlq_topic: ClassVar[KafkaTopic] = ServerTopic.WORKER_ACTIVE_DLQ
    dlq_group: ClassVar[ConsumerGroup] = ServerGroup.WORKER_ACTIVE_DLQ

    def __init__(self, worker_manager: WorkerManager):
        """
        Khởi tạo handler với WorkerManager được inject từ bên ngoài.

        Args:
            worker_manager (WorkerManager): Manager quản lý workers
        """
        self._worker_manager = worker_manager

    def handle(self, data: dict) -> None:
        """
        Xử lý logic khi worker báo active.

        Args:
            data (dict): Message data từ Kafka chứa WorkerActiveConsumerDto
                - sessionId: ID của worker

        Raises:
            ValidationError: Nếu data không đúng format → message vào DLQ
        """
        # 1. Parse DTO
        try:
            dto = WorkerActiveConsumerDto(**data)
        except ValidationError as e:
            print(f"[WorkerActiveConsumer] Invalid data format: {e}")
            raise  # Raise để message vào DLQ

        session_id = dto.session_id

        print(f"\n{'='*60}")
        print(f"[WorkerActiveConsumer] 🔄 PROCESSING WORKER ACTIVE")
        print(f"[WorkerActiveConsumer] Session ID: {session_id}")
        print(f"{'='*60}\n")

        # 2. Check xem worker đã tồn tại trong manager chưa
        existing_worker = self._worker_manager.get_worker(session_id)

        if existing_worker is None:
            # Worker chưa có → thêm mới với status ACTIVE
            self._worker_manager.add_worker(session_id, WorkerStatus.ACTIVE)
            print(f"[WorkerActiveConsumer] ✅ NEW WORKER REGISTERED")
            print(f"[WorkerActiveConsumer] Worker ID: {session_id}")
            print(f"[WorkerActiveConsumer] Status: ACTIVE")
            print(f"[WorkerActiveConsumer] Total workers: {self._worker_manager.count()}")

        else:
            # Worker đã tồn tại → check status
            if existing_worker.status == WorkerStatus.IDLE:
                # Update từ IDLE sang ACTIVE
                self._worker_manager.update_status(session_id, WorkerStatus.ACTIVE)
                print(f"[WorkerActiveConsumer] 🔄 WORKER STATUS UPDATED")
                print(f"[WorkerActiveConsumer] Worker ID: {session_id}")
                print(f"[WorkerActiveConsumer] Old Status: IDLE")
                print(f"[WorkerActiveConsumer] New Status: ACTIVE")

            elif existing_worker.status == WorkerStatus.ACTIVE:
                # Đã ACTIVE rồi → không làm gì
                print(f"[WorkerActiveConsumer] Worker {session_id} is already ACTIVE, no action needed")

        print(f"\n{'='*60}")
        print(f"[WorkerActiveConsumer] ✅ WORKER ACTIVE PROCESSED SUCCESSFULLY")
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
        print(f"[WorkerActiveConsumer] ⚠️ DLQ MESSAGE RECEIVED")
        print(f"[WorkerActiveConsumer] Error Type: {error_info.get('error_type')}")
        print(f"[WorkerActiveConsumer] Error Message: {error_info.get('error_message')}")
        print(f"[WorkerActiveConsumer] Original Topic: {error_info.get('original_topic')}")
        print(f"[WorkerActiveConsumer] Retry Count: {error_info.get('retry_count')}")
        print(f"[WorkerActiveConsumer] Data: {data}")
        print(f"{'='*60}\n")

        # TODO: Implement retry logic hoặc alert
