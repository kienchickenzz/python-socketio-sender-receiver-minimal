"""
WorkerDisconnectConsumerHandler - Consumer xử lý logic khi worker disconnect

Nhận message từ Kafka và thực hiện cleanup:
1. Xóa worker khỏi pool
"""
from typing import ClassVar
from pydantic import ValidationError

from src.kafka.consumer.server.dto.WorkerDisconnectedConsumerDto import WorkerDisconnectedConsumerDto

from src.kafka.consumer.shared.interface.IEventHandler import IEventHandler
from src.kafka.consumer.shared.interface.IDLQHandler import IDLQHandler
from src.kafka.consumer.shared.enum.KafkaTopic import KafkaTopic
from src.kafka.consumer.shared.enum.ConsumerGroup import ConsumerGroup
from src.kafka.consumer.server.enum.ServerTopic import ServerTopic
from src.kafka.consumer.server.enum.ServerGroup import ServerGroup

from src.socketio.socketio_server.main.manager.WorkerManager import WorkerManager


class WorkerDisconnectConsumerHandler(IEventHandler, IDLQHandler):
    """
    Consumer handler xử lý logic cleanup khi worker disconnect.

    Flow xử lý:
        1. Parse WorkerDisconnectedConsumerDto từ Kafka message
        2. Xóa worker khỏi pool

    WorkerManager được inject từ ServerRegistry.
    """

    # Main consumer config
    topic: ClassVar[KafkaTopic] = ServerTopic.WORKER_DISCONNECTED
    group: ClassVar[ConsumerGroup] = ServerGroup.WORKER_DISCONNECT

    # DLQ consumer config
    dlq_topic: ClassVar[KafkaTopic] = ServerTopic.WORKER_DISCONNECTED_DLQ
    dlq_group: ClassVar[ConsumerGroup] = ServerGroup.WORKER_DISCONNECT_DLQ

    def __init__(self, worker_manager: WorkerManager):
        """
        Khởi tạo handler với WorkerManager được inject từ bên ngoài.

        Args:
            worker_manager (WorkerManager): Manager quản lý workers
        """
        self._worker_manager = worker_manager

    def handle(self, data: dict):
        """
        Xử lý logic cleanup khi worker disconnect.

        Args:
            data: Message data từ Kafka chứa WorkerDisconnectedDto

        Raises:
            ValidationError: Nếu data không đúng format → message vào DLQ
        """
        # 1. Parse DTO
        try:
            dto = WorkerDisconnectedConsumerDto(**data)
        except ValidationError as e:
            print(f"[WorkerDisconnectConsumer] Invalid data format: {e}")
            raise  # Raise để message vào DLQ

        worker_id = dto.worker_id

        print(f"\n{'='*60}")
        print(f"[WorkerDisconnectConsumer] PROCESSING WORKER DISCONNECT")
        print(f"[WorkerDisconnectConsumer] Worker ID: {worker_id}")
        print(f"[WorkerDisconnectConsumer] Timestamp: {dto.timestamp}")
        print(f"{'='*60}\n")

        # 2. Xóa worker khỏi pool
        success = self._worker_manager.remove_worker(worker_id)
        if success:
            print(f"[WorkerDisconnectConsumer] Removed worker {worker_id}")
        else:
            print(f"[WorkerDisconnectConsumer] Worker {worker_id} not found in manager")

        print(f"\n{'='*60}")
        print(f"[WorkerDisconnectConsumer] WORKER DISCONNECT PROCESSED SUCCESSFULLY")
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
        print(f"[WorkerDisconnectConsumer] DLQ MESSAGE RECEIVED")
        print(f"[WorkerDisconnectConsumer] Error Type: {error_info.get('error_type')}")
        print(f"[WorkerDisconnectConsumer] Error Message: {error_info.get('error_message')}")
        print(f"[WorkerDisconnectConsumer] Original Topic: {error_info.get('original_topic')}")
        print(f"[WorkerDisconnectConsumer] Retry Count: {error_info.get('retry_count')}")
        print(f"[WorkerDisconnectConsumer] Data: {data}")
        print(f"{'='*60}\n")
