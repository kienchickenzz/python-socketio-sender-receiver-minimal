"""
WorkerActiveConsumerDto - DTO để parse message worker active từ Kafka.

Được sử dụng bởi WorkerActiveConsumerHandler để parse và validate
data nhận từ Kafka topic server.worker-active.
"""
from src.kafka.consumer.shared.dto.ConsumerEventDto import ConsumerEventDto


class WorkerActiveConsumerDto(ConsumerEventDto):
    """
    DTO để parse message worker active từ Kafka.

    Fields:
        session_id: ID của worker

    Example:
        def handle(self, data: dict):
            dto = WorkerActiveConsumerDto(**data)
            self._worker_manager.add_worker(dto.session_id, WorkerStatus.ACTIVE)
    """

    session_id: str
