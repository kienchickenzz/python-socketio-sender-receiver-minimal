"""
WorkerDisconnectedConsumerDto - DTO cho Kafka Consumer xử lý worker disconnect

Parse message từ Kafka topic server.worker-disconnected.
"""
from src.socketio.shared.dto.DtoBase import DtoBase


class WorkerDisconnectedConsumerDto(DtoBase):
    """
    DTO cho Kafka Consumer xử lý worker disconnect.

    Được parse từ message trong topic server.worker-disconnected.

    Fields:
        worker_id: ID của worker đã disconnect
        timestamp: Thời điểm disconnect (ISO format)

    Example:
        # Kafka message
        {"workerId": "worker-456", "timestamp": "2024-01-15T10:30:00Z"}

        # Parse thành DTO
        dto = WorkerDisconnectedConsumerDto(**data)
        worker_id = dto.worker_id  # "worker-456"
    """

    worker_id: str
    timestamp: str
