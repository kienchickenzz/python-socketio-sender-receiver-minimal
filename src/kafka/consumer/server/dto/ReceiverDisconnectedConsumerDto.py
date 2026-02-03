"""
ReceiverDisconnectedConsumerDto - DTO cho Kafka Consumer xử lý receiver disconnect

Parse message từ Kafka topic server.receiver-disconnected.
"""
from src.socketio.shared.dto.DtoBase import DtoBase


class ReceiverDisconnectedConsumerDto(DtoBase):
    """
    DTO cho Kafka Consumer xử lý receiver disconnect.

    Được parse từ message trong topic server.receiver-disconnected.

    Fields:
        receiver_id: ID của receiver đã disconnect
        timestamp: Thời điểm disconnect (ISO format)

    Example:
        # Kafka message
        {"receiverId": "receiver-456", "timestamp": "2024-01-15T10:30:00Z"}

        # Parse thành DTO
        dto = ReceiverDisconnectedConsumerDto(**data)
        receiver_id = dto.receiver_id  # "receiver-456"
    """

    receiver_id: str
    timestamp: str
