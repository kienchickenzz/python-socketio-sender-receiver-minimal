"""
SenderDisconnectedConsumerDto - DTO để parse message sender disconnected từ Kafka.

Được sử dụng bởi SenderDisconnectConsumerHandler để parse và validate
data nhận từ Kafka topic server.sender-disconnected.
"""
from src.kafka.consumer.shared.dto.ConsumerEventDto import ConsumerEventDto


class SenderDisconnectedConsumerDto(ConsumerEventDto):
    """
    DTO để parse message sender disconnected từ Kafka.

    Fields:
        sender_id: ID của sender đã disconnect (client_sid)
        timestamp: Thời điểm disconnect (ISO format)

    Example:
        def handle(self, data: dict):
            dto = SenderDisconnectedConsumerDto(**data)
            self._sender_manager.remove_sender(dto.sender_id)
    """

    sender_id: str
    timestamp: str
