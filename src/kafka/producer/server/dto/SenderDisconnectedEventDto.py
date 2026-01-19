"""
SenderDisconnectedEventDto - DTO cho sự kiện sender disconnect publish lên Kafka.

Được sử dụng khi sender disconnect,
SocketIO handler sẽ tạo DTO này và publish lên Kafka để trigger cleanup.
"""
from src.kafka.producer.shared.dto.KafkaEventDto import KafkaEventDto
from src.kafka.producer.shared.enum.KafkaTopic import KafkaTopic
from src.kafka.producer.server.enum.ServerTopic import ServerTopic


class SenderDisconnectedEventDto(KafkaEventDto):
    """
    DTO cho sự kiện sender disconnect publish lên Kafka.

    Được publish khi sender disconnect để trigger cleanup:
    - Set receiver về IDLE
    - Cancel tất cả jobs của pair
    - Xóa pair và sender

    Fields:
        sender_id: ID của sender (client_sid)
        timestamp: Thời điểm disconnect (ISO format)

    Example:
        dto = SenderDisconnectedEventDto(
            sender_id="sender-456",
            timestamp="2024-01-15T10:30:00Z"
        )
        publisher.publish(dto)  # Publish vào server.sender-disconnected
    """

    sender_id: str
    timestamp: str

    @classmethod
    def get_topic(cls) -> KafkaTopic:
        """
        Trả về topic cho sự kiện sender disconnected.

        Returns:
            KafkaTopic: ServerTopic.SENDER_DISCONNECTED
        """
        return ServerTopic.SENDER_DISCONNECTED
