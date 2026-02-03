"""
ReceiverDisconnectedEventDto - DTO cho sự kiện receiver disconnect publish lên Kafka.

Được sử dụng khi receiver disconnect,
SocketIO handler sẽ tạo DTO này và publish lên Kafka để trigger cleanup.
"""
from src.socketio.socketio_server.shared.kafka_producer.dto.KafkaEventDto import KafkaEventDto
from src.socketio.socketio_server.shared.kafka_producer.enum.KafkaTopic import KafkaTopic

from src.socketio.socketio_server.main.kafka_producer.server.enum.ServerTopic import ServerTopic


class ReceiverDisconnectedEventDto(KafkaEventDto):
    """
    DTO cho sự kiện receiver disconnect publish lên Kafka.

    Được publish khi receiver disconnect để trigger cleanup:
    - Xóa receiver khỏi pool

    Fields:
        receiver_id: ID của receiver (client_sid)
        timestamp: Thời điểm disconnect (ISO format)

    Example:
        dto = ReceiverDisconnectedEventDto(
            receiver_id="receiver-456",
            timestamp="2024-01-15T10:30:00Z"
        )
        publisher.publish(dto)  # Publish vào server.receiver-disconnected
    """

    receiver_id: str
    timestamp: str

    @classmethod
    def get_topic(cls) -> KafkaTopic:
        """
        Trả về topic cho sự kiện receiver disconnected.

        Returns:
            KafkaTopic: ServerTopic.RECEIVER_DISCONNECTED
        """
        return ServerTopic.RECEIVER_DISCONNECTED
