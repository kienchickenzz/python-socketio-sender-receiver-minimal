"""
WorkerDisconnectedEventDto - DTO cho sự kiện worker disconnect publish lên Kafka.

Được sử dụng khi worker disconnect,
SocketIO handler sẽ tạo DTO này và publish lên Kafka để trigger cleanup.
"""
from src.socketio.socketio_server.shared.kafka_producer.dto.KafkaEventDto import KafkaEventDto
from src.socketio.socketio_server.shared.kafka_producer.enum.KafkaTopic import KafkaTopic

from src.socketio.socketio_server.main.kafka_producer.server.enum.ServerTopic import ServerTopic


class WorkerDisconnectedEventDto(KafkaEventDto):
    """
    DTO cho sự kiện worker disconnect publish lên Kafka.

    Được publish khi worker disconnect để trigger cleanup:
    - Xóa worker khỏi pool

    Fields:
        worker_id: ID của worker (client_sid)
        timestamp: Thời điểm disconnect (ISO format)

    Example:
        dto = WorkerDisconnectedEventDto(
            worker_id="worker-456",
            timestamp="2024-01-15T10:30:00Z"
        )
        publisher.publish(dto)  # Publish vào server.worker-disconnected
    """

    worker_id: str
    timestamp: str

    @classmethod
    def get_topic(cls) -> KafkaTopic:
        """
        Trả về topic cho sự kiện worker disconnected.

        Returns:
            KafkaTopic: ServerTopic.WORKER_DISCONNECTED
        """
        return ServerTopic.WORKER_DISCONNECTED
