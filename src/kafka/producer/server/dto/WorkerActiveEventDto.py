"""
WorkerActiveEventDto - DTO cho sự kiện worker active publish lên Kafka.

Được sử dụng khi worker báo active sau khi nhận session ID,
SocketIO handler sẽ tạo DTO này và publish lên Kafka.
"""
from src.kafka.producer.shared.dto.KafkaEventDto import KafkaEventDto
from src.kafka.producer.shared.enum.KafkaTopic import KafkaTopic
from src.kafka.producer.server.enum.ServerTopic import ServerTopic


class WorkerActiveEventDto(KafkaEventDto):
    """
    DTO cho sự kiện worker active publish lên Kafka.

    Được publish khi worker báo active sau khi connect.
    Consumer sẽ nhận message này và thêm/update worker trong pool.

    Fields:
        session_id: ID của worker (từ client emit)

    Example:
        dto = WorkerActiveEventDto(session_id="worker-123")
        publisher.publish(dto)  # Publish vào server.worker-active
    """

    session_id: str

    @classmethod
    def get_topic(cls) -> KafkaTopic:
        """
        Trả về topic cho sự kiện worker active.

        Returns:
            KafkaTopic: ServerTopic.WORKER_ACTIVE
        """
        return ServerTopic.WORKER_ACTIVE
