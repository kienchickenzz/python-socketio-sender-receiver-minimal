"""
ReceiverReadyEventDto - DTO cho sự kiện receiver ready publish lên Kafka.

Được sử dụng khi receiver báo sẵn sàng nhận pairing,
SocketIO handler sẽ tạo DTO này và publish lên Kafka.
"""
from src.kafka.producer.shared.dto.KafkaEventDto import KafkaEventDto
from src.kafka.producer.shared.enum.KafkaTopic import KafkaTopic
from src.kafka.producer.server.enum.ServerTopic import ServerTopic


class ReceiverReadyEventDto(KafkaEventDto):
    """
    DTO cho sự kiện receiver ready publish lên Kafka.

    Được publish khi receiver báo sẵn sàng nhận pairing.
    Consumer sẽ nhận message này và thêm receiver vào pool.

    Fields:
        session_id: ID của receiver (client_sid)

    Example:
        dto = ReceiverReadyEventDto(session_id="receiver-123")
        publisher.publish(dto)  # Publish vào server.receiver-ready
    """

    session_id: str

    @classmethod
    def get_topic(cls) -> KafkaTopic:
        """
        Trả về topic cho sự kiện receiver ready.

        Returns:
            KafkaTopic: ServerTopic.RECEIVER_READY
        """
        return ServerTopic.RECEIVER_READY
