"""
SenderPairRequestEventDto - DTO cho sự kiện sender pair request publish lên Kafka.

Được sử dụng khi sender yêu cầu pair với receiver,
SocketIO handler sẽ tạo DTO này và publish lên Kafka.
"""
from src.socketio.socketio_server.shared.kafka_producer.dto.KafkaEventDto import KafkaEventDto
from src.socketio.socketio_server.shared.kafka_producer.enum.KafkaTopic import KafkaTopic

from src.socketio.socketio_server.main.kafka_producer.server.enum.ServerTopic import ServerTopic


class SenderPairRequestEventDto(KafkaEventDto):
    """
    DTO cho sự kiện sender pair request publish lên Kafka.

    Được publish khi sender emit sender-pair-request event.
    Consumer sẽ nhận message này và thực hiện pairing logic.

    Fields:
        session_id: ID của sender (từ client emit)
        client_sid: Socket ID của sender (để emit kết quả về)

    Example:
        dto = SenderPairRequestEventDto(
            session_id="sender-123",
            client_sid="socket-abc"
        )
        publisher.publish(dto)  # Publish vào server.sender-pair-request
    """

    session_id: str
    client_sid: str

    @classmethod
    def get_topic(cls) -> KafkaTopic:
        """
        Trả về topic cho sự kiện sender pair request.

        Returns:
            KafkaTopic: ServerTopic.SENDER_PAIR_REQUEST
        """
        return ServerTopic.SENDER_PAIR_REQUEST
