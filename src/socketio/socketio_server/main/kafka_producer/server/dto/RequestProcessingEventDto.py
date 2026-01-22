"""
RequestProcessingEventDto - DTO cho sự kiện request processing publish lên Kafka.

Được sử dụng khi sender gửi request xử lý data,
SocketIO handler sẽ tạo DTO này và publish lên Kafka.
"""
from typing import Any

from src.socketio.socketio_server.shared.kafka_producer.dto.KafkaEventDto import KafkaEventDto
from src.socketio.socketio_server.shared.kafka_producer.enum.KafkaTopic import KafkaTopic

from src.socketio.socketio_server.main.kafka_producer.server.enum.ServerTopic import ServerTopic


class RequestProcessingEventDto(KafkaEventDto):
    """
    DTO cho sự kiện request processing publish lên Kafka.

    Được publish khi sender emit request-processing event.
    Consumer sẽ nhận message này và dispatch job cho worker.

    Fields:
        client_sid: Socket ID của sender (để tracking)
        pair_id: ID của cặp sender-receiver
        sender_id: ID của sender
        receiver_id: ID của receiver
        data: Dữ liệu cần xử lý (list of numbers)

    Example:
        dto = RequestProcessingEventDto(
            client_sid="socket-abc",
            pair_id="pair-123",
            sender_id="sender-123",
            receiver_id="receiver-456",
            data=[5, 3, 1, 4, 2]
        )
        publisher.publish(dto)
    """

    client_sid: str
    pair_id: str
    sender_id: str
    receiver_id: str
    data: list[Any]

    @classmethod
    def get_topic(cls) -> KafkaTopic:
        """
        Trả về topic cho sự kiện request processing.

        Returns:
            KafkaTopic: ServerTopic.REQUEST_PROCESSING
        """
        return ServerTopic.REQUEST_PROCESSING
