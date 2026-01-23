"""
SenderDisconnectedEmitDto - DTO cho emit event sender disconnected.

Consumer publish DTO này để SocketIO server emit
sender-disconnected event về receiver.
"""
from src.kafka.consumer.shared.kafka_publisher.dto.KafkaEventDto import KafkaEventDto
from src.kafka.consumer.shared.kafka_publisher.enum.KafkaTopic import KafkaTopic
from src.kafka.consumer.server.kafka_publisher.enum.EmitTopic import EmitTopic


class SenderDisconnectedEmitDto(KafkaEventDto):
    """
    DTO cho emit event sender disconnected.

    Chứa thông tin để SocketIO server emit thông báo sender ngắt kết nối về receiver.

    Fields:
        target_sid: Socket ID của receiver cần emit tới
        sender_id: ID của sender đã ngắt kết nối
        pair_id: ID của cặp sender-receiver

    Example:
        dto = SenderDisconnectedEmitDto(
            target_sid="receiver-123",
            sender_id="sender-456",
            pair_id="pair-xyz"
        )
        emit_publisher.publish(dto)
    """

    target_sid: str
    sender_id: str
    pair_id: str

    @classmethod
    def get_topic(cls) -> KafkaTopic:
        """
        Trả về topic cho emit event sender disconnected.

        Returns:
            KafkaTopic: EmitTopic.SENDER_DISCONNECTED
        """
        return EmitTopic.SENDER_DISCONNECTED
