"""
PairRequestFailedEmitDto - DTO cho emit event pair request failed.

Consumer publish DTO này để SocketIO server emit
pair-request-failed event về client.
"""
from src.kafka.consumer.shared.kafka_publisher.dto.KafkaEventDto import KafkaEventDto
from src.kafka.consumer.shared.kafka_publisher.enum.KafkaTopic import KafkaTopic
from src.kafka.consumer.server.kafka_publisher.enum.EmitTopic import EmitTopic


class PairRequestFailedEmitDto(KafkaEventDto):
    """
    DTO cho emit event pair request failed.

    Chứa thông tin để SocketIO server emit về client khi pair thất bại.

    Fields:
        target_sid: Socket ID của client cần emit tới
        sender_id: ID của sender
        reason: Lý do pair thất bại

    Example:
        dto = PairRequestFailedEmitDto(
            target_sid="socket-abc",
            sender_id="sender-123",
            reason="No available receiver"
        )
        emit_publisher.publish(dto)
    """

    target_sid: str
    sender_id: str
    reason: str

    @classmethod
    def get_topic(cls) -> KafkaTopic:
        """
        Trả về topic cho emit event pair request failed.

        Returns:
            KafkaTopic: EmitTopic.PAIR_REQUEST_FAILED
        """
        return EmitTopic.PAIR_REQUEST_FAILED
