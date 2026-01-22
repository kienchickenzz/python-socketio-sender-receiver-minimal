"""
PairRequestSuccessEmitDto - DTO cho emit event pair request success.

Consumer publish DTO này để SocketIO server emit
pair-request-success event về client.
"""
from src.kafka.consumer.shared.kafka_publisher.dto.KafkaEventDto import KafkaEventDto
from src.kafka.consumer.shared.kafka_publisher.enum.KafkaTopic import KafkaTopic
from src.kafka.consumer.server.kafka_publisher.enum.EmitTopic import EmitTopic


class PairRequestSuccessEmitDto(KafkaEventDto):
    """
    DTO cho emit event pair request success.

    Chứa thông tin để SocketIO server emit về client khi pair thành công.

    Fields:
        target_sid: Socket ID của client cần emit tới
        pair_id: ID của pair được tạo
        sender_id: ID của sender
        receiver_id: ID của receiver

    Example:
        dto = PairRequestSuccessEmitDto(
            target_sid="socket-abc",
            pair_id="pair-123",
            sender_id="sender-123",
            receiver_id="receiver-456"
        )
        emit_publisher.publish(dto)
    """

    target_sid: str
    pair_id: str
    sender_id: str
    receiver_id: str

    @classmethod
    def get_topic(cls) -> KafkaTopic:
        """
        Trả về topic cho emit event pair request success.

        Returns:
            KafkaTopic: EmitTopic.PAIR_REQUEST_SUCCESS
        """
        return EmitTopic.PAIR_REQUEST_SUCCESS
