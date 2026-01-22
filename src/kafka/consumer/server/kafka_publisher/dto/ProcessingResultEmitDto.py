"""
ProcessingResultEmitDto - DTO cho emit event processing result.

Consumer publish DTO này để SocketIO server emit
processing-result event về receiver.
"""
from typing import Any

from src.kafka.consumer.shared.kafka_publisher.dto.KafkaEventDto import KafkaEventDto
from src.kafka.consumer.shared.kafka_publisher.enum.KafkaTopic import KafkaTopic
from src.kafka.consumer.server.kafka_publisher.enum.EmitTopic import EmitTopic


class ProcessingResultEmitDto(KafkaEventDto):
    """
    DTO cho emit event processing result.

    Chứa thông tin để SocketIO server emit kết quả xử lý về receiver.

    Fields:
        target_sid: Socket ID của receiver cần emit tới
        job_id: ID của job đã hoàn thành
        pair_id: ID của cặp sender-receiver
        sender_id: ID của sender
        worker_id: ID của worker đã xử lý
        original_data: Dữ liệu gốc
        result: Kết quả đã xử lý (sorted data)

    Example:
        dto = ProcessingResultEmitDto(
            target_sid="receiver-123",
            job_id="job-abc",
            pair_id="pair-xyz",
            sender_id="sender-456",
            worker_id="worker-789",
            original_data=[5, 3, 1, 4, 2],
            result=[1, 2, 3, 4, 5]
        )
        emit_publisher.publish(dto)
    """

    target_sid: str
    job_id: str
    pair_id: str
    sender_id: str
    worker_id: str
    original_data: list[Any]
    result: list[Any]

    @classmethod
    def get_topic(cls) -> KafkaTopic:
        """
        Trả về topic cho emit event processing result.

        Returns:
            KafkaTopic: EmitTopic.PROCESSING_RESULT
        """
        return EmitTopic.PROCESSING_RESULT
