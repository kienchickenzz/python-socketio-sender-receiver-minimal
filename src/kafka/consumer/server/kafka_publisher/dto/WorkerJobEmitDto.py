"""
WorkerJobEmitDto - DTO cho emit event worker job.

Consumer publish DTO này để SocketIO server emit
worker-job event về worker.
"""
from typing import Any

from src.kafka.consumer.shared.kafka_publisher.dto.KafkaEventDto import KafkaEventDto
from src.kafka.consumer.shared.kafka_publisher.enum.KafkaTopic import KafkaTopic
from src.kafka.consumer.server.kafka_publisher.enum.EmitTopic import EmitTopic


class WorkerJobEmitDto(KafkaEventDto):
    """
    DTO cho emit event worker job.

    Chứa thông tin để SocketIO server emit job tới worker để xử lý.

    Fields:
        job_id: ID của job được tạo bởi JobManager
        target_sid: Socket ID của worker cần emit tới (= worker_id)
        pair_id: ID của cặp sender-receiver
        sender_id: ID của sender
        receiver_id: ID của receiver
        worker_id: ID của worker được chọn
        data: Dữ liệu cần xử lý (list of numbers)

    Example:
        dto = WorkerJobEmitDto(
            job_id="job-xyz",
            target_sid="worker-123",
            pair_id="pair-abc",
            sender_id="sender-456",
            receiver_id="receiver-789",
            worker_id="worker-123",
            data=[5, 3, 1, 4, 2]
        )
        emit_publisher.publish(dto)
    """

    job_id: str
    target_sid: str
    pair_id: str
    sender_id: str
    receiver_id: str
    worker_id: str
    data: list[Any]

    @classmethod
    def get_topic(cls) -> KafkaTopic:
        """
        Trả về topic cho emit event worker job.

        Returns:
            KafkaTopic: EmitTopic.WORKER_JOB
        """
        return EmitTopic.WORKER_JOB
