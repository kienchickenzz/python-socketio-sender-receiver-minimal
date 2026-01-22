"""
WorkerResultConsumerDto - DTO để parse message worker result từ Kafka.

Được sử dụng bởi WorkerResultConsumerHandler để parse và validate
data nhận từ Kafka topic server.worker-result.
"""
from typing import Any

from src.kafka.consumer.shared.dto.ConsumerEventDto import ConsumerEventDto


class WorkerResultConsumerDto(ConsumerEventDto):
    """
    DTO để parse message worker result từ Kafka.

    Fields:
        client_sid: Socket ID của worker (để tracking)
        job_id: ID của job được tạo bởi JobManager
        pair_id: ID của cặp sender-receiver
        sender_id: ID của sender
        receiver_id: ID của receiver
        worker_id: ID của worker đã xử lý
        original_data: Dữ liệu gốc
        result: Kết quả đã xử lý (sorted data)

    Example:
        def handle(self, data: dict):
            dto = WorkerResultConsumerDto(**data)
            # Tìm job, update status, emit kết quả...
    """

    client_sid: str
    job_id: str
    pair_id: str
    sender_id: str
    receiver_id: str
    worker_id: str
    original_data: list[Any]
    result: list[Any]
