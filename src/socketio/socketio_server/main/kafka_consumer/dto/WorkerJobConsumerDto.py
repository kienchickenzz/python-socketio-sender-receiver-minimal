"""
WorkerJobConsumerDto - DTO để parse message worker job từ Kafka.

Được sử dụng bởi WorkerJobEmitHandler để parse và validate
data nhận từ Kafka topic emit.worker-job.
"""
from typing import Any

from src.socketio.socketio_server.shared.kafka_consumer.dto.ConsumerEmitDto import (
    ConsumerEmitDto,
)


class WorkerJobConsumerDto(ConsumerEmitDto):
    """
    DTO để parse message worker job từ Kafka.

    Fields:
        target_sid: Socket ID của worker cần emit tới (= worker_id)
        pair_id: ID của cặp sender-receiver
        sender_id: ID của sender
        receiver_id: ID của receiver
        worker_id: ID của worker được chọn
        data: Dữ liệu cần xử lý (list of numbers)

    Example:
        async def handle(self, sio, data: dict):
            dto = WorkerJobConsumerDto(**data)
            await sio.emit("worker-job", {...}, to=dto.target_sid)
    """

    target_sid: str
    pair_id: str
    sender_id: str
    receiver_id: str
    worker_id: str
    data: list[Any]
