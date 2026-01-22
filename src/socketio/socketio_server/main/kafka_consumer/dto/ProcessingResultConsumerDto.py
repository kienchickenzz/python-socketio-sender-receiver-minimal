"""
ProcessingResultConsumerDto - DTO để parse message processing result từ Kafka.

Được sử dụng bởi ProcessingResultEmitHandler để parse và validate
data nhận từ Kafka topic emit.processing-result.
"""
from typing import Any

from src.socketio.socketio_server.shared.kafka_consumer.dto.ConsumerEmitDto import (
    ConsumerEmitDto,
)


class ProcessingResultConsumerDto(ConsumerEmitDto):
    """
    DTO để parse message processing result từ Kafka.

    Fields:
        target_sid: Socket ID của receiver cần emit tới
        job_id: ID của job đã hoàn thành
        pair_id: ID của cặp sender-receiver
        sender_id: ID của sender
        worker_id: ID của worker đã xử lý
        original_data: Dữ liệu gốc
        result: Kết quả đã xử lý (sorted data)

    Example:
        async def handle(self, sio, data: dict):
            dto = ProcessingResultConsumerDto(**data)
            await sio.emit("processing-result", {...}, to=dto.target_sid)
    """

    target_sid: str
    job_id: str
    pair_id: str
    sender_id: str
    worker_id: str
    original_data: list[Any]
    result: list[Any]
