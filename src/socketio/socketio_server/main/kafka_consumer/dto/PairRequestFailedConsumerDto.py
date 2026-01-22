"""
PairRequestFailedConsumerDto - DTO để parse message pair request failed từ Kafka.

Được sử dụng bởi PairRequestFailedEmitHandler để parse và validate
data nhận từ Kafka topic emit.pair-request-failed.
"""
from src.socketio.socketio_server.shared.kafka_consumer.dto.ConsumerEmitDto import (
    ConsumerEmitDto,
)


class PairRequestFailedConsumerDto(ConsumerEmitDto):
    """
    DTO để parse message pair request failed từ Kafka.

    Fields:
        target_sid: Socket ID của client cần emit tới
        sender_id: ID của sender
        reason: Lý do pair thất bại

    Example:
        async def handle(self, sio, data: dict):
            dto = PairRequestFailedConsumerDto(**data)
            await sio.emit("pair-request-failed", {...}, to=dto.target_sid)
    """

    target_sid: str
    sender_id: str
    reason: str
