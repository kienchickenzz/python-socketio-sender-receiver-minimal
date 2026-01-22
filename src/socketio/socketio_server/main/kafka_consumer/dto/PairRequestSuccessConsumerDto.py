"""
PairRequestSuccessConsumerDto - DTO để parse message pair request success từ Kafka.

Được sử dụng bởi PairRequestSuccessEmitHandler để parse và validate
data nhận từ Kafka topic emit.pair-request-success.
"""
from src.socketio.socketio_server.shared.kafka_consumer.dto.ConsumerEmitDto import (
    ConsumerEmitDto,
)


class PairRequestSuccessConsumerDto(ConsumerEmitDto):
    """
    DTO để parse message pair request success từ Kafka.

    Fields:
        target_sid: Socket ID của client cần emit tới
        pair_id: ID của pair được tạo
        sender_id: ID của sender
        receiver_id: ID của receiver

    Example:
        async def handle(self, sio, data: dict):
            dto = PairRequestSuccessConsumerDto(**data)
            await sio.emit("pair-request-success", {...}, to=dto.target_sid)
    """

    target_sid: str
    pair_id: str
    sender_id: str
    receiver_id: str
