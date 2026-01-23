"""
SenderDisconnectedConsumerDto - DTO để parse message sender disconnected từ Kafka.

Được sử dụng bởi SenderDisconnectedEmitHandler để parse và validate
data nhận từ Kafka topic emit.sender-disconnected.
"""
from src.socketio.socketio_server.shared.kafka_consumer.dto.ConsumerEmitDto import (
    ConsumerEmitDto,
)


class SenderDisconnectedConsumerDto(ConsumerEmitDto):
    """
    DTO để parse message sender disconnected từ Kafka.

    Fields:
        target_sid: Socket ID của receiver cần emit tới
        sender_id: ID của sender đã ngắt kết nối
        pair_id: ID của cặp sender-receiver

    Example:
        async def handle(self, sio, data: dict):
            dto = SenderDisconnectedConsumerDto(**data)
            await sio.emit("sender-disconnected", {...}, to=dto.target_sid)
    """

    target_sid: str
    sender_id: str
    pair_id: str
