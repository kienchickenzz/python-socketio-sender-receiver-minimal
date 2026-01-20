"""
IEmitHandler - Abstract base class cho tất cả Kafka emit handlers

Trách nhiệm:
- Define contract cho emit handlers
- Mỗi handler xử lý 1 topic cụ thể trong 1 consumer group
- Nhận AsyncServer instance để emit SocketIO events
"""
from abc import ABC, abstractmethod
from typing import ClassVar

from socketio import AsyncServer

from src.socketio.socketio_server.shared.kafka_consumer.enum.KafkaTopic import KafkaTopic
from src.socketio.socketio_server.shared.kafka_consumer.enum.ConsumerGroup import ConsumerGroup


class IEmitHandler(ABC):
    """
    Abstract base class cho Kafka emit handlers.

    Subclass phải:
    1. Set class attributes: topic (KafkaTopic), group (ConsumerGroup)
    2. Implement async handle(sio, data) method

    Example:
        class PairResultEmitHandler(IEmitHandler):
            topic = EmitTopic.PAIR_RESULT
            group = EmitGroup.PAIR_RESULT

            async def handle(self, sio: AsyncServer, data: dict) -> None:
                target_sid = data.get("target_sid")
                await sio.emit("pair_success", {"pair_id": "..."}, to=target_sid)
    """

    # Class attributes - MUST be set by subclass
    topic: ClassVar[KafkaTopic]
    group: ClassVar[ConsumerGroup]

    @abstractmethod
    async def handle(self, sio: AsyncServer, data: dict) -> None:
        """
        Handle event data từ Kafka và emit SocketIO event.

        Args:
            sio (AsyncServer): SocketIO AsyncServer instance để emit events
            data (dict): Event data từ Kafka message
        """
        pass
