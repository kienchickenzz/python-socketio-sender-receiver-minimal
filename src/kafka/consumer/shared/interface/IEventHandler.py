"""
IEventHandler - Abstract base class cho tất cả Kafka event handlers

Trách nhiệm:
- Define contract cho consumer handlers
- Mỗi handler xử lý 1 topic cụ thể trong 1 consumer group
"""
from abc import ABC, abstractmethod
from typing import ClassVar

from src.consumer.shared.enum.KafkaTopic import KafkaTopic
from src.consumer.shared.enum.ConsumerGroup import ConsumerGroup


class IEventHandler(ABC):
    """
    Abstract base class cho Kafka event handlers.

    Subclass phải:
    1. Set class attributes: topic (KafkaTopic), group (ConsumerGroup)
    2. Implement handle(data) method

    Example:
        class PredictionHandler(IEventHandler):
            topic = PredictionTopics.REQUESTED
            group = PredictionGroups.WORKER

            def handle(self, data: dict) -> Any:
                request_id = data.get("request_id")
                input_text = data.get("input_text")
                # Business logic here
                return {"status": "ok"}
    """

    # Class attributes - MUST be set by subclass
    topic: ClassVar[KafkaTopic]
    group: ClassVar[ConsumerGroup]

    @abstractmethod
    def handle(self, data: dict):
        """
        Handle event data từ Kafka.

        Args:
            data (dict): Event data từ Kafka message

        Returns:
            Any: Kết quả xử lý (optional, có thể dùng để publish event tiếp)
        """
        pass
