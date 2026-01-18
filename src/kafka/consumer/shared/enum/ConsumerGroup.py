"""
ConsumerGroup - Dataclass-based consumer group system cho Kafka

Sử dụng dataclass để đảm bảo type-safe cho consumer groups.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class ConsumerGroup:
    """
    Immutable dataclass đại diện cho một Kafka consumer group.

    Attributes:
        value (str): Consumer group ID
        name (str): Human-readable name (auto-generated nếu không set)
    """
    value: str
    name: str = ""

    def __post_init__(self):
        """Auto-generate name from value if not provided."""
        if not self.name:
            object.__setattr__(self, 'name', self.value.upper().replace('-', '_').replace('.', '_'))

    def __str__(self) -> str:
        return self.value


class BaseGroups:
    """
    Base consumer groups chung cho Kafka.

    Các domain cụ thể sẽ kế thừa class này và thêm groups riêng.

    Example:
        class PredictionGroups(BaseGroups):
            WORKER = ConsumerGroup("prediction-worker")
            ANALYTICS = ConsumerGroup("prediction-analytics")
    """

    DEFAULT = ConsumerGroup("default-group")
