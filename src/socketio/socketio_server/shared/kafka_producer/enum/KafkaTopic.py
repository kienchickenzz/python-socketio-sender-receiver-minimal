"""
KafkaTopic - Dataclass-based topic system cho Kafka

Sử dụng dataclass thay vì Enum để cho phép kế thừa.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaTopic:
    """
    Immutable dataclass đại diện cho một Kafka topic.

    Attributes:
        value (str): Topic name
        name (str): Human-readable name (auto-generated nếu không set)
    """
    value: str
    name: str = ""

    def __post_init__(self):
        """Auto-generate name from value if not provided."""
        if not self.name:
            object.__setattr__(self, 'name', self.value.upper().replace('.', '_').replace('-', '_'))

    def __str__(self) -> str:
        return self.value


class BaseTopics:
    """
    Base topics chung cho Kafka.

    Các domain cụ thể sẽ kế thừa class này và thêm topics riêng.

    Example:
        class PredictionTopics(BaseTopics):
            REQUESTED = KafkaTopic("prediction.requested")
            COMPLETED = KafkaTopic("prediction.completed")
    """

    DLQ = KafkaTopic("dlq.events")
