"""
ConsumerEventDto - Base DTO cho các events nhận từ Kafka consumer.

Tất cả DTOs dùng để parse Kafka messages trong consumer phải kế thừa class này.
"""
from pydantic import BaseModel, ConfigDict
from humps import camelize


class ConsumerEventDto(BaseModel):
    """
    Base DTO cho các Kafka consumer events.

    Cung cấp:
    - Auto validation với Pydantic
    - Auto serialization/deserialization
    - Immutable objects (frozen=True)
    - Snake case → Camel case conversion (alias_generator=camelize)

    Example:
        class ReceiverReadyConsumerDto(ConsumerEventDto):
            session_id: str

        # Usage trong handler
        def handle(self, data: dict):
            dto = ReceiverReadyConsumerDto(**data)
            receiver_id = dto.session_id
    """

    model_config = ConfigDict(
        frozen=True,
        alias_generator=camelize,
        populate_by_name=True,
        validate_assignment=True,
        extra="ignore",
        from_attributes=True,
    )
