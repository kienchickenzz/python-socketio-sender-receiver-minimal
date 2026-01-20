"""
ConsumerEmitDto - Base DTO cho các emit events nhận từ Kafka consumer.

Tất cả DTOs dùng để parse Kafka messages trong emit consumer phải kế thừa class này.
"""
from pydantic import BaseModel, ConfigDict
from humps import camelize


class ConsumerEmitDto(BaseModel):
    """
    Base DTO cho các Kafka emit consumer events.

    Cung cấp:
    - Auto validation với Pydantic
    - Auto serialization/deserialization
    - Immutable objects (frozen=True)
    - Snake case → Camel case conversion (alias_generator=camelize)

    Example:
        class PairResultEmitDto(ConsumerEmitDto):
            target_sid: str
            success: bool

        # Usage trong handler
        def handle(self, data: dict):
            dto = PairResultEmitDto(**data)
            target = dto.target_sid
    """

    model_config = ConfigDict(
        frozen=True,
        alias_generator=camelize,
        populate_by_name=True,
        validate_assignment=True,
        extra="ignore",
        from_attributes=True,
    )
