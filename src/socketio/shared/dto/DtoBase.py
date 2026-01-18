"""
DtoBase - Base class cho tất cả DTOs trong toàn bộ dự án
"""
from pydantic import BaseModel, ConfigDict
from humps import camelize


class DtoBase(BaseModel):
    """
    Base DTO class cho toàn bộ data transfer objects trong project.

    Tất cả DTOs phải kế thừa từ class này để có:
    - Auto validation với Pydantic
    - Auto serialization/deserialization
    - Immutable objects (frozen=True)
    - Snake case → Camel case conversion (alias_generator=camelize)

    Example:
        class UserDto(DtoBase):
            user_id: str
            user_name: str

        # Serialize to dict with camelCase
        dto = UserDto(user_id="123", user_name="John")
        dto.model_dump(by_alias=True)
        # → {"userId": "123", "userName": "John"}

        # Deserialize from dict with camelCase or snake_case
        UserDto(userId="123", userName="John")
        UserDto(user_id="123", user_name="John")
    """

    model_config = ConfigDict(
        frozen=True,  # Immutable objects
        alias_generator=camelize,  # Auto convert snake_case → camelCase
        populate_by_name=True,  # Accept both formats
        validate_assignment=True,  # Validate on assignment
        extra="ignore",  # Ignore unknown fields
        from_attributes=True,  # Create from object attributes
    )
