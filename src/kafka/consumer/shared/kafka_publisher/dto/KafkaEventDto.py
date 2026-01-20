"""
KafkaEventDto - Base DTO cho các events publish lên Kafka.

Tất cả DTOs cần publish lên Kafka phải kế thừa class này
và implement get_topic() để xác định topic đích.
"""
from abc import abstractmethod

from pydantic import BaseModel, ConfigDict
from humps import camelize

from src.kafka.consumer.shared.kafka_publisher.enum.KafkaTopic import KafkaTopic


class KafkaEventDto(BaseModel):
    """
    Base DTO cho các Kafka producer events.

    Cung cấp:
    - Auto validation với Pydantic
    - Auto serialization/deserialization
    - Immutable objects (frozen=True)
    - Snake case → Camel case conversion (alias_generator=camelize)
    - get_topic() để xác định topic đích

    Example:
        class ReceiverReadyEventDto(KafkaEventDto):
            session_id: str

            @classmethod
            def get_topic(cls) -> KafkaTopic:
                return ServerTopic.RECEIVER_READY

        # Usage
        dto = ReceiverReadyEventDto(session_id="receiver-123")
        publisher.publish(dto)  # Topic tự động xác định
    """

    model_config = ConfigDict(
        frozen=True,
        alias_generator=camelize,
        populate_by_name=True,
        validate_assignment=True,
        extra="ignore",
        from_attributes=True,
    )

    @classmethod
    @abstractmethod
    def get_topic(cls) -> KafkaTopic:
        """
        Trả về topic mà DTO này sẽ được publish vào.

        Returns:
            KafkaTopic: Topic đích cho event này
        """
        pass
