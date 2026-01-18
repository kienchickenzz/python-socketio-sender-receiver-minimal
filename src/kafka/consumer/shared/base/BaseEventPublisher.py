"""
BaseEventPublisher - Base class cung cấp publish() generic

Trách nhiệm:
- Cung cấp concrete implementation cho publish() (generic, reusable)
- Nhận KafkaProducer và Serializer qua constructor (DI)
- Không có abstract methods

Note:
- Domain publishers sẽ kế thừa CẢ HAI: BaseEventPublisher và IEventPublisher
- BaseEventPublisher: cung cấp publish() implementation
- IEventPublisher: định nghĩa create_event() contract
"""
from kafka import KafkaProducer

from src.consumer.shared.enum.KafkaTopic import KafkaTopic
from src.shared.base.JsonSerializer import JsonSerializer


class BaseEventPublisher:
    """
    Base class cho publishers - cung cấp generic publish() implementation.

    Chứa:
    - __init__() để inject dependencies
    - publish() concrete method (generic, reusable)
    - flush() và close() để quản lý lifecycle

    Không có:
    - create_event() (sẽ được define trong IEventPublisher interface)

    Usage:
        producer = KafkaProducerFactory.create()
        serializer = JsonSerializer()

        class MyPublisher(BaseEventPublisher, IEventPublisher):
            topic = MyTopics.SOME_TOPIC

            def create_event(self, payload, key=None):
                # implement domain-specific logic
                ...

        publisher = MyPublisher(producer, serializer)
    """

    def __init__(self, producer: KafkaProducer, serializer: JsonSerializer) -> None:
        """
        Initialize base publisher với injected dependencies.

        Args:
            producer (KafkaProducer): Kafka producer instance
            serializer (JsonSerializer): Serializer instance
        """
        self._producer = producer
        self._serializer = serializer

    def publish(self, topic: KafkaTopic, data: dict, key: str | None = None) -> None:
        """
        Publish event data vào topic (CONCRETE implementation).

        Generic implementation - reusable cho tất cả domains.
        Subclasses KHÔNG cần override method này.

        Args:
            topic (KafkaTopic): Topic để publish
            data (dict): Event data
            key (str | None): Partition key (optional)
        """
        value = self._serializer.serialize(data)
        key_bytes = key.encode() if key else None
        self._producer.send(topic=topic.value, key=key_bytes, value=value)

    def flush(self, timeout: float | None = None) -> None:
        """
        Đợi tất cả messages được gửi.

        Args:
            timeout (float | None): Max wait time
        """
        self._producer.flush(timeout=timeout)

    def close(self) -> None:
        """Đóng producer."""
        self._producer.close()
