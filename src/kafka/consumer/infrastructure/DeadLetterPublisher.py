"""
DeadLetterPublisher - Publisher chung cho tất cả DLQ events

Trách nhiệm:
- Publish failed messages vào DLQ topics
- Thread-safe (KafkaProducer is thread-safe)
- Được dùng chung bởi tất cả handlers trong Registry
"""
from kafka import KafkaProducer

from src.consumer.shared.enum.KafkaTopic import KafkaTopic
from src.shared.base.JsonSerializer import JsonSerializer
from src.consumer.shared.model.DLQMessage import DLQMessage


class DeadLetterPublisher:
    """
    Publisher chung cho tất cả DLQ events.

    Thread-safe: Có thể được dùng bởi nhiều consumer threads cùng lúc.

    Example:
        producer = KafkaProducerFactory(config).create()
        serializer = JsonSerializer()
        dlq_publisher = DeadLetterPublisher(producer, serializer)

        # Trong Registry khi handle() fail
        dlq_message = DLQMessage(original_message, error_info)
        dlq_publisher.publish(handler.dlq_topic, dlq_message)
    """

    def __init__(self, producer: KafkaProducer, serializer: JsonSerializer) -> None:
        """
        Initialize DeadLetterPublisher.

        Args:
            producer (KafkaProducer): Kafka producer instance
            serializer (JsonSerializer): Serializer instance
        """
        self._producer = producer
        self._serializer = serializer

    def publish(self, dlq_topic: KafkaTopic, dlq_message: DLQMessage) -> None:
        """
        Publish failed message vào DLQ topic.

        Args:
            dlq_topic (KafkaTopic): DLQ topic để publish
            dlq_message (DLQMessage): Message đã wrap với error info
        """
        value = self._serializer.serialize(dlq_message.to_dict())
        self._producer.send(topic=dlq_topic.value, value=value)

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
