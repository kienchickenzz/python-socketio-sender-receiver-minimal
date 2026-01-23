"""
KafkaEmitPublisher - Generic publisher cho emit events từ business logic consumer.

Chỉ cần 1 class duy nhất, topic được xác định tự động từ DTO
thông qua tính đa hình (polymorphism).
"""
from kafka import KafkaProducer

from src.kafka.consumer.shared.base.JsonSerializer import JsonSerializer
from src.kafka.consumer.shared.kafka_publisher.dto.KafkaEventDto import KafkaEventDto


class KafkaEmitPublisher:
    """
    Generic publisher cho emit events từ business logic consumer.

    Sử dụng đa hình: DTO tự biết topic của mình thông qua get_topic().
    Publisher chỉ cần gọi dto.get_topic() để biết publish vào đâu.

    Example:
        # Khởi tạo ở application root (run_consumer.py)
        producer = KafkaProducerFactory(config).get_instance()
        serializer = JsonSerializer()
        emit_publisher = KafkaEmitPublisher(producer, serializer)

        # Inject vào ServerRegistry
        registry = ServerRegistry(..., emit_publisher=emit_publisher)

        # Sử dụng trong handler
        dto = PairResultEmitEventDto(target_sid="sender-123", success=True, ...)
        emit_publisher.publish(dto)  # Tự động publish vào đúng topic
    """

    def __init__(self, producer: KafkaProducer, serializer: JsonSerializer) -> None:
        """
        Khởi tạo publisher với injected dependencies.

        Args:
            producer (KafkaProducer): Kafka producer instance
            serializer (JsonSerializer): Serializer để serialize DTO thành bytes
        """
        self._producer = producer
        self._serializer = serializer

    def publish(self, dto: KafkaEventDto) -> None:
        """
        Publish DTO vào Kafka topic.

        Topic được xác định tự động từ dto.get_topic().

        Args:
            dto (KafkaEventDto): DTO cần publish (phải kế thừa KafkaEventDto)
        """
        topic = dto.get_topic()
        data = dto.model_dump(by_alias=True)
        value = self._serializer.serialize(data)

        self._producer.send(topic=topic.value, value=value)

        print(f"[KafkaEmitPublisher] Published to {topic.value}: {data}")

    def flush(self, timeout: float | None = None) -> None:
        """
        Đợi tất cả messages được gửi.

        Args:
            timeout (float | None): Max wait time (seconds)
        """
        self._producer.flush(timeout=timeout)

    def close(self) -> None:
        """Đóng producer."""
        self._producer.close()
