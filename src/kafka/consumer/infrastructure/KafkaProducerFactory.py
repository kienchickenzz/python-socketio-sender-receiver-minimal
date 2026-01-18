"""
KafkaProducerFactory - Factory để tạo KafkaProducer

Trách nhiệm:
- Tạo KafkaProducer instance
- Singleton pattern để reuse producer
"""
from kafka import KafkaProducer

from src.consumer.infrastructure.KafkaConfig import KafkaConfig


class KafkaProducerFactory:
    """
    Factory để tạo KafkaProducer instance.

    Hỗ trợ singleton để reuse producer across publishers.

    Example:
        config = KafkaConfig(bootstrap_servers="localhost:9092")
        factory = KafkaProducerFactory(config)
        producer = factory.create()
    """

    _instance: KafkaProducer | None = None

    def __init__(self, config: KafkaConfig) -> None:
        """
        Initialize factory với config.

        Args:
            config (KafkaConfig): Kafka configuration
        """
        self._config = config

    def create(self) -> KafkaProducer:
        """
        Tạo KafkaProducer instance mới.

        Returns:
            KafkaProducer: Producer instance
        """
        return KafkaProducer(
            bootstrap_servers=self._config.bootstrap_servers,
        )

    def get_instance(self) -> KafkaProducer:
        """
        Lấy singleton KafkaProducer instance.

        Returns:
            KafkaProducer: Singleton producer
        """
        if KafkaProducerFactory._instance is None:
            KafkaProducerFactory._instance = self.create()
        return KafkaProducerFactory._instance

    @classmethod
    def close_instance(cls) -> None:
        """Đóng singleton instance."""
        if cls._instance is not None:
            cls._instance.close()
            cls._instance = None
