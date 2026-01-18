"""
KafkaConsumerFactory - Factory để tạo KafkaConsumer cho handlers

Trách nhiệm:
- Tạo KafkaConsumer instance cho main handler
- Tạo KafkaConsumer instance cho DLQ handler
- Encapsulate config và serialization setup
"""
from kafka import KafkaConsumer

from src.consumer.infrastructure.KafkaConfig import KafkaConfig
from src.shared.base.JsonSerializer import JsonSerializer
from src.consumer.shared.interface.IEventHandler import IEventHandler
from src.consumer.shared.interface.IDLQHandler import IDLQHandler


class KafkaConsumerFactory:
    """
    Factory để tạo KafkaConsumer instances.

    Example:
        config = KafkaConfig(bootstrap_servers="localhost:9092")
        factory = KafkaConsumerFactory(config)

        handler = PredictionHandler()
        main_consumer = factory.create(handler)
        dlq_consumer = factory.create_dlq(handler)
    """

    # DLQ consumer poll interval dài hơn để tiết kiệm resource
    DLQ_POLL_TIMEOUT_MS = 5000

    def __init__(self, config: KafkaConfig) -> None:
        """
        Initialize factory với config.

        Args:
            config (KafkaConfig): Kafka configuration
        """
        self._config = config
        self._serializer = JsonSerializer()

    def create(self, handler: IEventHandler) -> KafkaConsumer:
        """
        Tạo KafkaConsumer cho main handler.

        Args:
            handler (IEventHandler): Handler với topic và group

        Returns:
            KafkaConsumer: Consumer instance cho main topic
        """
        return KafkaConsumer(
            handler.topic.value,
            bootstrap_servers=self._config.bootstrap_servers,
            group_id=handler.group.value,
            auto_offset_reset=self._config.auto_offset_reset,
            enable_auto_commit=self._config.enable_auto_commit,
            value_deserializer=lambda m: self._serializer.deserialize_raw(m),
        )

    def create_dlq(self, handler: IDLQHandler) -> KafkaConsumer:
        """
        Tạo KafkaConsumer cho DLQ handler.

        DLQ consumer có poll interval dài hơn để tiết kiệm resource.

        Args:
            handler (IDLQHandler): Handler với dlq_topic và dlq_group

        Returns:
            KafkaConsumer: Consumer instance cho DLQ topic
        """
        return KafkaConsumer(
            handler.dlq_topic.value,
            bootstrap_servers=self._config.bootstrap_servers,
            group_id=handler.dlq_group.value,
            auto_offset_reset=self._config.auto_offset_reset,
            enable_auto_commit=self._config.enable_auto_commit,
            value_deserializer=lambda m: self._serializer.deserialize_raw(m),
        )
