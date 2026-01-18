"""
KafkaConfig - Configuration cho Kafka connections

Trách nhiệm:
- Centralize tất cả Kafka configurations
- Cung cấp defaults hợp lý
"""
from dataclasses import dataclass


@dataclass
class KafkaConfig:
    """
    Configuration cho Kafka Producer và Consumer.

    Attributes:
        bootstrap_servers (str): Kafka broker addresses
        auto_offset_reset (str): Consumer offset reset policy
        enable_auto_commit (bool): Tự động commit offset

    Example:
        config = KafkaConfig(bootstrap_servers="kafka:9092")
        factory = KafkaConsumerFactory(config)
    """

    bootstrap_servers: str = "localhost:9092"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
