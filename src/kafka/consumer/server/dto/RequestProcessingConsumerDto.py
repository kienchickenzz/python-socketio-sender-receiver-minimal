"""
RequestProcessingConsumerDto - DTO để parse message request processing từ Kafka.

Được sử dụng bởi RequestProcessingConsumerHandler để parse và validate
data nhận từ Kafka topic server.request-processing.
"""
from typing import Any

from src.kafka.consumer.shared.dto.ConsumerEventDto import ConsumerEventDto


class RequestProcessingConsumerDto(ConsumerEventDto):
    """
    DTO để parse message request processing từ Kafka.

    Fields:
        client_sid: Socket ID của sender (để tracking)
        pair_id: ID của cặp sender-receiver
        sender_id: ID của sender
        receiver_id: ID của receiver
        data: Dữ liệu cần xử lý (list of numbers)

    Example:
        def handle(self, data: dict):
            dto = RequestProcessingConsumerDto(**data)
            # Select worker và dispatch job...
    """

    client_sid: str
    pair_id: str
    sender_id: str
    receiver_id: str
    data: list[Any]
