"""
SenderPairRequestConsumerDto - DTO để parse message sender pair request từ Kafka.

Được sử dụng bởi SenderPairRequestConsumerHandler để parse và validate
data nhận từ Kafka topic server.sender-pair-request.
"""
from src.kafka.consumer.shared.dto.ConsumerEventDto import ConsumerEventDto


class SenderPairRequestConsumerDto(ConsumerEventDto):
    """
    DTO để parse message sender pair request từ Kafka.

    Fields:
        session_id: ID của sender (từ client emit)
        client_sid: Socket ID của sender (để emit kết quả về)

    Example:
        def handle(self, data: dict):
            dto = SenderPairRequestConsumerDto(**data)
            # Xử lý pairing logic...
    """

    session_id: str
    client_sid: str
