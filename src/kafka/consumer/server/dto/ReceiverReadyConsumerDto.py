"""
ReceiverReadyConsumerDto - DTO để parse message receiver ready từ Kafka.

Được sử dụng bởi ReceiverReadyConsumerHandler để parse và validate
data nhận từ Kafka topic server.receiver-ready.
"""
from src.kafka.consumer.shared.dto.ConsumerEventDto import ConsumerEventDto


class ReceiverReadyConsumerDto(ConsumerEventDto):
    """
    DTO để parse message receiver ready từ Kafka.

    Fields:
        session_id: ID của receiver (client_sid)

    Example:
        def handle(self, data: dict):
            dto = ReceiverReadyConsumerDto(**data)
            self._receiver_manager.add_receiver(dto.session_id, ReceiverStatus.IDLE)
    """

    session_id: str
