"""
ServerTopic - Kafka topics cho Server Producer

Định nghĩa các topics mà server sẽ publish events vào.
"""
from src.kafka.producer.shared.enum.KafkaTopic import KafkaTopic, BaseTopics


class ServerTopic(BaseTopics):
    """
    Kafka topics cho Server domain.

    Kế thừa từ BaseTopics và thêm các topics riêng cho server.

    Attributes:
        SENDER_DISCONNECTED: Topic cho sự kiện sender disconnect
        RECEIVER_READY: Topic cho sự kiện receiver ready
    """

    SENDER_DISCONNECTED = KafkaTopic("server.sender-disconnected")
    RECEIVER_READY = KafkaTopic("server.receiver-ready")
