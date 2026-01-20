"""
EmitTopic - Kafka topics cho Emit Consumer

Định nghĩa các topics mà emit consumer sẽ subscribe và consume.
"""
from src.socketio.socketio_server.shared.kafka_consumer.enum.KafkaTopic import KafkaTopic, BaseTopics


class EmitTopic(BaseTopics):
    """
    Kafka topics cho Emit domain consumer.

    Kế thừa từ BaseTopics và thêm các topics riêng cho emit events.

    Attributes:
        PAIR_RESULT: Topic cho kết quả pairing
        PAIR_RESULT_DLQ: DLQ topic cho pair result failures
        SENDER_DISCONNECTED: Topic cho thông báo sender disconnect
        SENDER_DISCONNECTED_DLQ: DLQ topic cho sender disconnect failures
    """

    PAIR_RESULT = KafkaTopic("emit.pair-result")
    PAIR_RESULT_DLQ = KafkaTopic("emit.pair-result.dlq")
    SENDER_DISCONNECTED = KafkaTopic("emit.sender-disconnected")
    SENDER_DISCONNECTED_DLQ = KafkaTopic("emit.sender-disconnected.dlq")
