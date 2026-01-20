"""
EmitGroup - Consumer groups cho Emit Consumer

Định nghĩa các consumer groups cho emit domain.
"""
from src.socketio.socketio_server.shared.kafka_consumer.enum.ConsumerGroup import ConsumerGroup, BaseGroups


class EmitGroup(BaseGroups):
    """
    Consumer groups cho Emit domain.

    Kế thừa từ BaseGroups và thêm các groups riêng cho emit.

    Attributes:
        PAIR_RESULT: Group cho xử lý pair result
        PAIR_RESULT_DLQ: Group cho xử lý DLQ của pair result
        SENDER_DISCONNECTED: Group cho xử lý sender disconnected
        SENDER_DISCONNECTED_DLQ: Group cho xử lý DLQ của sender disconnected
    """

    PAIR_RESULT = ConsumerGroup("emit-pair-result-group")
    PAIR_RESULT_DLQ = ConsumerGroup("emit-pair-result-dlq-group")
    SENDER_DISCONNECTED = ConsumerGroup("emit-sender-disconnected-group")
    SENDER_DISCONNECTED_DLQ = ConsumerGroup("emit-sender-disconnected-dlq-group")
