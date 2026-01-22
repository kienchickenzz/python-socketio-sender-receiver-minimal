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
        PAIR_REQUEST_SUCCESS: Group cho xử lý pair request success
        PAIR_REQUEST_SUCCESS_DLQ: Group cho xử lý DLQ của pair request success
        PAIR_REQUEST_FAILED: Group cho xử lý pair request failed
        PAIR_REQUEST_FAILED_DLQ: Group cho xử lý DLQ của pair request failed
    """

    PAIR_REQUEST_SUCCESS = ConsumerGroup("emit-pair-request-success-group")
    PAIR_REQUEST_SUCCESS_DLQ = ConsumerGroup("emit-pair-request-success-dlq-group")
    PAIR_REQUEST_FAILED = ConsumerGroup("emit-pair-request-failed-group")
    PAIR_REQUEST_FAILED_DLQ = ConsumerGroup("emit-pair-request-failed-dlq-group")
