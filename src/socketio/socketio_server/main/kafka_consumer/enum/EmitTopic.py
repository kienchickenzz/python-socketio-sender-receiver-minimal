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
        PAIR_REQUEST_SUCCESS: Topic cho kết quả pairing thành công
        PAIR_REQUEST_SUCCESS_DLQ: DLQ topic cho pair success failures
        PAIR_REQUEST_FAILED: Topic cho kết quả pairing thất bại
        PAIR_REQUEST_FAILED_DLQ: DLQ topic cho pair failed failures
    """

    PAIR_REQUEST_SUCCESS = KafkaTopic("emit.pair-request-success")
    PAIR_REQUEST_SUCCESS_DLQ = KafkaTopic("emit.pair-request-success.dlq")
    PAIR_REQUEST_FAILED = KafkaTopic("emit.pair-request-failed")
    PAIR_REQUEST_FAILED_DLQ = KafkaTopic("emit.pair-request-failed.dlq")
