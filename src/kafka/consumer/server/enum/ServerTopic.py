"""
ServerTopic - Kafka topics cho Server Consumer

Định nghĩa các topics mà server consumer sẽ subscribe và consume.
"""
from src.kafka.consumer.shared.enum.KafkaTopic import KafkaTopic, BaseTopics


class ServerTopic(BaseTopics):
    """
    Kafka topics cho Server domain consumer.

    Kế thừa từ BaseTopics và thêm các topics riêng cho server.

    Attributes:
        SENDER_DISCONNECTED: Topic cho sự kiện sender disconnect
        SENDER_DISCONNECTED_DLQ: DLQ topic cho sender disconnect failures
        RECEIVER_READY: Topic cho sự kiện receiver ready
        RECEIVER_READY_DLQ: DLQ topic cho receiver ready failures
        WORKER_ACTIVE: Topic cho sự kiện worker active
        WORKER_ACTIVE_DLQ: DLQ topic cho worker active failures
        SENDER_PAIR_REQUEST: Topic cho sự kiện sender yêu cầu pair
        SENDER_PAIR_REQUEST_DLQ: DLQ topic cho sender pair request failures
        REQUEST_PROCESSING: Topic cho sự kiện sender gửi request xử lý
        REQUEST_PROCESSING_DLQ: DLQ topic cho request processing failures
    """

    SENDER_DISCONNECTED = KafkaTopic("server.sender-disconnected")
    SENDER_DISCONNECTED_DLQ = KafkaTopic("server.sender-disconnected.dlq")
    RECEIVER_READY = KafkaTopic("server.receiver-ready")
    RECEIVER_READY_DLQ = KafkaTopic("server.receiver-ready.dlq")
    WORKER_ACTIVE = KafkaTopic("server.worker-active")
    WORKER_ACTIVE_DLQ = KafkaTopic("server.worker-active.dlq")
    SENDER_PAIR_REQUEST = KafkaTopic("server.sender-pair-request")
    SENDER_PAIR_REQUEST_DLQ = KafkaTopic("server.sender-pair-request.dlq")
    REQUEST_PROCESSING = KafkaTopic("server.request-processing")
    REQUEST_PROCESSING_DLQ = KafkaTopic("server.request-processing.dlq")
