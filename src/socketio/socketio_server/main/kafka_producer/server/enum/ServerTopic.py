"""
ServerTopic - Kafka topics cho Server Producer

Định nghĩa các topics mà server sẽ publish events vào.
"""
from src.socketio.socketio_server.shared.kafka_producer.enum.KafkaTopic import KafkaTopic, BaseTopics


class ServerTopic(BaseTopics):
    """
    Kafka topics cho Server domain.

    Kế thừa từ BaseTopics và thêm các topics riêng cho server.

    Attributes:
        SENDER_DISCONNECTED: Topic cho sự kiện sender disconnect
        RECEIVER_DISCONNECTED: Topic cho sự kiện receiver disconnect
        RECEIVER_READY: Topic cho sự kiện receiver ready
        WORKER_ACTIVE: Topic cho sự kiện worker active
        WORKER_DISCONNECTED: Topic cho sự kiện worker disconnect
        SENDER_PAIR_REQUEST: Topic cho sự kiện sender yêu cầu pair
        REQUEST_PROCESSING: Topic cho sự kiện sender gửi request xử lý
        WORKER_RESULT: Topic cho sự kiện worker trả kết quả
    """

    SENDER_DISCONNECTED = KafkaTopic("server.sender-disconnected")
    RECEIVER_DISCONNECTED = KafkaTopic("server.receiver-disconnected")
    RECEIVER_READY = KafkaTopic("server.receiver-ready")
    WORKER_ACTIVE = KafkaTopic("server.worker-active")
    WORKER_DISCONNECTED = KafkaTopic("server.worker-disconnected")
    SENDER_PAIR_REQUEST = KafkaTopic("server.sender-pair-request")
    REQUEST_PROCESSING = KafkaTopic("server.request-processing")
    WORKER_RESULT = KafkaTopic("server.worker-result")
