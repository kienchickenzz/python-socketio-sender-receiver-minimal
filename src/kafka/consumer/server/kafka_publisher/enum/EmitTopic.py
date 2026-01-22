"""
EmitTopic - Kafka topics cho emit events từ consumer về SocketIO server.

Định nghĩa các topics mà consumer sẽ publish emit events vào,
SocketIO server sẽ subscribe và emit về clients.
"""
from src.kafka.consumer.shared.kafka_publisher.enum.KafkaTopic import KafkaTopic, BaseTopics


class EmitTopic(BaseTopics):
    """
    Kafka topics cho emit events từ consumer.

    Các topics này được consumer publish khi cần SocketIO server
    emit events về clients.

    Attributes:
        PAIR_REQUEST_SUCCESS: Emit kết quả pair thành công
        PAIR_REQUEST_FAILED: Emit kết quả pair thất bại
        WORKER_JOB: Emit job tới worker để xử lý
        PROCESSING_RESULT: Emit kết quả xử lý về receiver
    """

    PAIR_REQUEST_SUCCESS = KafkaTopic("emit.pair-request-success")
    PAIR_REQUEST_FAILED = KafkaTopic("emit.pair-request-failed")
    WORKER_JOB = KafkaTopic("emit.worker-job")
    PROCESSING_RESULT = KafkaTopic("emit.processing-result")
