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
        WORKER_JOB: Group cho xử lý worker job events
        WORKER_JOB_DLQ: Group cho xử lý DLQ của worker job
        PROCESSING_RESULT: Group cho xử lý processing result events
        PROCESSING_RESULT_DLQ: Group cho xử lý DLQ của processing result
    """

    PAIR_REQUEST_SUCCESS = ConsumerGroup("emit-pair-request-success-group")
    PAIR_REQUEST_SUCCESS_DLQ = ConsumerGroup("emit-pair-request-success-dlq-group")
    PAIR_REQUEST_FAILED = ConsumerGroup("emit-pair-request-failed-group")
    PAIR_REQUEST_FAILED_DLQ = ConsumerGroup("emit-pair-request-failed-dlq-group")
    WORKER_JOB = ConsumerGroup("emit-worker-job-group")
    WORKER_JOB_DLQ = ConsumerGroup("emit-worker-job-dlq-group")
    PROCESSING_RESULT = ConsumerGroup("emit-processing-result-group")
    PROCESSING_RESULT_DLQ = ConsumerGroup("emit-processing-result-dlq-group")
