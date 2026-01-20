"""
ServerGroup - Consumer groups cho Server Consumer

Định nghĩa các consumer groups cho server domain.
"""
from src.kafka.consumer.shared.enum.ConsumerGroup import ConsumerGroup, BaseGroups


class ServerGroup(BaseGroups):
    """
    Consumer groups cho Server domain.

    Kế thừa từ BaseGroups và thêm các groups riêng cho server.

    Attributes:
        SENDER_DISCONNECT: Group cho xử lý sender disconnect
        SENDER_DISCONNECT_DLQ: Group cho xử lý DLQ của sender disconnect
        RECEIVER_READY: Group cho xử lý receiver ready
        RECEIVER_READY_DLQ: Group cho xử lý DLQ của receiver ready
        WORKER_ACTIVE: Group cho xử lý worker active
        WORKER_ACTIVE_DLQ: Group cho xử lý DLQ của worker active
    """

    SENDER_DISCONNECT = ConsumerGroup("server-sender-disconnect-group")
    SENDER_DISCONNECT_DLQ = ConsumerGroup("server-sender-disconnect-dlq-group")
    RECEIVER_READY = ConsumerGroup("server-receiver-ready-group")
    RECEIVER_READY_DLQ = ConsumerGroup("server-receiver-ready-dlq-group")
    WORKER_ACTIVE = ConsumerGroup("server-worker-active-group")
    WORKER_ACTIVE_DLQ = ConsumerGroup("server-worker-active-dlq-group")
