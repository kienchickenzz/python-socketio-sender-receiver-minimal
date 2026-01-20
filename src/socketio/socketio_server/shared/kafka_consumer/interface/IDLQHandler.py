"""
IDLQHandler - Interface bắt buộc cho DLQ handling

Trách nhiệm:
- Define contract cho DLQ handling logic
- Mỗi handler PHẢI implement interface này cùng với IEmitHandler
"""
from abc import ABC, abstractmethod
from typing import ClassVar

from src.socketio.socketio_server.shared.kafka_consumer.enum.KafkaTopic import KafkaTopic
from src.socketio.socketio_server.shared.kafka_consumer.enum.ConsumerGroup import ConsumerGroup


class IDLQHandler(ABC):
    """
    Interface bắt buộc cho DLQ handling.

    Subclass phải:
    1. Set class attributes: dlq_topic, dlq_group
    2. Implement handle_dlq() method

    Example:
        class PairResultEmitHandler(IEmitHandler, IDLQHandler):
            # Main
            topic = EmitTopic.PAIR_RESULT
            group = EmitGroup.PAIR_RESULT

            # DLQ
            dlq_topic = EmitTopic.PAIR_RESULT_DLQ
            dlq_group = EmitGroup.PAIR_RESULT_DLQ

            def handle(self, data: dict):
                # Main emit logic
                ...

            def handle_dlq(self, data: dict, error_info: dict) -> None:
                # DLQ logic: retry, alert, log
                ...
    """

    # Class attributes - MUST be set by subclass
    dlq_topic: ClassVar[KafkaTopic]
    dlq_group: ClassVar[ConsumerGroup]

    @abstractmethod
    def handle_dlq(self, data: dict, error_info: dict) -> None:
        """
        Xử lý message từ DLQ.

        Args:
            data (dict): Original message data
            error_info (dict): Error information với keys:
                - error_message (str): Nội dung lỗi
                - error_type (str): Loại exception
                - timestamp (str): Thời điểm xảy ra lỗi
                - original_topic (str): Topic gốc
                - handler_name (str): Tên handler
                - retry_count (int): Số lần đã retry
        """
        pass
