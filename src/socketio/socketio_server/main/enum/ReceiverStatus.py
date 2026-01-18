"""
ReceiverStatus - Enum cho trạng thái của receiver

Định nghĩa các trạng thái có thể có của một receiver.
"""
from enum import Enum


class ReceiverStatus(str, Enum):
    """
    Enum định nghĩa các trạng thái của receiver.

    Attributes:
        ACTIVE: Receiver đang hoạt động và sẵn sàng nhận dữ liệu
        IDLE: Receiver đang rảnh, chưa xử lý gì
    """

    ACTIVE = "ACTIVE"
    IDLE = "IDLE"
