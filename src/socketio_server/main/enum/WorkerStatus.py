"""
WorkerStatus - Enum cho trạng thái của worker

Định nghĩa các trạng thái có thể có của một worker.
"""
from enum import Enum


class WorkerStatus(str, Enum):
    """
    Enum định nghĩa các trạng thái của worker.

    Attributes:
        ACTIVE: Worker đang hoạt động và đang xử lý job
        IDLE: Worker đang rảnh, sẵn sàng nhận job mới
    """

    ACTIVE = "ACTIVE"
    IDLE = "IDLE"
