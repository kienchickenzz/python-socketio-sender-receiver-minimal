"""
JobStatus - Enum cho trạng thái của job

Định nghĩa các trạng thái có thể có của một job.
"""
from enum import Enum


class JobStatus(str, Enum):
    """
    Enum định nghĩa các trạng thái của job.

    Attributes:
        IN_PROGRESS: Job đang được worker xử lý
        COMPLETED: Job đã hoàn thành
        INCOMPLETE: Job chưa hoàn thành (đang chờ hoặc failed)
        CANCEL: Job bị hủy (ví dụ: sender disconnect)
    """

    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    INCOMPLETE = "INCOMPLETE"
    CANCEL = "CANCEL"
