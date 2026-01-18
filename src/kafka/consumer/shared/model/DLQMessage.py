"""
DLQMessage - Model cho Dead Letter Queue message

Trách nhiệm:
- Wrap original message với error information
- Serialize/deserialize cho Kafka
"""
from dataclasses import dataclass, field, asdict
from datetime import datetime


@dataclass
class ErrorInfo:
    """
    Thông tin về lỗi xảy ra.

    Attributes:
        error_message (str): Nội dung lỗi
        error_type (str): Loại exception (e.g., "ValueError")
        timestamp (str): Thời điểm xảy ra lỗi (ISO format)
        original_topic (str): Topic gốc của message
        handler_name (str): Tên handler xử lý message
        retry_count (int): Số lần đã retry
    """

    error_message: str
    error_type: str
    original_topic: str
    handler_name: str
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    retry_count: int = 0

    def to_dict(self) -> dict:
        """
        Convert to dict.

        Returns:
            dict: Dictionary representation
        """
        return asdict(self)

    @classmethod
    def from_exception(
        cls,
        exception: Exception,
        original_topic: str,
        handler_name: str,
        retry_count: int = 0,
    ) -> "ErrorInfo":
        """
        Tạo ErrorInfo từ exception.

        Args:
            exception (Exception): Exception đã xảy ra
            original_topic (str): Topic gốc
            handler_name (str): Tên handler
            retry_count (int): Số lần đã retry

        Returns:
            ErrorInfo: Instance mới
        """
        return cls(
            error_message=str(exception),
            error_type=type(exception).__name__,
            original_topic=original_topic,
            handler_name=handler_name,
            retry_count=retry_count,
        )


@dataclass
class DLQMessage:
    """
    Wrapper cho message gửi vào DLQ topic.

    Attributes:
        original_message (dict): Message gốc
        error_info (ErrorInfo): Thông tin lỗi
    """

    original_message: dict
    error_info: ErrorInfo

    def to_dict(self) -> dict:
        """
        Convert to dict để serialize.

        Returns:
            dict: Dictionary representation
        """
        return {
            "original_message": self.original_message,
            "error_info": self.error_info.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DLQMessage":
        """
        Tạo DLQMessage từ dict.

        Args:
            data (dict): Dictionary với keys original_message, error_info

        Returns:
            DLQMessage: Instance mới
        """
        return cls(
            original_message=data["original_message"],
            error_info=ErrorInfo(**data["error_info"]),
        )
