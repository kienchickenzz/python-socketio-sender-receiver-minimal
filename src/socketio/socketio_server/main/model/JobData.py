"""
JobData - Data model cho active job

Model lưu trữ thông tin về job đang được xử lý.
"""
from dataclasses import dataclass
from typing import Any, Optional

from src.socketio.socketio_server.main.enum.JobStatus import JobStatus


@dataclass
class JobData:
    """
    Data model cho mỗi active job.

    Attributes:
        id: ID duy nhất của job (UUID)
        worker_id: ID của worker đang xử lý job
        sender_id: ID của sender đã gửi request
        pair_id: ID của cặp sender-receiver
        input: Dữ liệu đầu vào cần xử lý
        output: Kết quả đã xử lý (None nếu chưa hoàn thành)
        status: Trạng thái hiện tại của job (IN_PROGRESS, COMPLETED, INCOMPLETE)
    """

    id: str
    worker_id: str
    sender_id: str
    pair_id: str
    input: list[Any]
    output: Optional[list[Any]]
    status: JobStatus

    def to_dict(self) -> dict:
        """
        Convert JobData sang dict để serialize.

        Returns:
            Dict representation của JobData
        """
        return {
            "id": self.id,
            "worker_id": self.worker_id,
            "sender_id": self.sender_id,
            "pair_id": self.pair_id,
            "input": self.input,
            "output": self.output,
            "status": self.status.value,
        }
