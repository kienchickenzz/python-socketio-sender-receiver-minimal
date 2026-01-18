"""
WorkerData - Data model cho worker

Model đơn giản lưu trữ thông tin trạng thái của worker.
"""
from dataclasses import dataclass

from src.socketio.socketio_server.main.enum.WorkerStatus import WorkerStatus


@dataclass
class WorkerData:
    """
    Data model cho mỗi worker.

    Attributes:
        status: Trạng thái hiện tại của worker (ACTIVE/IDLE)
    """

    status: WorkerStatus

    def to_dict(self) -> dict:
        """
        Convert WorkerData sang dict để serialize.

        Returns:
            Dict representation của WorkerData
        """
        return {"status": self.status.value}
