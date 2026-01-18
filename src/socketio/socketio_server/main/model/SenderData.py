"""
SenderData - Data model cho sender

Model đơn giản lưu trữ thông tin trạng thái của sender.
"""
from dataclasses import dataclass

from src.socketio.socketio_server.main.enum.SenderStatus import SenderStatus


@dataclass
class SenderData:
    """
    Data model cho mỗi sender.

    Attributes:
        status: Trạng thái hiện tại của sender (ACTIVE/IDLE)
    """

    status: SenderStatus

    def to_dict(self) -> dict:
        """
        Convert SenderData sang dict để serialize.

        Returns:
            Dict representation của SenderData
        """
        return {"status": self.status.value}
