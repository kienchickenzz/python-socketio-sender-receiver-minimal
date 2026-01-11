"""
ReceiverData - Data model cho receiver

Model đơn giản lưu trữ thông tin trạng thái của receiver.
"""
from dataclasses import dataclass

from src.socketio_server.main.enum.ReceiverStatus import ReceiverStatus


@dataclass
class ReceiverData:
    """
    Data model cho mỗi receiver.

    Attributes:
        status: Trạng thái hiện tại của receiver (ACTIVE/IDLE)
    """

    status: ReceiverStatus

    def to_dict(self) -> dict:
        """
        Convert ReceiverData sang dict để serialize.

        Returns:
            Dict representation của ReceiverData
        """
        return {"status": self.status.value}
