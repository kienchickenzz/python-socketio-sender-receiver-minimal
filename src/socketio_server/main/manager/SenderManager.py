"""
SenderManager - Singleton manager quản lý active senders

Quản lý tập hợp các sender đang active, cho phép thêm, xóa và cập nhật trạng thái.
"""
from typing import Dict, Optional

from src.socketio_server.main.model.SenderData import SenderData
from src.socketio_server.main.enum.SenderStatus import SenderStatus


class SenderManager:
    """
    Singleton manager quản lý active senders.

    Structure của active senders:
    {
        "sender_id_1": SenderData(status=ACTIVE),
        "sender_id_2": SenderData(status=IDLE),
    }

    Usage:
        manager = SenderManager()
        manager.add_sender("sender_123", SenderStatus.ACTIVE)
        manager.update_status("sender_123", SenderStatus.IDLE)
        manager.remove_sender("sender_123")
    """

    _instance = None
    _senders: Dict[str, SenderData] = {}

    def __new__(cls):
        """
        Singleton pattern - đảm bảo chỉ có 1 instance duy nhất.

        Returns:
            SenderManager instance duy nhất
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._senders = {}
        return cls._instance

    def add_sender(
        self, sender_id: str, status: SenderStatus = SenderStatus.ACTIVE
    ):
        """
        Thêm sender vào pool.

        Args:
            sender_id: ID của sender
            status: Trạng thái ban đầu (default: ACTIVE)
        """
        self._senders[sender_id] = SenderData(status=status)
        print(f"[SenderManager] Added sender {sender_id} with status {status.value}")

    def remove_sender(self, sender_id: str) -> bool:
        """
        Xóa sender khỏi pool.

        Args:
            sender_id: ID của sender cần xóa

        Returns:
            True nếu xóa thành công, False nếu sender không tồn tại
        """
        if sender_id in self._senders:
            del self._senders[sender_id]
            print(f"[SenderManager] Removed sender {sender_id}")
            return True
        return False

    def update_status(self, sender_id: str, status: SenderStatus) -> bool:
        """
        Cập nhật status của sender.

        Args:
            sender_id: ID của sender
            status: Trạng thái mới

        Returns:
            True nếu cập nhật thành công, False nếu sender không tồn tại
        """
        if sender_id in self._senders:
            self._senders[sender_id].status = status
            print(
                f"[SenderManager] Updated sender {sender_id} status to {status.value}"
            )
            return True
        return False

    def get_sender(self, sender_id: str) -> Optional[SenderData]:
        """
        Lấy thông tin sender theo ID.

        Args:
            sender_id: ID của sender

        Returns:
            SenderData nếu tồn tại, None nếu không tìm thấy
        """
        return self._senders.get(sender_id)

    def get_all_senders(self) -> Dict[str, SenderData]:
        """
        Lấy tất cả senders.

        Returns:
            Dict chứa tất cả senders (copy để tránh modification)
        """
        return self._senders.copy()

    def get_senders_by_status(self, status: SenderStatus) -> Dict[str, SenderData]:
        """
        Lấy tất cả senders theo trạng thái.

        Args:
            status: Trạng thái cần filter

        Returns:
            Dict chứa các senders có trạng thái phù hợp
        """
        return {
            sender_id: data
            for sender_id, data in self._senders.items()
            if data.status == status
        }

    def count(self) -> int:
        """
        Đếm tổng số senders.

        Returns:
            Số lượng senders
        """
        return len(self._senders)

    def count_by_status(self, status: SenderStatus) -> int:
        """
        Đếm số senders theo trạng thái.

        Args:
            status: Trạng thái cần đếm

        Returns:
            Số lượng senders có trạng thái phù hợp
        """
        return sum(1 for data in self._senders.values() if data.status == status)
