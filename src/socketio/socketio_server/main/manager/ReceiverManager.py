"""
ReceiverManager - Singleton manager quản lý active receivers

Quản lý tập hợp các receiver đang active, cho phép thêm, xóa và cập nhật trạng thái.
"""
from typing import Optional

from src.socketio.socketio_server.main.model.ReceiverData import ReceiverData
from src.socketio.socketio_server.main.enum.ReceiverStatus import ReceiverStatus


class ReceiverManager:
    """
    Singleton manager quản lý active receivers.

    Structure của active receivers:
    {
        "receiver_id_1": ReceiverData(status=ACTIVE),
        "receiver_id_2": ReceiverData(status=IDLE),
    }

    Usage:
        manager = ReceiverManager()
        manager.add_receiver("receiver_123", ReceiverStatus.ACTIVE)
        manager.update_status("receiver_123", ReceiverStatus.IDLE)
        manager.remove_receiver("receiver_123")
    """

    _instance = None
    _receivers: dict[str, ReceiverData] = {}

    def __new__(cls):
        """
        Singleton pattern - đảm bảo chỉ có 1 instance duy nhất.

        Returns:
            ReceiverManager instance duy nhất
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._receivers = {}
        return cls._instance

    def add_receiver(
        self, receiver_id: str, status: ReceiverStatus = ReceiverStatus.ACTIVE
    ):
        """
        Thêm receiver vào pool.

        Args:
            receiver_id: ID của receiver
            status: Trạng thái ban đầu (default: ACTIVE)
        """
        self._receivers[receiver_id] = ReceiverData(status=status)
        print(f"[ReceiverManager] Added receiver {receiver_id} with status {status.value}")

    def remove_receiver(self, receiver_id: str) -> bool:
        """
        Xóa receiver khỏi pool.

        Args:
            receiver_id: ID của receiver cần xóa

        Returns:
            True nếu xóa thành công, False nếu receiver không tồn tại
        """
        if receiver_id in self._receivers:
            del self._receivers[receiver_id]
            print(f"[ReceiverManager] Removed receiver {receiver_id}")
            return True
        return False

    def update_status(self, receiver_id: str, status: ReceiverStatus) -> bool:
        """
        Cập nhật status của receiver.

        Args:
            receiver_id: ID của receiver
            status: Trạng thái mới

        Returns:
            True nếu cập nhật thành công, False nếu receiver không tồn tại
        """
        if receiver_id in self._receivers:
            self._receivers[receiver_id].status = status
            print(
                f"[ReceiverManager] Updated receiver {receiver_id} status to {status.value}"
            )
            return True
        return False

    def get_receiver(self, receiver_id: str) -> Optional[ReceiverData]:
        """
        Lấy thông tin receiver theo ID.

        Args:
            receiver_id: ID của receiver

        Returns:
            ReceiverData nếu tồn tại, None nếu không tìm thấy
        """
        return self._receivers.get(receiver_id)

    def get_all_receivers(self) -> dict[str, ReceiverData]:
        """
        Lấy tất cả receivers.

        Returns:
            Dict chứa tất cả receivers (copy để tránh modification)
        """
        return self._receivers.copy()

    def get_receivers_by_status(self, status: ReceiverStatus) -> dict[str, ReceiverData]:
        """
        Lấy tất cả receivers theo trạng thái.

        Args:
            status: Trạng thái cần filter

        Returns:
            Dict chứa các receivers có trạng thái phù hợp
        """
        return {
            receiver_id: data
            for receiver_id, data in self._receivers.items()
            if data.status == status
        }

    def count(self) -> int:
        """
        Đếm tổng số receivers.

        Returns:
            Số lượng receivers
        """
        return len(self._receivers)

    def count_by_status(self, status: ReceiverStatus) -> int:
        """
        Đếm số receivers theo trạng thái.

        Args:
            status: Trạng thái cần đếm

        Returns:
            Số lượng receivers có trạng thái phù hợp
        """
        return sum(1 for data in self._receivers.values() if data.status == status)
