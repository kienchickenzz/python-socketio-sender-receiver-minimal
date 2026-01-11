"""
PairManager - Singleton manager quản lý active sender-receiver pairs

Quản lý tập hợp các cặp sender-receiver đang active với auto-increment ID.
"""
import uuid
from typing import Dict, Optional

from src.socketio_server.main.model.PairData import PairData


class PairManager:
    """
    Singleton manager quản lý active sender-receiver pairs.

    Structure của active pairs:
    {
        1: PairData(id=1, sender_id="sender_123", receiver_id="receiver_456"),
        2: PairData(id=2, sender_id="sender_789", receiver_id="receiver_012"),
    }

    Usage:
        manager = PairManager()
        pair_id = manager.add_pair("sender_123", "receiver_456")
        pair = manager.get_pair(pair_id)
        manager.remove_pair(pair_id)
    """

    _instance = None
    _pairs: Dict[uuid.UUID, PairData] = {}

    def __new__(cls):
        """
        Singleton pattern - đảm bảo chỉ có 1 instance duy nhất.

        Returns:
            PairManager instance duy nhất
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._pairs = {}
        return cls._instance

    def add_pair(self, sender_id: str, receiver_id: str) -> uuid.UUID:
        """
        Thêm một cặp sender-receiver mới vào pool.

        Args:
            sender_id: ID của sender
            receiver_id: ID của receiver

        Returns:
            int: ID của pair vừa tạo
        """
        pair_id = uuid.uuid4()
        self._pairs[pair_id] = PairData(
            id=pair_id, sender_id=sender_id, receiver_id=receiver_id
        )
        print(
            f"[PairManager] Created pair #{pair_id}: sender={sender_id}, receiver={receiver_id}"
        )
        return pair_id

    def remove_pair(self, pair_id: uuid.UUID) -> bool:
        """
        Xóa một pair khỏi pool.

        Args:
            pair_id: ID của pair cần xóa

        Returns:
            True nếu xóa thành công, False nếu pair không tồn tại
        """
        if pair_id in self._pairs:
            pair = self._pairs[pair_id]
            del self._pairs[pair_id]
            print(
                f"[PairManager] Removed pair #{pair_id}: sender={pair.sender_id}, receiver={pair.receiver_id}"
            )
            return True
        return False

    def get_pair(self, pair_id: uuid.UUID) -> Optional[PairData]:
        """
        Lấy thông tin pair theo ID.

        Args:
            pair_id: ID của pair

        Returns:
            PairData nếu tồn tại, None nếu không tìm thấy
        """
        return self._pairs.get(pair_id)

    def get_pair_by_sender(self, sender_id: str) -> Optional[PairData]:
        """
        Tìm pair theo sender ID.

        Args:
            sender_id: ID của sender

        Returns:
            PairData nếu tìm thấy, None nếu không tìm thấy
        """
        for pair in self._pairs.values():
            if pair.sender_id == sender_id:
                return pair
        return None

    def get_pair_by_receiver(self, receiver_id: str) -> Optional[PairData]:
        """
        Tìm pair theo receiver ID.

        Args:
            receiver_id: ID của receiver

        Returns:
            PairData nếu tìm thấy, None nếu không tìm thấy
        """
        for pair in self._pairs.values():
            if pair.receiver_id == receiver_id:
                return pair
        return None

    def get_all_pairs(self) -> Dict[uuid.UUID, PairData]:
        """
        Lấy tất cả pairs.

        Returns:
            Dict chứa tất cả pairs (copy để tránh modification)
        """
        return self._pairs.copy()

    def count(self) -> int:
        """
        Đếm tổng số pairs.

        Returns:
            Số lượng pairs
        """
        return len(self._pairs)

    def is_sender_paired(self, sender_id: str) -> bool:
        """
        Kiểm tra xem sender đã được pair chưa.

        Args:
            sender_id: ID của sender

        Returns:
            True nếu đã paired, False nếu chưa
        """
        return self.get_pair_by_sender(sender_id) is not None

    def is_receiver_paired(self, receiver_id: str) -> bool:
        """
        Kiểm tra xem receiver đã được pair chưa.

        Args:
            receiver_id: ID của receiver

        Returns:
            True nếu đã paired, False nếu chưa
        """
        return self.get_pair_by_receiver(receiver_id) is not None
