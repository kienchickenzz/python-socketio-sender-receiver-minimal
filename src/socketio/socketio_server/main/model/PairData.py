"""
PairData - Data model cho sender-receiver pair

Model lưu trữ thông tin về một cặp sender-receiver đang active.
"""
import uuid
from dataclasses import dataclass


@dataclass
class PairData:
    """
    Data model cho mỗi sender-receiver pair.

    Attributes:
        id: ID tự động tăng của pair
        sender_id: ID của sender trong pair
        receiver_id: ID của receiver trong pair
    """

    id: uuid.UUID
    sender_id: str
    receiver_id: str

    def to_dict(self) -> dict:
        """
        Convert PairData sang dict để serialize.

        Returns:
            Dict representation của PairData
        """
        return {
            "id": str(self.id),  # Convert UUID to string
            "sender_id": self.sender_id,
            "receiver_id": self.receiver_id,
        }
