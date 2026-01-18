"""
SenderStatus - Enum cho trạng thái của sender

Định nghĩa các trạng thái có thể có của một sender.
"""
from enum import Enum


class SenderStatus(str, Enum):
    """
    Enum định nghĩa các trạng thái của sender.

    Logic của sender:
    - Sau khi connect, sender chỉ có trạng thái ACTIVE
    - Server sẽ emit sender-pair-request để tìm receiver
    - Nếu không pair được → sender disconnect
    - Không cần IDLE vì sender hoặc đang active hoặc đã disconnect

    Attributes:
        ACTIVE: Sender đang hoạt động và sẵn sàng pair với receiver
    """

    ACTIVE = "ACTIVE"
