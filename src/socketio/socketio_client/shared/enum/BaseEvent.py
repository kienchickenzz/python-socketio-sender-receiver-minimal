"""
BaseEvent - Dataclass-based event system cho SocketIO Client

Sử dụng dataclass thay vì Enum để cho phép kế thừa.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class SocketEvent:
    """
    Immutable dataclass đại diện cho một Socket event.

    Attributes:
        value: String value của event (event name)
        name: Optional human-readable name
    """
    value: str
    name: str = ""

    def __post_init__(self):
        """Auto-generate name from value if not provided"""
        if not self.name:
            # Convert "connect" -> "CONNECT", "new_message" -> "NEW_MESSAGE"
            object.__setattr__(self, 'name', self.value.upper().replace('-', '_'))

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return f"SocketEvent(value={self.value!r}, name={self.name!r})"


class BaseEvents:
    """
    Base events chung cho mọi SocketIO client.

    Các client cụ thể sẽ kế thừa class này và thêm events riêng.

    Example:
        class ChatEvents(BaseEvents):
            NEW_MESSAGE = SocketEvent("new_message")
            MESSAGE_SENT = SocketEvent("message_sent")
    """

    # Base events - có trong mọi SocketIO client
    CONNECT = SocketEvent("connect")
    CONNECTION_CONFIRMED = SocketEvent("connection_confirmed")

    DISCONNECT = SocketEvent("disconnect")
