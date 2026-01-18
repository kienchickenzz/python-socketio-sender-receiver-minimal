"""
BaseNamespace - Dataclass-based namespace system cho SocketIO Client

Sử dụng dataclass để đảm bảo type-safe cho namespaces.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Namespace:
    """
    Immutable dataclass đại diện cho một Socket namespace.

    Attributes:
        value: String value của namespace (namespace path)
        name: Optional human-readable name
    """
    value: str
    name: str = ""

    def __post_init__(self):
        """Auto-generate name from value if not provided"""
        if not self.name:
            # Convert "/chat" -> "CHAT", "/" -> "ROOT"
            if self.value == "/":
                object.__setattr__(self, 'name', 'ROOT')
            else:
                name = self.value.strip('/').upper().replace('-', '_')
                object.__setattr__(self, 'name', name)

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return f"Namespace(value={self.value!r}, name={self.name!r})"


class BaseNamespace:
    """
    Base namespaces chung cho mọi SocketIO client.

    Các client cụ thể sẽ kế thừa class này và thêm namespaces riêng.

    Example:
        class ChatNamespaces(BaseNamespaces):
            CHAT = Namespace("/chat")
            GAME = Namespace("/game")
    """

    # Base namespace - root namespace
    ROOT = Namespace("/")
