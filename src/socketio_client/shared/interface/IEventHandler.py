"""
IEventHandler - Abstract base class cho tất cả Socket event handlers (Client)

Trách nhiệm:
- Define contract cho client handlers
- Async-only interface (tất cả handlers đều async)
- Clean API với SocketIO AsyncClient instance
"""
from abc import ABC, abstractmethod
from typing import ClassVar

from socketio import AsyncClient

from src.socketio_client.shared.enum.BaseEvent import SocketEvent
from src.socketio_client.shared.enum.BaseNamespace import Namespace


class IEventHandler(ABC):
    """
    Abstract base class cho Socket event handlers (Client side).

    Subclass phải:
    1. Set class attributes: event (SocketEvent), namespace (Namespace)
    2. Implement async handle(sio, data) method

    Example:
        class MessageReceivedHandler(IEventHandler):
            event = ChatEvents.NEW_MESSAGE
            namespace = ChatNamespaces.CHAT

            async def handle(self, sio: AsyncClient, data=None):
                message = data.get("text") if data else ""
                print(f"Received message: {message}")
    """

    # Class attributes - MUST be set by subclass
    event: ClassVar[SocketEvent]
    namespace: ClassVar[Namespace]

    @abstractmethod
    async def handle(self, sio: AsyncClient, session_id: str | None, data: dict = {}) -> str | None:
        """
        Handle Socket event từ server.

        Args:
            sio: SocketIO AsyncClient instance
            session_id: Session ID từ server (None nếu chưa có)
            data: Optional event data from server

        Returns:
            Optional[str]: Trả về session_id mới nếu handler update (cho ConnectionConfirmedHandler),
                          None nếu không thay đổi

        Example:
            async def handle(self, sio: AsyncClient, session_id: str | None, data=None):
                # Access session_id
                print(f"Current session: {session_id}")

                # Access data
                text = data.get("text") if data else ""

                # Business logic
                result = await self.process(text)

                # Emit to server using sio
                await sio.emit("response", result, namespace=self.namespace)

                # Return None (không update session_id)
                return None
        """
        pass
