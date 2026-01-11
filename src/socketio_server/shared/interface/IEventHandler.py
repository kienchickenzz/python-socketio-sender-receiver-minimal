"""
IEventHandler - Abstract base class cho tất cả Socket event handlers

Trách nhiệm:
- Define contract cho handlers
- Async-only interface (tất cả handlers đều async)
- Clean API với SocketIO instance
"""
from abc import ABC, abstractmethod
from typing import ClassVar

from socketio import AsyncServer

from src.socketio_server.shared.enum.BaseEvent import SocketEvent
from src.socketio_server.shared.enum.BaseNamespace import Namespace


class IEventHandler(ABC):
    """
    Abstract base class cho Socket event handlers.

    Subclass phải:
    1. Set class attributes: event (SocketEvent), namespace (Namespace)
    2. Implement async handle(sio, sid, data) method

    Example:
        class MessageHandler(IEventHandler):
            event = ChatEvents.NEW_MESSAGE
            namespace = ChatNamespaces.CHAT

            async def handle(self, sio: AsyncServer, sid: str, data=None):
                message = data.get("text")
                await sio.emit("new_message", {"text": message}, namespace=self.namespace.value)
    """

    # Class attributes - MUST be set by subclass
    event: ClassVar[SocketEvent]
    namespace: ClassVar[Namespace]

    session_id: str = ""

    @abstractmethod
    async def handle(self, sio: AsyncServer, data: dict = {}):
        """
        Handle Socket event.

        Args:
            sio: SocketIO AsyncServer instance
            sid: Socket ID
            data: Optional event data from client

        Returns:
            None: Fire-and-forget, không cần response

        Example:
            async def handle(self, sio: AsyncServer, sid: str, data=None):
                # Access data
                text = data.get("text") if data else ""

                # Business logic
                result = await self.process(text)

                # Emit/broadcast using sio
                await sio.emit("response", result, room=sid, namespace=self.namespace)
        """
        pass
