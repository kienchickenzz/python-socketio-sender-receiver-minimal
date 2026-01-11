"""
DisconnectHandler - Xử lý khi disconnect khỏi server

Example handler cho disconnect event
"""
from socketio import AsyncClient

from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.sender.enum.SenderEvent import SenderEvent
from src.socketio_client.sender.enum.SenderNamespace import SenderNamespace


class DisconnectHandler(IEventHandler):
    """Handler xử lý disconnect event"""

    event = SenderEvent.DISCONNECT
    namespace = SenderNamespace.ROOT

    async def handle(self, sio: AsyncClient, session_id: str | None, data=None):
        """
        Xử lý khi disconnect khỏi server

        Args:
            sio: SocketIO AsyncClient instance
            session_id: Session ID hiện tại
            data: Optional data

        Returns:
            None (không update session_id)
        """
        # Log disconnection
        print(f"[Sender] Disconnected from server")
        if session_id:
            print(f"[Sender] Session {session_id} ended")

        # Cleanup (nếu cần)
        # - Clear local state
        # - Stop background tasks
        # - etc.

        return None
