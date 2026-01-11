"""
DisconnectHandler - Xử lý khi disconnect khỏi server

Example handler cho disconnect event
"""
from socketio import AsyncClient

from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.receiver.enum.ReceiverEvent import ReceiverEvent
from src.socketio_client.receiver.enum.ReceiverNamespace import ReceiverNamespace


class DisconnectHandler(IEventHandler):
    """Handler xử lý disconnect event"""

    event = ReceiverEvent.DISCONNECT
    namespace = ReceiverNamespace.ROOT

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
        print(f"[Receiver] Disconnected from server")
        if session_id:
            print(f"[Receiver] Session {session_id} ended")

        # Cleanup (nếu cần)
        # - Clear local state
        # - Stop background tasks
        # - etc.

        return None
