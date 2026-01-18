"""
ConnectHandler - Xử lý khi connect tới server

Example handler để demo cách implement IEventHandler cho client
"""
from socketio import AsyncClient

from src.socketio.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio.socketio_client.receiver.enum.ReceiverEvent import ReceiverEvent
from src.socketio.socketio_client.receiver.enum.ReceiverNamespace import ReceiverNamespace


class ConnectHandler(IEventHandler):
    """Handler xử lý sự kiện connect"""

    event = ReceiverEvent.CONNECT
    namespace = ReceiverNamespace.ROOT

    async def handle(self, sio: AsyncClient, session_id: str | None, data=None):
        """
        Xử lý khi connect tới server thành công

        Args:
            sio: SocketIO AsyncClient instance
            session_id: Session ID hiện tại (None nếu chưa có)
            data: Optional data from server

        Returns:
            None (không update session_id)
        """
        # Log connection
        print(f"[Receiver] Connected to server successfully!")

        return None
