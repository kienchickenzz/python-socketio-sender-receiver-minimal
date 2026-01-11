"""
ConnectHandler - Xử lý khi connect tới server

Example handler để demo cách implement IEventHandler cho client
"""
from socketio import AsyncClient

from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.sender.enum.SenderEvent import SenderEvent
from src.socketio_client.sender.enum.SenderNamespace import SenderNamespace


class ConnectHandler(IEventHandler):
    """Handler xử lý sự kiện connect"""

    event = SenderEvent.CONNECT
    namespace = SenderNamespace.ROOT

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
        print(f"[Sender] Connected to server successfully!")
        if session_id:
            print(f"[Sender] Current session: {session_id}")

        # Optional: Send initial data to server
        # await sio.emit("join_room", {"room": "lobby"}, namespace=self.namespace)

        return None
