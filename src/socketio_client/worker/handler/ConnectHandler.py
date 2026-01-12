"""
ConnectHandler - Xử lý khi worker connect tới server

Handler xử lý sự kiện khi worker kết nối thành công với server.
"""
from socketio import AsyncClient

from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.worker.enum.WorkerEvent import WorkerEvent
from src.socketio_client.worker.enum.WorkerNamespace import WorkerNamespace


class ConnectHandler(IEventHandler):
    """Handler xử lý sự kiện connect của worker"""

    event = WorkerEvent.CONNECT
    namespace = WorkerNamespace.ROOT

    async def handle(self, sio: AsyncClient, session_id: str | None, data=None):
        """
        Xử lý khi worker connect tới server thành công.

        Args:
            sio: SocketIO AsyncClient instance
            session_id: Session ID hiện tại (None nếu chưa có)
            data: Optional data from server

        Returns:
            None (không update session_id)
        """
        print(f"[Worker] Connected to server successfully!")
        if session_id:
            print(f"[Worker] Current session: {session_id}")

        return None
