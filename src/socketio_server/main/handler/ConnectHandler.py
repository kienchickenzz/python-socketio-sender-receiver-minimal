"""
ConnectHandler - Xử lý khi client connect tới server

Handler cho connect event trên server side
"""
from socketio import AsyncServer

from src.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio_server.main.enum.MainNamespace import MainNamespaces


class ConnectHandler(IEventHandler):
    """Handler xử lý sự kiện connect từ client"""

    event = MainEvents.CONNECT
    namespace = MainNamespaces.ROOT

    async def handle(self, sio: AsyncServer, sid: str, data=None):
        """
        Xử lý khi client connect tới server thành công

        Args:
            sio: SocketIO AsyncServer instance
            sid: Socket ID của client
            data: Optional data từ client

        Returns:
            None (fire-and-forget)
        """
        print(f"[Server] Client {sid} connected to {self.namespace.value}")

        # Gửi CONNECTION_CONFIRMED với session ID cho client
        await sio.emit(
            MainEvents.CONNECTION_CONFIRMED.value,
            {"sid": sid},
            room=sid,
            namespace=self.namespace.value
        )
        print(f"[Server] Sent CONNECTION_CONFIRMED to {sid}")
