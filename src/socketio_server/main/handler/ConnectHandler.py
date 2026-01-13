"""
ConnectHandler - Xử lý khi client connect tới server

Handler cho connect event trên server side
"""
from socketio import AsyncServer

from src.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio_server.main.enum.MainNamespace import MainNamespaces
from src.shared.dto.connection import ConnectionConfirmedDto


class ConnectHandler(IEventHandler):
    """Handler xử lý sự kiện connect từ client"""

    event = MainEvents.CONNECT
    namespace = MainNamespaces.ROOT

    async def handle(self, sio: AsyncServer, client_sid: str | None, data: dict = {}):
        """
        Xử lý khi client connect tới server thành công

        Args:
            sio: SocketIO AsyncServer instance
            client_sid: Socket ID của client
            data: Optional data từ client

        Returns:
            None (fire-and-forget)
        """
        print(f"[Server] Client {client_sid} connected to {self.namespace.value}")

        # Tạo DTO và serialize để emit
        dto = ConnectionConfirmedDto(client_sid=client_sid)
        payload = dto.model_dump(by_alias=True)

        await sio.emit(
            MainEvents.CONNECTION_CONFIRMED.value,
            payload,
            room=client_sid,
            namespace=self.namespace.value
        )
        print(f"[Server] Sent CONNECTION_CONFIRMED to {client_sid} with payload: {payload}")