"""
DisconnectHandler - Xử lý khi client disconnect khỏi server

Handler cho disconnect event trên server side
"""
from socketio import AsyncServer

from src.socketio.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio.socketio_server.main.enum.MainNamespace import MainNamespaces
from src.socketio.socketio_server.main.manager.ReceiverManager import ReceiverManager
from src.socketio.socketio_server.main.manager.SenderManager import SenderManager


class DisconnectHandler(IEventHandler):
    """
    Handler xử lý disconnect event từ client.

    Stateless handler - không lưu trữ state, lấy managers từ data.
    """

    event = MainEvents.DISCONNECT
    namespace = MainNamespaces.ROOT

    async def handle(self, sio: AsyncServer, client_sid: str | None, data=None):
        """
        Xử lý khi client disconnect khỏi server.

        Args:
            sio: SocketIO AsyncServer instance
            client_sid: Socket ID của client
            data: Dict chứa:
                - __receiver_manager__: ReceiverManager instance (injected by registry)
                - __sender_manager__: SenderManager instance (injected by registry)

        Returns:
            None (fire-and-forget)
        """
        print(f"[Server] Client {client_sid} disconnected from {self.namespace.value}")

        if data and isinstance(data, dict):
            # Cleanup receiver nếu có
            receiver_manager: ReceiverManager | None = data.get("__receiver_manager__")
            if receiver_manager:
                removed = receiver_manager.remove_receiver(client_sid)
                if removed:
                    print(f"[DisconnectHandler] Cleaned up receiver {client_sid}")

            # Cleanup sender nếu có
            sender_manager: SenderManager | None = data.get("__sender_manager__")
            if sender_manager:
                removed = sender_manager.remove_sender(client_sid)
                if removed:
                    print(f"[DisconnectHandler] Cleaned up sender {client_sid}")

            # Nếu không tìm thấy trong cả 2 pool
            if not receiver_manager and not sender_manager:
                print(f"[DisconnectHandler] No managers found in data")

        # Additional cleanup nếu cần
        # - Notify other clients
        # - Release resources
