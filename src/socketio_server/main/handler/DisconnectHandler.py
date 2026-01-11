"""
DisconnectHandler - Xử lý khi client disconnect khỏi server

Handler cho disconnect event trên server side
"""
from socketio import AsyncServer

from src.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio_server.main.enum.MainNamespace import MainNamespaces
from src.socketio_server.main.manager.ReceiverManager import ReceiverManager


class DisconnectHandler(IEventHandler):
    """
    Handler xử lý disconnect event từ client.

    Stateless handler - không lưu trữ state, lấy ReceiverManager từ data.
    """

    event = MainEvents.DISCONNECT
    namespace = MainNamespaces.ROOT

    async def handle(self, sio: AsyncServer, sid: str, data=None):
        """
        Xử lý khi client disconnect khỏi server.

        Args:
            sio: SocketIO AsyncServer instance
            sid: Socket ID của client (cũng là receiver_id)
            data: Dict chứa:
                - __receiver_manager__: ReceiverManager instance (injected by registry)

        Returns:
            None (fire-and-forget)
        """
        print(f"[Server] Client {sid} disconnected from {self.namespace.value}")

        # Lấy ReceiverManager từ data (injected by registry)
        if data and isinstance(data, dict):
            receiver_manager: ReceiverManager | None = data.get("__receiver_manager__", None)
            if receiver_manager:
                # Xóa receiver khỏi pool (receiver_id = sid)
                removed = receiver_manager.remove_receiver(sid)
                if removed:
                    print(f"[DisconnectHandler] Cleaned up receiver {sid}")
                else:
                    print(f"[DisconnectHandler] Receiver {sid} not found in pool")
            else:
                print(f"[DisconnectHandler] ReceiverManager not found in data")

        # Additional cleanup nếu cần
        # - Notify other clients
        # - Release resources
