"""
ReceiverReadyHandler - Xử lý khi receiver sẵn sàng

Handler xử lý sự kiện receiver_ready từ receiver client.
"""
from socketio import AsyncServer

from src.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio_server.main.enum.MainNamespace import MainNamespaces
from src.socketio_server.main.enum.ReceiverStatus import ReceiverStatus
from src.socketio_server.main.manager.ReceiverManager import ReceiverManager


class ReceiverReadyHandler(IEventHandler):
    """
    Handler xử lý sự kiện receiver_ready.

    Stateless handler - không lưu trữ state, lấy ReceiverManager từ data.
    """

    event = MainEvents.RECEIVER_READY
    namespace = MainNamespaces.ROOT

    async def handle(self, sio: AsyncServer, sid: str, data=None):
        """
        Xử lý khi receiver emit receiver_ready event.

        Args:
            sio: SocketIO AsyncServer instance
            sid: Socket ID của receiver
            data: Dict chứa:
                - session_id: ID của receiver
                - __receiver_manager__: ReceiverManager instance (injected by registry)

        Returns:
            None (fire-and-forget)
        """
        if not data:
            print(f"[ReceiverReadyHandler] No data received from {sid}")
            return

        # Lấy ReceiverManager từ data (injected by registry)
        receiver_manager: ReceiverManager = data.get("__receiver_manager__")
        if not receiver_manager:
            print(f"[ReceiverReadyHandler] ReceiverManager not found in data")
            return

        # Lấy session_id từ data
        session_id = data.get("session_id")
        if not session_id:
            print(f"[ReceiverReadyHandler] session_id not found in data from {sid}")
            return

        # Thêm receiver vào pool với status ACTIVE
        receiver_manager.add_receiver(session_id, ReceiverStatus.ACTIVE)

        print(
            f"[ReceiverReadyHandler] Receiver {session_id} (sid: {sid}) is now ACTIVE"
        )
        print(
            f"[ReceiverReadyHandler] Total active receivers: {receiver_manager.count_by_status(ReceiverStatus.ACTIVE)}"
        )
        print(
            f"[ReceiverReadyHandler] Total idle receivers: {receiver_manager.count_by_status(ReceiverStatus.IDLE)}"
        )
