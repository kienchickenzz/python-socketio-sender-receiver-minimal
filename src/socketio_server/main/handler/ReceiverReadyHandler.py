"""
ReceiverReadyHandler - Xử lý khi receiver sẵn sàng

Handler xử lý sự kiện receiver_ready từ receiver client.
"""
from socketio import AsyncServer
from pydantic import ValidationError

from src.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio_server.main.enum.MainNamespace import MainNamespaces
from src.socketio_server.main.enum.ReceiverStatus import ReceiverStatus
from src.socketio_server.main.manager.ReceiverManager import ReceiverManager
from src.shared.dto.connection import ReceiverReadyDto


class ReceiverReadyHandler(IEventHandler):
    """
    Handler xử lý sự kiện receiver_ready.

    Stateless handler - không lưu trữ state, lấy ReceiverManager từ data.
    """

    event = MainEvents.RECEIVER_READY
    namespace = MainNamespaces.ROOT

    async def handle(self, sio: AsyncServer, client_sid: str | None, data=None):
        """
        Xử lý khi receiver emit receiver_ready event.

        Args:
            sio: SocketIO AsyncServer instance
            client_sid: Socket ID của receiver
            data: Dict chứa:
                - session_id: ID của receiver
                - __receiver_manager__: ReceiverManager instance (injected by registry)

        Returns:
            None (fire-and-forget)
        """
        if not data:
            print(f"[ReceiverReadyHandler] No data received from {client_sid}")
            return

        # Lấy ReceiverManager từ data (injected by registry)
        receiver_manager: ReceiverManager | None = data.get("__receiver_manager__")
        if not receiver_manager:
            print(f"[ReceiverReadyHandler] ReceiverManager not found in data")
            return

        # Deserialize data thành DTO
        try:
            dto = ReceiverReadyDto(**data)
        except ValidationError as e:
            print(f"[ReceiverReadyHandler] Invalid data format from {client_sid}: {e}")
            return

        # Thêm receiver vào pool với status IDLE
        receiver_manager.add_receiver(dto.session_id, ReceiverStatus.IDLE)

        print(
            f"[ReceiverReadyHandler] Receiver {dto.session_id} (client_sid: {client_sid}) is now IDLE"
        )
        print(
            f"[ReceiverReadyHandler] Total active receivers: {receiver_manager.count_by_status(ReceiverStatus.ACTIVE)}"
        )
        print(
            f"[ReceiverReadyHandler] Total idle receivers: {receiver_manager.count_by_status(ReceiverStatus.IDLE)}"
        )
