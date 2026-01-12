"""
ProcessingAcknowledgedHandler - Xử lý khi worker xác nhận đã nhận được request

Handler nhận acknowledgment từ worker khi worker đã nhận được data để xử lý.
"""
from socketio import AsyncServer

from src.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio_server.main.enum.MainNamespace import MainNamespaces


class ProcessingAcknowledgedHandler(IEventHandler):
    """
    Handler xử lý sự kiện processing-acknowledged.

    Stateless handler - nhận acknowledgment từ worker và log thông tin.
    Sau này có thể mở rộng để cập nhật trạng thái worker, thông báo sender, etc.
    """

    event = MainEvents.PROCESSING_ACKNOWLEDGED
    namespace = MainNamespaces.ROOT

    async def handle(self, sio: AsyncServer, client_sid: str | None, data=None):
        """
        Xử lý khi worker emit processing-acknowledged event.

        Args:
            sio: SocketIO AsyncServer instance
            sid: Socket ID của worker
            data: Dict chứa:
                - pair_id: ID của cặp sender-receiver
                - worker_id: ID của worker
                - receiver_id: ID của receiver đã gửi data
                - status: Trạng thái acknowledgment (optional)

        Returns:
            None (fire-and-forget)
        """
        if not data:
            print(f"[ProcessingAcknowledgedHandler] No data received from {client_sid}")
            return

        pair_id = data.get("pair_id")
        worker_id = data.get("worker_id")
        receiver_id = data.get("receiver_id")
        status = data.get("status", "acknowledged")

        print(f"\n{'='*60}")
        print(f"[ProcessingAcknowledgedHandler] ✅ PROCESSING ACKNOWLEDGED")
        print(f"[ProcessingAcknowledgedHandler] Pair ID: {pair_id}")
        print(f"[ProcessingAcknowledgedHandler] Worker ID: {worker_id}")
        print(f"[ProcessingAcknowledgedHandler] Receiver ID: {receiver_id}")
        print(f"[ProcessingAcknowledgedHandler] Status: {status}")
        print(f"{'='*60}\n")

        # Hiện tại chỉ log thông tin, có thể mở rộng logic ở đây nếu cần thiết
