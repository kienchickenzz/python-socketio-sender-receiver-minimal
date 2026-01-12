"""
DisconnectHandler - Xử lý khi worker disconnect khỏi server

Handler xử lý sự kiện khi worker ngắt kết nối với server.
"""
from socketio import AsyncClient

from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.worker.enum.WorkerEvent import WorkerEvent
from src.socketio_client.worker.enum.WorkerNamespace import WorkerNamespace


class DisconnectHandler(IEventHandler):
    """Handler xử lý disconnect event của worker"""

    event = WorkerEvent.DISCONNECT
    namespace = WorkerNamespace.ROOT

    async def handle(self, sio: AsyncClient, session_id: str | None, data=None):
        """
        Xử lý khi worker disconnect khỏi server.

        Args:
            sio: SocketIO AsyncClient instance
            session_id: Session ID hiện tại
            data: Optional data

        Returns:
            None (không update session_id)
        """
        print(f"[Worker] Disconnected from server")
        if session_id:
            print(f"[Worker] Session {session_id} ended")

        # Cleanup nếu cần
        # - Clear local state
        # - Stop background processing tasks
        # - etc.

        return None
