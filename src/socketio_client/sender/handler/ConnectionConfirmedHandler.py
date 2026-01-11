"""
ConnectionConfirmedHandler - Xử lý khi server xác nhận connection và gửi session ID

Handler này nhận session ID từ server và trả về để registry cập nhật.
"""
from socketio import AsyncClient

from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.sender.enum.SenderEvent import SenderEvent
from src.socketio_client.sender.enum.SenderNamespace import SenderNamespace


class ConnectionConfirmedHandler(IEventHandler):
    """Handler xử lý CONNECTION_CONFIRMED event và lưu session ID"""

    event = SenderEvent.CONNECTION_CONFIRMED
    namespace = SenderNamespace.ROOT

    async def handle(self, sio: AsyncClient, session_id: str | None, data: dict = {}) -> str | None:
        """
        Xử lý khi nhận CONNECTION_CONFIRMED từ server

        Args:
            sio: SocketIO AsyncClient instance
            session_id: Session ID hiện tại (sẽ là None lần đầu)
            data: Data từ server chứa session ID mới

        Returns:
            str: Session ID mới từ server để registry cập nhật
        """
        # Lấy session ID từ data
        new_session_id: str | None = data.get("sid") if data else None

        if new_session_id:
            print(f"[Sender] Connection confirmed with session ID: {new_session_id}")
            # Trả về session_id mới để wrapper cập nhật vào registry
            return new_session_id
        else:
            print(f"[Sender] Warning: CONNECTION_CONFIRMED received but no session ID in data")
            return None
