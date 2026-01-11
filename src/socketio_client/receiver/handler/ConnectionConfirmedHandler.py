"""
ConnectionConfirmedHandler - Xử lý khi server xác nhận connection và gửi session ID

Handler này nhận session ID từ server và trả về để registry cập nhật.
"""
from socketio import AsyncClient

from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.receiver.enum.ReceiverEvent import ReceiverEvent
from src.socketio_client.receiver.enum.ReceiverNamespace import ReceiverNamespace


class ConnectionConfirmedHandler(IEventHandler):
    """Handler xử lý CONNECTION_CONFIRMED event và lưu session ID"""

    event = ReceiverEvent.CONNECTION_CONFIRMED
    namespace = ReceiverNamespace.ROOT

    async def handle(self, sio: AsyncClient, session_id: str | None, data=None):
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
        new_session_id = data.get("sid") if data else None

        if new_session_id:
            print(f"[Receiver] Connection confirmed with session ID: {new_session_id}")
            # Trả về session_id mới để wrapper cập nhật vào registry

            # Emit receiver_ready event khi đã có session_id
            await sio.emit(
                ReceiverEvent.RECEIVER_READY.value,
                {"session_id": new_session_id},
                namespace=ReceiverNamespace.ROOT.value
            )
            print(f"[Receiver] Emitted receiver_ready event with session_id: {new_session_id}")

            return new_session_id
        else:
            print(f"[Receiver] Warning: CONNECTION_CONFIRMED received but no session ID in data")
            return None
