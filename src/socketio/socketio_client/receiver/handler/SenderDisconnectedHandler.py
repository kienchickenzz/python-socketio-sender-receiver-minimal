"""
SenderDisconnectedHandler - Xử lý khi sender ngắt kết nối

Handler nhận thông báo từ server khi sender đã ngắt kết nối
và thực hiện business logic tương ứng (hiện tại chỉ in thông báo).
"""
from socketio import AsyncClient
from pydantic import ValidationError

from src.socketio.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio.socketio_client.receiver.enum.ReceiverEvent import ReceiverEvent
from src.socketio.socketio_client.receiver.enum.ReceiverNamespace import ReceiverNamespace
from src.socketio.shared.dto.connection import SenderDisconnectDto


class SenderDisconnectedHandler(IEventHandler):
    """
    Handler xử lý sự kiện sender-disconnected.

    Khi sender ngắt kết nối, server sẽ gửi event này cho receiver
    để thông báo và receiver có thể thực hiện cleanup hoặc reset state.
    """

    event = ReceiverEvent.SENDER_DISCONNECTED
    namespace = ReceiverNamespace.ROOT

    async def handle(self, sio: AsyncClient, session_id: str | None, data=None):
        """
        Xử lý khi nhận sender-disconnected từ server.

        Args:
            sio (AsyncClient): SocketIO AsyncClient instance
            session_id (str | None): Session ID hiện tại của receiver
            data: Data từ server chứa:
                - sender_id: ID của sender đã ngắt kết nối
                - pair_id: ID của cặp sender-receiver

        Returns:
            None (không update session_id)
        """
        if not data:
            print(f"[Receiver] sender-disconnected received but no data")
            return None

        # Deserialize data thành DTO
        try:
            dto = SenderDisconnectDto(**data)
        except ValidationError as e:
            print(f"[Receiver] Invalid sender-disconnected data: {e}")
            return None

        print(f"\n{'='*60}")
        print(f"[Receiver] ⚠️ SENDER DISCONNECTED")
        print(f"[Receiver] Sender ID: {dto.sender_id}")
        print(f"[Receiver] Pair ID: {dto.pair_id}")
        print(f"[Receiver] ")
        print(f"[Receiver] 📋 Receiver is now IDLE and waiting for new sender...")
        print(f"{'='*60}\n")

        return None
