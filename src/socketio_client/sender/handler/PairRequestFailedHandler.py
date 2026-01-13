"""
PairRequestFailedHandler - Xử lý khi pairing thất bại

Handler xử lý khi server thông báo không thể pair với receiver.
"""
from socketio import AsyncClient
from pydantic import ValidationError

from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.sender.enum.SenderEvent import SenderEvent
from src.socketio_client.sender.enum.SenderNamespace import SenderNamespace
from src.shared.dto.pairing import PairRequestFailedDto


class PairRequestFailedHandler(IEventHandler):
    """
    Handler xử lý sự kiện pair-request-failed.

    Khi không có receiver available, server sẽ emit event này.
    Sender sẽ disconnect vì không thể hoạt động mà không có receiver.
    """

    event = SenderEvent.PAIR_REQUEST_FAILED
    namespace = SenderNamespace.ROOT

    async def handle(self, sio: AsyncClient, session_id: str | None, data=None):
        """
        Xử lý khi nhận pair-request-failed từ server.

        Args:
            sio: SocketIO AsyncClient instance
            session_id: Session ID hiện tại của sender
            data: Data từ server chứa:
                - sender_id: ID của sender (chính mình)
                - reason: Lý do thất bại

        Returns:
            None (không update session_id)
        """
        if not data:
            print(f"[Sender] pair-request-failed received but no data")
            await sio.disconnect()
            return None

        # Deserialize data thành DTO
        try:
            dto = PairRequestFailedDto(**data)
        except ValidationError as e:
            print(f"[Sender] Invalid pair-request-failed data: {e}")
            await sio.disconnect()
            return None

        print(f"\n{'='*60}")
        print(f"[Sender] ❌ PAIRING FAILED!")
        print(f"[Sender] Sender ID: {dto.sender_id}")
        print(f"[Sender] Reason: {dto.reason}")
        print(f"[Sender] Disconnecting...")
        print(f"{'='*60}\n")

        # Disconnect vì không có receiver để pair
        await sio.disconnect()

        return None
