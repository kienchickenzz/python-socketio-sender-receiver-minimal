"""
PairRequestFailedHandler - Xử lý khi pairing thất bại

Handler xử lý khi server thông báo không thể pair với receiver.
"""
from socketio import AsyncClient

from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.sender.enum.SenderEvent import SenderEvent
from src.socketio_client.sender.enum.SenderNamespace import SenderNamespace


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

        sender_id = data.get("sender_id")
        reason = data.get("reason", "Unknown reason")

        print(f"\n{'='*60}")
        print(f"[Sender] ❌ PAIRING FAILED!")
        print(f"[Sender] Sender ID: {sender_id}")
        print(f"[Sender] Reason: {reason}")
        print(f"[Sender] Disconnecting...")
        print(f"{'='*60}\n")

        # Disconnect vì không có receiver để pair
        await sio.disconnect()

        return None
