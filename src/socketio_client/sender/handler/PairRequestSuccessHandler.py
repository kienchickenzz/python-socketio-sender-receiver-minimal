"""
PairRequestSuccessHandler - Xử lý khi pairing thành công

Handler xử lý khi server thông báo pair với receiver thành công.
"""
from socketio import AsyncClient

from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.sender.enum.SenderEvent import SenderEvent
from src.socketio_client.sender.enum.SenderNamespace import SenderNamespace


class PairRequestSuccessHandler(IEventHandler):
    """
    Handler xử lý sự kiện pair-request-success.

    Khi sender được pair thành công với receiver, server sẽ emit event này.
    """

    event = SenderEvent.PAIR_REQUEST_SUCCESS
    namespace = SenderNamespace.ROOT

    async def handle(self, sio: AsyncClient, session_id: str | None, data=None):
        """
        Xử lý khi nhận pair-request-success từ server.

        Args:
            sio: SocketIO AsyncClient instance
            session_id: Session ID hiện tại của sender
            data: Data từ server chứa:
                - pair_id: ID của cặp sender-receiver
                - sender_id: ID của sender (chính mình)
                - receiver_id: ID của receiver được pair

        Returns:
            None (không update session_id)
        """
        if not data:
            print(f"[Sender] pair-request-success received but no data")
            return None

        pair_id = data.get("pair_id")
        sender_id = data.get("sender_id")
        receiver_id = data.get("receiver_id")

        print(f"\n{'='*60}")
        print(f"[Sender] 🎉 PAIRING SUCCESS!")
        print(f"[Sender] Pair ID: {pair_id}")
        print(f"[Sender] Sender ID: {sender_id}")
        print(f"[Sender] Receiver ID: {receiver_id}")
        print(f"{'='*60}\n")

        # TODO: Lưu pair_id vào state nếu cần
        # Ví dụ: self.current_pair_id = pair_id

        # TODO: Bắt đầu gửi data cho receiver
        # Ví dụ: await self.start_sending_data()

        return None
