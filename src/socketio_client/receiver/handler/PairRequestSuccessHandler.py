"""
PairRequestSuccessHandler - Xử lý khi được pair với sender

Handler xử lý khi server thông báo receiver được pair với sender.
"""
from socketio import AsyncClient

from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.receiver.enum.ReceiverEvent import ReceiverEvent
from src.socketio_client.receiver.enum.ReceiverNamespace import ReceiverNamespace


class PairRequestSuccessHandler(IEventHandler):
    """
    Handler xử lý sự kiện pair-request-success.

    Khi receiver được pair với sender, server sẽ emit event này.
    """

    event = ReceiverEvent.PAIR_REQUEST_SUCCESS
    namespace = ReceiverNamespace.ROOT

    async def handle(self, sio: AsyncClient, session_id: str | None, data=None):
        """
        Xử lý khi nhận pair-request-success từ server.

        Args:
            sio: SocketIO AsyncClient instance
            session_id: Session ID hiện tại của receiver
            data: Data từ server chứa:
                - pair_id: ID của cặp sender-receiver
                - sender_id: ID của sender được pair
                - receiver_id: ID của receiver (chính mình)

        Returns:
            None (không update session_id)
        """
        if not data:
            print(f"[Receiver] pair-request-success received but no data")
            return None

        pair_id = data.get("pair_id")
        sender_id = data.get("sender_id")
        receiver_id = data.get("receiver_id")

        print(f"\n{'='*60}")
        print(f"[Receiver] 🎉 PAIRED WITH SENDER!")
        print(f"[Receiver] Pair ID: {pair_id}")
        print(f"[Receiver] Sender ID: {sender_id}")
        print(f"[Receiver] Receiver ID: {receiver_id}")
        print(f"[Receiver] Ready to receive data...")
        print(f"{'='*60}\n")

        return None
