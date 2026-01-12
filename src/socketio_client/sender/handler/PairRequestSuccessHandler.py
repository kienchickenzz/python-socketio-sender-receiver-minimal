"""
PairRequestSuccessHandler - Xử lý khi pairing thành công

Handler xử lý khi server thông báo pair với receiver thành công.
Sau khi pair thành công, sẽ gửi dãy số ngẫu nhiên mỗi 5 giây liên tục.
"""
import random
import asyncio

from socketio import AsyncClient

from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.sender.enum.SenderEvent import SenderEvent
from src.socketio_client.sender.enum.SenderNamespace import SenderNamespace


class PairRequestSuccessHandler(IEventHandler):
    """
    Handler xử lý sự kiện pair-request-success.

    Khi sender được pair thành công với receiver, server sẽ emit event này.
    Handler sẽ bắt đầu gửi dãy số ngẫu nhiên mỗi 5 giây liên tục.
    """

    event = SenderEvent.PAIR_REQUEST_SUCCESS
    namespace = SenderNamespace.ROOT

    async def _send_data_periodically(
        self, sio: AsyncClient, pair_id: str, sender_id: str, receiver_id: str
    ):
        """
        Background task gửi dãy số ngẫu nhiên mỗi 5 giây liên tục.

        Args:
            sio: SocketIO AsyncClient instance
            pair_id: ID của cặp sender-receiver
            sender_id: ID của sender
            receiver_id: ID của receiver
        """
        counter = 1
        while True:
            try:
                # Generate 10 random numbers từ 1-20
                random_numbers = [random.randint(1, 20) for _ in range(10)]
                print(
                    f"\n[Sender] 📤 Sending batch #{counter}: {random_numbers}"
                )

                # Emit request-processing với dãy số ngẫu nhiên
                await sio.emit(
                    SenderEvent.REQUEST_PROCESSING.value,
                    {
                        "pair_id": pair_id,
                        "sender_id": sender_id,
                        "receiver_id": receiver_id,
                        "data": random_numbers,
                    },
                    namespace=SenderNamespace.ROOT.value,
                )

                print(f"[Sender] ✅ Batch #{counter} sent successfully")
                counter += 1

                # Đợi 2 giây trước khi gửi tiếp
                await asyncio.sleep(2)

            except Exception as e:
                print(f"[Sender] ❌ Error sending data: {e}")
                break

    async def handle(self, sio: AsyncClient, session_id: str | None, data=None):
        """
        Xử lý khi nhận pair-request-success từ server.

        Khởi chạy background task để gửi dãy số ngẫu nhiên mỗi 5 giây liên tục.

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
        print(f"[Sender] Starting continuous data transmission (every 5 seconds)...")
        print(f"{'='*60}\n")

        # Khởi chạy background task để gửi data liên tục mỗi 5 giây
        asyncio.create_task(
            self._send_data_periodically(sio, pair_id, sender_id, receiver_id)
        )

        return None
