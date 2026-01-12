"""
ProcessingResultHandler - Xử lý khi nhận kết quả từ server

Handler nhận kết quả đã xử lý từ worker thông qua server và in ra.
"""
from socketio import AsyncClient

from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.receiver.enum.ReceiverEvent import ReceiverEvent
from src.socketio_client.receiver.enum.ReceiverNamespace import ReceiverNamespace


class ProcessingResultHandler(IEventHandler):
    """
    Handler xử lý sự kiện processing-result.

    Khi worker hoàn thành xử lý và server forward kết quả về receiver,
    receiver sẽ nhận event này và in ra kết quả.
    """

    event = ReceiverEvent.PROCESSING_RESULT
    namespace = ReceiverNamespace.ROOT

    async def handle(self, sio: AsyncClient, session_id: str | None, data=None):
        """
        Xử lý khi nhận processing-result từ server.

        Args:
            sio: SocketIO AsyncClient instance
            session_id: Session ID hiện tại của receiver
            data: Data từ server chứa:
                - pair_id: ID của cặp sender-receiver
                - sender_id: ID của sender
                - worker_id: ID của worker đã xử lý
                - original_data: Data gốc từ sender
                - result: Kết quả đã xử lý (sorted data)

        Returns:
            None (không update session_id)
        """
        if not data:
            print(f"[Receiver] processing-result received but no data")
            return None

        pair_id = data.get("pair_id")
        sender_id = data.get("sender_id")
        worker_id = data.get("worker_id")
        original_data = data.get("original_data", [])
        result = data.get("result", [])

        print(f"\n{'='*60}")
        print(f"[Receiver] 🎉 PROCESSING RESULT RECEIVED")
        print(f"[Receiver] Pair ID: {pair_id}")
        print(f"[Receiver] Sender ID: {sender_id}")
        print(f"[Receiver] Worker ID: {worker_id}")
        print(f"[Receiver] ")
        print(f"[Receiver] 📊 DATA COMPARISON:")
        print(f"[Receiver] Original data: {original_data}")
        print(f"[Receiver] Processed result: {result}")
        print(f"[Receiver] ")
        print(f"[Receiver] ✅ Data has been sorted in ascending order!")
        print(f"{'='*60}\n")

        return None
