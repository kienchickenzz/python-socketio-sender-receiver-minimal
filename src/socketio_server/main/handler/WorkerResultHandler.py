"""
WorkerResultHandler - Xử lý khi worker trả về kết quả

Handler nhận kết quả từ worker, gửi cho receiver, và cập nhật trạng thái worker.
"""
from socketio import AsyncServer

from src.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio_server.main.enum.MainNamespace import MainNamespaces
from src.socketio_server.main.manager.WorkerManager import WorkerManager
from src.socketio_server.main.enum.WorkerStatus import WorkerStatus


class WorkerResultHandler(IEventHandler):
    """
    Handler xử lý sự kiện worker-result.

    Stateless handler - nhận kết quả từ worker, forward cho receiver,
    và cập nhật trạng thái worker về ACTIVE.
    """

    event = MainEvents.WORKER_RESULT
    namespace = MainNamespaces.ROOT

    async def handle(self, sio: AsyncServer, sid: str, data=None):
        """
        Xử lý khi worker emit worker-result event.

        Args:
            sio: SocketIO AsyncServer instance
            sid: Socket ID của worker
            data: Dict chứa:
                - pair_id: ID của cặp sender-receiver
                - sender_id: ID của sender
                - receiver_id: ID của receiver
                - worker_id: ID của worker đã xử lý
                - original_data: Data gốc
                - result: Kết quả đã xử lý (sorted data)
                - __worker_manager__: WorkerManager injected từ registry

        Returns:
            None (fire-and-forget)
        """
        if not data:
            print(f"[WorkerResultHandler] No data received from {sid}")
            return

        pair_id = data.get("pair_id")
        sender_id = data.get("sender_id")
        receiver_id = data.get("receiver_id")
        worker_id = data.get("worker_id")
        original_data = data.get("original_data", [])
        result = data.get("result", [])
        worker_manager: WorkerManager | None = data.get("__worker_manager__")

        print(f"\n{'='*60}")
        print(f"[WorkerResultHandler] 📦 RECEIVED WORKER RESULT")
        print(f"[WorkerResultHandler] Pair ID: {pair_id}")
        print(f"[WorkerResultHandler] Worker ID: {worker_id}")
        print(f"[WorkerResultHandler] Sender ID: {sender_id}")
        print(f"[WorkerResultHandler] Receiver ID: {receiver_id}")
        print(f"[WorkerResultHandler] Original data: {original_data}")
        print(f"[WorkerResultHandler] Processed result: {result}")
        print(f"{'='*60}\n")

        # Emit processing-result cho receiver
        await sio.emit(
            MainEvents.PROCESSING_RESULT.value,
            {
                "pair_id": pair_id,
                "sender_id": sender_id,
                "worker_id": worker_id,
                "original_data": original_data,
                "result": result,
            },
            room=receiver_id,
            namespace=self.namespace.value,
        )

        print(f"[WorkerResultHandler] ✅ Emitted processing-result to receiver {receiver_id}")

        # Cập nhật trạng thái worker về ACTIVE
        if worker_manager and worker_id:
            # Worker đã hoàn thành job, set lại status về ACTIVE
            success = worker_manager.update_status(worker_id, WorkerStatus.ACTIVE)
            if success:
                print(f"[WorkerResultHandler] 🔄 Updated worker {worker_id} status back to ACTIVE")
            else:
                print(f"[WorkerResultHandler] ⚠️ Failed to update worker {worker_id} status")
        else:
            print(f"[WorkerResultHandler] ⚠️ No WorkerManager or worker_id to update status")

        print(f"{'='*60}\n")
