"""
WorkerJobHandler - Xử lý khi nhận job từ server

Handler nhận data từ server, xử lý (sort tăng dần), và trả kết quả về.
"""
from socketio import AsyncClient

from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.worker.enum.WorkerEvent import WorkerEvent
from src.socketio_client.worker.enum.WorkerNamespace import WorkerNamespace


class WorkerJobHandler(IEventHandler):
    """
    Handler xử lý sự kiện worker-job.

    Khi server emit worker-job, worker sẽ:
    1. Nhận data cần xử lý
    2. Sort data theo thứ tự tăng dần
    3. Emit worker-result với kết quả đã xử lý
    """

    event = WorkerEvent.WORKER_JOB
    namespace = WorkerNamespace.ROOT

    async def handle(self, sio: AsyncClient, session_id: str | None, data=None):
        """
        Xử lý khi nhận worker-job từ server.

        Args:
            sio: SocketIO AsyncClient instance
            session_id: Session ID hiện tại của worker
            data: Data từ server chứa:
                - pair_id: ID của cặp sender-receiver
                - sender_id: ID của sender
                - receiver_id: ID của receiver
                - data: Dãy số cần xử lý (list of int)

        Returns:
            None (không update session_id)
        """
        if not data:
            print(f"[Worker] worker-job received but no data")
            return None

        pair_id = data.get("pair_id")
        sender_id = data.get("sender_id")
        receiver_id = data.get("receiver_id")
        job_data = data.get("data", [])

        print(f"\n{'='*60}")
        print(f"[Worker] 📋 RECEIVED JOB")
        print(f"[Worker] Pair ID: {pair_id}")
        print(f"[Worker] Sender ID: {sender_id}")
        print(f"[Worker] Receiver ID: {receiver_id}")
        print(f"[Worker] Original data: {job_data}")
        print(f"[Worker] Data length: {len(job_data)}")
        print(f"{'='*60}\n")

        # Xử lý: Sort data theo thứ tự tăng dần
        sorted_data = sorted(job_data)

        print(f"[Worker] 🔄 Processing: Sorting data in ascending order...")
        print(f"[Worker] Sorted data: {sorted_data}")

        # Emit worker-result với kết quả đã xử lý
        await sio.emit(
            WorkerEvent.WORKER_RESULT.value,
            {
                "pair_id": pair_id,
                "sender_id": sender_id,
                "receiver_id": receiver_id,
                "worker_id": session_id,
                "original_data": job_data,
                "result": sorted_data,
            },
            namespace=WorkerNamespace.ROOT.value,
        )

        print(f"[Worker] ✅ Emitted worker-result with sorted data")
        print(f"{'='*60}\n")

        return None
