"""
WorkerJobHandler - Xử lý khi nhận job từ server

Handler nhận data từ server, xử lý (sort tăng dần), và trả kết quả về.
"""
from socketio import AsyncClient
from pydantic import ValidationError

from src.socketio.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio.socketio_client.worker.enum.WorkerEvent import WorkerEvent
from src.socketio.socketio_client.worker.enum.WorkerNamespace import WorkerNamespace
from src.socketio.shared.dto.processing import WorkerJobDto, WorkerResultDto


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
            sio (AsyncClient): SocketIO AsyncClient instance
            session_id (str | None): Session ID hiện tại của worker
            data: Data từ server chứa:
                - job_id: ID của job được tạo bởi JobManager
                - pair_id: ID của cặp sender-receiver
                - sender_id: ID của sender
                - receiver_id: ID của receiver
                - worker_id: ID của worker được chọn
                - data: Dãy số cần xử lý (list of int)

        Returns:
            None: Không update session_id
        """
        if not data:
            print(f"[Worker] worker-job received but no data")
            return None

        # Deserialize data thành DTO
        try:
            dto = WorkerJobDto(**data)
        except ValidationError as e:
            print(f"[Worker] Invalid worker-job data: {e}")
            return None

        print(f"\n{'='*60}")
        print(f"[Worker] 📋 RECEIVED JOB")
        print(f"[Worker] Job ID: {dto.job_id}")
        print(f"[Worker] Pair ID: {dto.pair_id}")
        print(f"[Worker] Sender ID: {dto.sender_id}")
        print(f"[Worker] Receiver ID: {dto.receiver_id}")
        print(f"[Worker] Worker ID: {dto.worker_id}")
        print(f"[Worker] Original data: {dto.data}")
        print(f"[Worker] Data length: {len(dto.data)}")
        print(f"{'='*60}\n")

        # Xử lý: Sort data theo thứ tự tăng dần
        sorted_data = sorted(dto.data)

        print(f"[Worker] 🔄 Processing: Sorting data in ascending order...")
        print(f"[Worker] Sorted data: {sorted_data}")

        # Tạo DTO và serialize để emit
        result_dto = WorkerResultDto(
            job_id=dto.job_id,
            pair_id=dto.pair_id,
            sender_id=dto.sender_id,
            receiver_id=dto.receiver_id,
            worker_id=session_id,
            original_data=dto.data,
            result=sorted_data,
        )
        payload = result_dto.model_dump(by_alias=True)

        # Emit worker-result với kết quả đã xử lý
        await sio.emit(
            WorkerEvent.WORKER_RESULT.value,
            payload,
            namespace=WorkerNamespace.ROOT.value,
        )

        print(f"[Worker] ✅ Emitted worker-result with sorted data")
        print(f"{'='*60}\n")

        return None
