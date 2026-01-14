"""
WorkerResultHandler - Xử lý khi worker trả về kết quả

Handler nhận kết quả từ worker, gửi cho receiver, và cập nhật trạng thái worker.
"""
from socketio import AsyncServer
from pydantic import ValidationError

from src.socketio_server.main.model.JobData import JobData
from src.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio_server.main.enum.MainNamespace import MainNamespaces
from src.socketio_server.main.manager.WorkerManager import WorkerManager
from src.socketio_server.main.manager.JobManager import JobManager
from src.socketio_server.main.enum.WorkerStatus import WorkerStatus
from src.shared.dto.processing import WorkerResultDto, ProcessingResultDto


class WorkerResultHandler(IEventHandler):
    """
    Handler xử lý sự kiện worker-result.

    Stateless handler - nhận kết quả từ worker, forward cho receiver,
    và cập nhật trạng thái worker về ACTIVE.
    """

    event = MainEvents.WORKER_RESULT
    namespace = MainNamespaces.ROOT

    async def handle(self, sio: AsyncServer, client_sid: str | None, data=None):
        """
        Xử lý khi worker emit worker-result event.

        Args:
            sio: SocketIO AsyncServer instance
            client_sid: Socket ID của worker
            data: Dict chứa:
                - pair_id: ID của cặp sender-receiver
                - sender_id: ID của sender
                - receiver_id: ID của receiver
                - worker_id: ID của worker đã xử lý
                - original_data: Data gốc
                - result: Kết quả đã xử lý (sorted data)
                - __worker_manager__: WorkerManager injected từ registry
                - __job_manager__: JobManager injected từ registry

        Returns:
            None (fire-and-forget)
        """
        if not data:
            print(f"[WorkerResultHandler] No data received from {client_sid}")
            return

        worker_manager: WorkerManager | None = data.get("__worker_manager__")
        job_manager: JobManager | None = data.get("__job_manager__")

        # Deserialize data thành DTO
        try:
            dto = WorkerResultDto(**data)
        except ValidationError as e:
            print(f"[WorkerResultHandler] Invalid data format from {client_sid}: {e}")
            return

        print(f"\n{'='*60}")
        print(f"[WorkerResultHandler] 📦 RECEIVED WORKER RESULT")
        print(f"[WorkerResultHandler] Pair ID: {dto.pair_id}")
        print(f"[WorkerResultHandler] Worker ID: {dto.worker_id}")
        print(f"[WorkerResultHandler] Sender ID: {dto.sender_id}")
        print(f"[WorkerResultHandler] Receiver ID: {dto.receiver_id}")
        print(f"[WorkerResultHandler] Original data: {dto.original_data}")
        print(f"[WorkerResultHandler] Processed result: {dto.result}")
        print(f"{'='*60}\n")

        # Tạo DTO và serialize để emit
        result_dto = ProcessingResultDto(
            pair_id=dto.pair_id,
            sender_id=dto.sender_id,
            worker_id=dto.worker_id,
            original_data=dto.original_data,
            result=dto.result,
        )
        payload = result_dto.model_dump(by_alias=True)

        # Emit processing-result cho receiver
        await sio.emit(
            MainEvents.PROCESSING_RESULT.value,
            payload,
            room=dto.receiver_id,
            namespace=self.namespace.value,
        )

        print(f"[WorkerResultHandler] ✅ Emitted processing-result to receiver {dto.receiver_id}")

        # Remove job khỏi JobManager
        if job_manager:
            # Tìm job tương ứng với worker_id và pair_id
            jobs_by_worker: dict[str, JobData] = job_manager.get_jobs_by_worker(dto.worker_id)
            matched_jobs = {
                job_id: job_data
                for job_id, job_data in jobs_by_worker.items()
                if job_data.pair_id == dto.pair_id
            }

            if matched_jobs:
                # Lấy job đầu tiên (FIFO) và remove
                job_id = next(iter(matched_jobs))
                job_manager.remove_job(job_id)
                print(f"[WorkerResultHandler] 🗑️ Removed job {job_id} from tracking")
                print(f"[WorkerResultHandler] Total active jobs: {job_manager.count()}")
            else:
                print(
                    f"[WorkerResultHandler] ⚠️ No matching job found for worker {dto.worker_id} and pair {dto.pair_id}"
                )
        else:
            print(f"[WorkerResultHandler] ⚠️ No JobManager to remove job")

        # Cập nhật trạng thái worker về ACTIVE
        if worker_manager and dto.worker_id:
            # Worker đã hoàn thành job, set lại status về ACTIVE
            success = worker_manager.update_status(dto.worker_id, WorkerStatus.ACTIVE)
            if success:
                print(f"[WorkerResultHandler] 🔄 Updated worker {dto.worker_id} status back to ACTIVE")
            else:
                print(f"[WorkerResultHandler] ⚠️ Failed to update worker {dto.worker_id} status")
        else:
            print(f"[WorkerResultHandler] ⚠️ No WorkerManager or worker_id to update status")

        print(f"{'='*60}\n")
