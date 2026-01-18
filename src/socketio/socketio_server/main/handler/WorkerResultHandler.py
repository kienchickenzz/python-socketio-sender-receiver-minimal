"""
WorkerResultHandler - Xử lý khi worker trả về kết quả

Handler nhận kết quả từ worker, gửi cho receiver, và cập nhật trạng thái worker.
"""
from socketio import AsyncServer
from pydantic import ValidationError

from src.socketio.socketio_server.main.model.JobData import JobData
from src.socketio.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio.socketio_server.main.enum.MainNamespace import MainNamespaces
from src.socketio.socketio_server.main.manager.WorkerManager import WorkerManager
from src.socketio.socketio_server.main.manager.JobManager import JobManager
from src.socketio.socketio_server.main.enum.WorkerStatus import WorkerStatus
from src.socketio.socketio_server.main.enum.JobStatus import JobStatus
from src.socketio.shared.dto.processing import WorkerResultDto, ProcessingResultDto


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

        worker_manager: WorkerManager = data["__worker_manager__"]
        job_manager: JobManager = data["__job_manager__"]

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

        # DO NOT emit immediately - need to handle async completion order
        # Jobs complete asynchronously, must ensure FIFO order when emitting


        # -----------------

        # TODO: Logic đoạn này bị sai, cần tìm dựa trên sender id và sau đó là job id 
        # Tìm job tương ứng với worker_id và pair_id
        jobs_by_worker = job_manager.get_jobs_by_worker(dto.worker_id)
        matched_jobs = [job for job in jobs_by_worker if job.pair_id == dto.pair_id]

        if not matched_jobs:
            print(
                f"[WorkerResultHandler] ⚠️ No matching job found for worker {dto.worker_id} and pair {dto.pair_id}"
            )
            return

        # Lấy job đầu tiên (FIFO)
        job = matched_jobs[0] # TODO: Bỏ dòng này sau khi sửa logic tìm job

        # -----------------



        # Update output và mark status = COMPLETED
        job_manager.update_job_output(job.id, dto.result)
        job_manager.update_job_status(job.id, JobStatus.COMPLETED)

        print(f"[WorkerResultHandler] ✅ Marked job {job.id} as COMPLETED with output")

        # Process completed jobs cascade - chỉ emit nếu job ở đầu queue
        jobs_to_emit = job_manager.process_completed_jobs(dto.sender_id, job.id)

        if not jobs_to_emit:
            print(
                f"[WorkerResultHandler] ⏳ Job {job.id} is waiting for previous jobs to complete"
            )
            print(f"[WorkerResultHandler] Not emitting to receiver yet")
        else:
            # Emit tất cả jobs theo thứ tự FIFO
            print(
                f"[WorkerResultHandler] 📤 Emitting {len(jobs_to_emit)} job(s) to receiver in FIFO order"
            )

            for completed_job in jobs_to_emit:
                result_dto = ProcessingResultDto(
                    pair_id=completed_job.pair_id,
                    sender_id=completed_job.sender_id,
                    worker_id=completed_job.worker_id,
                    original_data=completed_job.input,
                    result=completed_job.output or [],
                )
                payload = result_dto.model_dump(by_alias=True)

                await sio.emit(
                    MainEvents.PROCESSING_RESULT.value,
                    payload,
                    room=dto.receiver_id,
                    namespace=self.namespace.value,
                )

                print(
                    f"[WorkerResultHandler] ✅ Emitted job {completed_job.id} to receiver {dto.receiver_id}"
                )

            print(
                f"[WorkerResultHandler] Sender {dto.sender_id} queue size: {job_manager.count_sender_jobs(dto.sender_id)}"
            )
            print(
                f"[WorkerResultHandler] Total active jobs: {job_manager.count_all_jobs()}"
            )

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
