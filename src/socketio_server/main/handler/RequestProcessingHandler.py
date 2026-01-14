"""
RequestProcessingHandler - Xử lý khi sender gửi request xử lý data

Handler nhận data từ sender và chuyển tiếp cho worker để xử lý.
"""
import random
from socketio import AsyncServer
from pydantic import ValidationError

from src.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio_server.main.enum.MainNamespace import MainNamespaces
from src.socketio_server.main.manager.WorkerManager import WorkerManager
from src.socketio_server.main.enum.WorkerStatus import WorkerStatus
from src.shared.dto.processing import RequestProcessingDto, WorkerJobDto


class RequestProcessingHandler(IEventHandler):
    """
    Handler xử lý sự kiện request-processing.

    Stateless handler - nhận data từ sender, chọn worker ACTIVE ngẫu nhiên,
    và emit worker-job event cho worker để xử lý.
    """

    event = MainEvents.REQUEST_PROCESSING
    namespace = MainNamespaces.ROOT

    async def handle(self, sio: AsyncServer, client_sid: str | None, data=None):
        """
        Xử lý khi sender emit request-processing event.

        Args:
            sio: SocketIO AsyncServer instance
            sid: Socket ID của sender
            data: Dict chứa:
                - pair_id: ID của cặp sender-receiver
                - sender_id: ID của sender
                - receiver_id: ID của receiver
                - data: Dãy số cần xử lý (list of int)
                - __worker_manager__: WorkerManager injected từ registry

        Returns:
            None (fire-and-forget)
        """
        if not data:
            print(f"[RequestProcessingHandler] No data received from {client_sid}")
            return

        worker_manager: WorkerManager | None = data.get("__worker_manager__")

        # Deserialize data thành DTO
        try:
            dto = RequestProcessingDto(**data)
        except ValidationError as e:
            print(f"[RequestProcessingHandler] Invalid data format from {client_sid}: {e}")
            return

        print(f"\n{'='*60}")
        print(f"[RequestProcessingHandler] 📥 RECEIVED PROCESSING REQUEST")
        print(f"[RequestProcessingHandler] Pair ID: {dto.pair_id}")
        print(f"[RequestProcessingHandler] Sender ID: {dto.sender_id}")
        print(f"[RequestProcessingHandler] Receiver ID: {dto.receiver_id}")
        print(f"[RequestProcessingHandler] Data: {dto.data}")
        print(f"[RequestProcessingHandler] Data length: {len(dto.data)}")
        print(f"{'='*60}\n")

        if not worker_manager:
            print(f"[RequestProcessingHandler] ❌ No WorkerManager injected")
            return

        # Lấy tất cả workers đang ACTIVE
        active_workers = worker_manager.get_workers_by_status(WorkerStatus.ACTIVE)

        if not active_workers:
            print(f"[RequestProcessingHandler] ❌ No ACTIVE workers available")
            # TODO: Có thể emit event thông báo sender không có worker
            return

        # Chọn ngẫu nhiên 1 worker từ danh sách ACTIVE
        worker_id = random.choice(list(active_workers.keys()))

        print(f"[RequestProcessingHandler] 🎯 Selected worker: {worker_id}")
        print(f"[RequestProcessingHandler] Total ACTIVE workers: {len(active_workers)}")

        # Tạo DTO và serialize để emit
        job_dto = WorkerJobDto(
            pair_id=dto.pair_id,
            sender_id=dto.sender_id,
            receiver_id=dto.receiver_id,
            worker_id=worker_id,
            data=dto.data,
        )
        payload = job_dto.model_dump(by_alias=True)

        # Emit worker-job event tới worker được chọn
        await sio.emit(
            MainEvents.WORKER_JOB.value,
            payload,
            room=worker_id,
            namespace=self.namespace.value,
        )

        print(f"[RequestProcessingHandler] ✅ Emitted worker-job to worker {worker_id}")
        print(f"{'='*60}\n")
