"""
WorkerActiveHandler - Xử lý khi worker báo active

Handler xử lý khi worker emit worker_active event sau khi nhận được session ID.
"""
from socketio import AsyncServer
from pydantic import ValidationError

from src.socketio.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio.socketio_server.main.enum.MainNamespace import MainNamespaces
from src.socketio.socketio_server.main.manager.WorkerManager import WorkerManager
from src.socketio.socketio_server.main.enum.WorkerStatus import WorkerStatus
from src.socketio.shared.dto.connection import WorkerActiveDto


class WorkerActiveHandler(IEventHandler):
    """
    Handler xử lý sự kiện worker_active.

    Stateless handler - nhận worker_id từ client và quản lý trạng thái trong WorkerManager.
    Logic:
    - Nếu worker chưa có trong manager → thêm mới với status ACTIVE
    - Nếu worker đã có:
      - Status là IDLE → update sang ACTIVE
      - Status đã là ACTIVE → không làm gì
    """

    event = MainEvents.WORKER_ACTIVE
    namespace = MainNamespaces.ROOT

    async def handle(self, sio: AsyncServer, client_sid: str | None, data=None):
        """
        Xử lý khi worker emit worker_active event.

        Args:
            sio: SocketIO AsyncServer instance
            client_sid: Socket ID của worker
            data: Dict chứa:
                - dto.session_id: ID của worker
                - __worker_manager__: WorkerManager injected từ registry

        Returns:
            None (fire-and-forget)
        """
        if not data:
            print(f"[WorkerActiveHandler] No data received from {client_sid}")
            return

        worker_manager: WorkerManager | None = data.get("__worker_manager__")
        if not worker_manager:
            print(f"[WorkerActiveHandler] No WorkerManager injected")
            return

        # Deserialize data thành DTO
        try:
            dto = WorkerActiveDto(**data)
        except ValidationError as e:
            print(f"[WorkerActiveHandler] Invalid data format from {client_sid}: {e}")
            return

        # Check xem worker đã tồn tại trong manager chưa
        existing_worker = worker_manager.get_worker(dto.session_id)

        if existing_worker is None:
            # Worker chưa có → thêm mới với status ACTIVE
            worker_manager.add_worker(dto.session_id, WorkerStatus.ACTIVE)
            print(f"\n{'='*60}")
            print(f"[WorkerActiveHandler] ✅ NEW WORKER REGISTERED")
            print(f"[WorkerActiveHandler] Worker ID: {dto.session_id}")
            print(f"[WorkerActiveHandler] Status: ACTIVE")
            print(f"[WorkerActiveHandler] Total workers: {worker_manager.count()}")
            print(f"{'='*60}\n")

        else:
            # Worker đã tồn tại → check status
            if existing_worker.status == WorkerStatus.IDLE:
                # Update từ IDLE sang ACTIVE
                worker_manager.update_status(dto.session_id, WorkerStatus.ACTIVE)
                print(f"\n{'='*60}")
                print(f"[WorkerActiveHandler] 🔄 WORKER STATUS UPDATED")
                print(f"[WorkerActiveHandler] Worker ID: {dto.session_id}")
                print(f"[WorkerActiveHandler] Old Status: IDLE")
                print(f"[WorkerActiveHandler] New Status: ACTIVE")
                print(f"{'='*60}\n")

            elif existing_worker.status == WorkerStatus.ACTIVE:
                # Đã ACTIVE rồi → không làm gì
                print(f"[WorkerActiveHandler] Worker {dto.session_id} is already ACTIVE, no action needed")
