"""
WorkerManager - Singleton manager quản lý active workers

Quản lý tập hợp các worker đang active, cho phép thêm, xóa và cập nhật trạng thái.
"""
from typing import Optional

from src.socketio.socketio_server.main.model.WorkerData import WorkerData
from src.socketio.socketio_server.main.enum.WorkerStatus import WorkerStatus


class WorkerManager:
    """
    Singleton manager quản lý active workers.

    Structure của active workers:
    {
        "worker_id_1": WorkerData(status=ACTIVE),
        "worker_id_2": WorkerData(status=IDLE),
    }

    Usage:
        manager = WorkerManager()
        manager.add_worker("worker_123", WorkerStatus.IDLE)
        manager.update_status("worker_123", WorkerStatus.ACTIVE)
        manager.remove_worker("worker_123")
    """

    _instance = None
    _workers: dict[str, WorkerData] = {}

    def __new__(cls):
        """
        Singleton pattern - đảm bảo chỉ có 1 instance duy nhất.

        Returns:
            WorkerManager instance duy nhất
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._workers = {}
        return cls._instance

    def add_worker(
        self, worker_id: str, status: WorkerStatus = WorkerStatus.IDLE
    ):
        """
        Thêm worker vào pool.

        Args:
            worker_id: ID của worker
            status: Trạng thái ban đầu (default: IDLE)
        """
        self._workers[worker_id] = WorkerData(status=status)
        print(f"[WorkerManager] Added worker {worker_id} with status {status.value}")

    def remove_worker(self, worker_id: str) -> bool:
        """
        Xóa worker khỏi pool.

        Args:
            worker_id: ID của worker cần xóa

        Returns:
            True nếu xóa thành công, False nếu worker không tồn tại
        """
        if worker_id in self._workers:
            del self._workers[worker_id]
            print(f"[WorkerManager] Removed worker {worker_id}")
            return True
        return False

    def update_status(self, worker_id: str, status: WorkerStatus) -> bool:
        """
        Cập nhật status của worker.

        Args:
            worker_id: ID của worker
            status: Trạng thái mới

        Returns:
            True nếu cập nhật thành công, False nếu worker không tồn tại
        """
        if worker_id in self._workers:
            self._workers[worker_id].status = status
            print(
                f"[WorkerManager] Updated worker {worker_id} status to {status.value}"
            )
            return True
        return False

    def get_worker(self, worker_id: str) -> Optional[WorkerData]:
        """
        Lấy thông tin worker theo ID.

        Args:
            worker_id: ID của worker

        Returns:
            WorkerData nếu tồn tại, None nếu không tìm thấy
        """
        return self._workers.get(worker_id)

    def get_all_workers(self) -> dict[str, WorkerData]:
        """
        Lấy tất cả workers.

        Returns:
            Dict chứa tất cả workers (copy để tránh modification)
        """
        return self._workers.copy()

    def get_workers_by_status(self, status: WorkerStatus) -> dict[str, WorkerData]:
        """
        Lấy tất cả workers theo trạng thái.

        Args:
            status: Trạng thái cần filter

        Returns:
            Dict chứa các workers có trạng thái phù hợp
        """
        return {
            worker_id: data
            for worker_id, data in self._workers.items()
            if data.status == status
        }

    def count(self) -> int:
        """
        Đếm tổng số workers.

        Returns:
            Số lượng workers
        """
        return len(self._workers)

    def count_by_status(self, status: WorkerStatus) -> int:
        """
        Đếm số workers theo trạng thái.

        Args:
            status: Trạng thái cần đếm

        Returns:
            Số lượng workers có trạng thái phù hợp
        """
        return sum(1 for data in self._workers.values() if data.status == status)
