"""
JobManager - Singleton manager quản lý active jobs

Quản lý tập hợp các job đang được xử lý, cho phép thêm, xóa và truy vấn.
"""
import uuid
from typing import Optional

from src.socketio_server.main.model.JobData import JobData


class JobManager:
    """
    Singleton manager quản lý active jobs.

    Structure của active jobs:
    {
        "job_id_1": JobData(worker_id="worker1", sender_id="sender1", pair_id="pair1"),
        "job_id_2": JobData(worker_id="worker2", sender_id="sender2", pair_id="pair2"),
    }

    Usage:
        manager = JobManager()
        job_id = manager.add_job("worker_123", "sender_456", "pair_789")
        job = manager.get_job(job_id)
        manager.remove_job(job_id)
    """

    _instance = None
    _jobs: dict[str, JobData] = {}

    def __new__(cls):
        """
        Singleton pattern - đảm bảo chỉ có 1 instance duy nhất.

        Returns:
            JobManager instance duy nhất
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._jobs = {}
        return cls._instance

    def add_job(self, worker_id: str, sender_id: str, pair_id: str) -> str:
        """
        Thêm job mới vào manager.

        Args:
            worker_id: ID của worker đang xử lý
            sender_id: ID của sender đã gửi request
            pair_id: ID của cặp sender-receiver

        Returns:
            job_id đã được tạo (auto-generated UUID)
        """
        job_id = str(uuid.uuid4())
        self._jobs[job_id] = JobData(
            worker_id=worker_id, sender_id=sender_id, pair_id=pair_id
        )
        print(
            f"[JobManager] Added job {job_id}: worker={worker_id}, sender={sender_id}, pair={pair_id}"
        )
        return job_id

    def remove_job(self, job_id: str) -> bool:
        """
        Xóa job khỏi manager.

        Args:
            job_id: ID của job cần xóa

        Returns:
            True nếu xóa thành công, False nếu job không tồn tại
        """
        if job_id in self._jobs:
            job_data = self._jobs[job_id]
            del self._jobs[job_id]
            print(
                f"[JobManager] Removed job {job_id}: worker={job_data.worker_id}, pair={job_data.pair_id}"
            )
            return True
        return False

    def get_job(self, job_id: str) -> Optional[JobData]:
        """
        Lấy thông tin job theo ID.

        Args:
            job_id: ID của job

        Returns:
            JobData nếu tồn tại, None nếu không tìm thấy
        """
        return self._jobs.get(job_id)

    def get_all_jobs(self) -> Dict[str, JobData]:
        """
        Lấy tất cả jobs.

        Returns:
            Dict chứa tất cả jobs (copy để tránh modification)
        """
        return self._jobs.copy()

    def get_jobs_by_worker(self, worker_id: str) -> Dict[str, JobData]:
        """
        Lấy tất cả jobs của một worker.

        Args:
            worker_id: ID của worker

        Returns:
            Dict chứa các jobs được xử lý bởi worker này
        """
        return {
            job_id: data
            for job_id, data in self._jobs.items()
            if data.worker_id == worker_id
        }

    def get_jobs_by_sender(self, sender_id: str) -> Dict[str, JobData]:
        """
        Lấy tất cả jobs của một sender.

        Args:
            sender_id: ID của sender

        Returns:
            Dict chứa các jobs từ sender này
        """
        return {
            job_id: data
            for job_id, data in self._jobs.items()
            if data.sender_id == sender_id
        }

    def get_jobs_by_pair(self, pair_id: str) -> Dict[str, JobData]:
        """
        Lấy tất cả jobs của một pair.

        Args:
            pair_id: ID của pair

        Returns:
            Dict chứa các jobs của pair này
        """
        return {
            job_id: data for job_id, data in self._jobs.items() if data.pair_id == pair_id
        }

    def count(self) -> int:
        """
        Đếm tổng số jobs đang active.

        Returns:
            Số lượng jobs
        """
        return len(self._jobs)
