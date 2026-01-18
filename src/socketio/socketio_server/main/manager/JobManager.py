"""
JobManager - Singleton manager quản lý active jobs theo sender queues

Quản lý các job theo FIFO queue riêng biệt cho mỗi sender.
"""
import uuid
from typing import Optional, Any

from src.socketio.socketio_server.main.model.JobData import JobData
from src.socketio.socketio_server.main.enum.JobStatus import JobStatus


class JobManager:
    """
    Singleton manager quản lý active jobs theo sender queues.

    Structure của job queues:
    {
        "sender_id_1": [JobData1, JobData2, ...],  # FIFO queue
        "sender_id_2": [JobData3, JobData4, ...],
    }

    Mỗi sender có một queue riêng, jobs được xử lý theo thứ tự FIFO.

    Usage:
        manager = JobManager()
        job_id = manager.add_job("sender_123", "worker_456", "pair_789", [1,2,3])
        job = manager.get_job(job_id)
        manager.update_job_output(job_id, [1,2,3])
        manager.update_job_status(job_id, JobStatus.COMPLETED)
    """

    _instance = None
    _sender_queues: dict[str, list[JobData]] = {}

    def __new__(cls):
        """
        Singleton pattern - đảm bảo chỉ có 1 instance duy nhất.

        Returns:
            JobManager instance duy nhất
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._sender_queues = {}
        return cls._instance

    def add_job(
        self,
        sender_id: str,
        worker_id: str,
        pair_id: str,
        input_data: list[Any],
        status: JobStatus = JobStatus.IN_PROGRESS,
    ) -> str:
        """
        Thêm job mới vào queue của sender (FIFO).

        Args:
            sender_id: ID của sender đã gửi request
            worker_id: ID của worker đang xử lý
            pair_id: ID của cặp sender-receiver
            input_data: Dữ liệu đầu vào cần xử lý
            status: Trạng thái ban đầu (default: IN_PROGRESS)

        Returns:
            job_id đã được tạo (auto-generated UUID)
        """
        job_id = str(uuid.uuid4())
        job_data = JobData(
            id=job_id,
            worker_id=worker_id,
            sender_id=sender_id,
            pair_id=pair_id,
            input=input_data,
            output=None,
            status=status,
        )

        # Tạo queue mới nếu sender chưa có queue
        if sender_id not in self._sender_queues:
            self._sender_queues[sender_id] = []

        # Thêm job vào cuối queue (FIFO)
        self._sender_queues[sender_id].append(job_data)

        print(
            f"[JobManager] Added job {job_id} to sender {sender_id}'s queue: "
            f"worker={worker_id}, pair={pair_id}, status={status.value}"
        )
        print(
            f"[JobManager] Sender {sender_id} queue size: {len(self._sender_queues[sender_id])}"
        )

        return job_id

    def remove_job(self, job_id: str) -> bool:
        """
        Xóa job khỏi queue theo ID.

        Args:
            job_id: ID của job cần xóa

        Returns:
            True nếu xóa thành công, False nếu job không tồn tại
        """
        # Tìm job trong tất cả queues
        for sender_id, queue in self._sender_queues.items():
            for i, job in enumerate(queue):
                if job.id == job_id:
                    removed_job = queue.pop(i)
                    print(
                        f"[JobManager] Removed job {job_id} from sender {sender_id}'s queue: "
                        f"worker={removed_job.worker_id}, pair={removed_job.pair_id}"
                    )
                    print(
                        f"[JobManager] Sender {sender_id} queue size: {len(queue)}"
                    )

                    # Xóa queue nếu rỗng
                    if len(queue) == 0:
                        del self._sender_queues[sender_id]
                        print(f"[JobManager] Removed empty queue for sender {sender_id}")

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
        # Tìm job trong tất cả queues
        for queue in self._sender_queues.values():
            for job in queue:
                if job.id == job_id:
                    return job
        return None

    def get_sender_queue(self, sender_id: str) -> list[JobData]:
        """
        Lấy queue của một sender.

        Args:
            sender_id: ID của sender

        Returns:
            List các JobData trong queue (copy để tránh modification)
        """
        if sender_id in self._sender_queues:
            return self._sender_queues[sender_id].copy()
        return []

    def get_first_job(self, sender_id: str) -> Optional[JobData]:
        """
        Lấy job đầu tiên trong queue của sender (không remove).

        Args:
            sender_id: ID của sender

        Returns:
            JobData đầu tiên nếu queue không rỗng, None nếu rỗng
        """
        if sender_id in self._sender_queues and len(self._sender_queues[sender_id]) > 0:
            return self._sender_queues[sender_id][0]
        return None

    def pop_first_job(self, sender_id: str) -> Optional[JobData]:
        """
        Lấy và remove job đầu tiên trong queue của sender.

        Args:
            sender_id: ID của sender

        Returns:
            JobData đầu tiên nếu queue không rỗng, None nếu rỗng
        """
        if sender_id in self._sender_queues and len(self._sender_queues[sender_id]) > 0:
            job = self._sender_queues[sender_id].pop(0)
            print(
                f"[JobManager] Popped job {job.id} from sender {sender_id}'s queue"
            )
            print(
                f"[JobManager] Sender {sender_id} queue size: {len(self._sender_queues[sender_id])}"
            )

            # Xóa queue nếu rỗng
            if len(self._sender_queues[sender_id]) == 0:
                del self._sender_queues[sender_id]
                print(f"[JobManager] Removed empty queue for sender {sender_id}")

            return job
        return None

    def update_job_status(self, job_id: str, status: JobStatus) -> bool:
        """
        Cập nhật status của job.

        Args:
            job_id: ID của job
            status: Trạng thái mới

        Returns:
            True nếu cập nhật thành công, False nếu job không tồn tại
        """
        job = self.get_job(job_id)
        if job:
            job.status = status
            print(
                f"[JobManager] Updated job {job_id} status to {status.value}"
            )
            return True
        return False

    def update_job_output(self, job_id: str, output: list[Any]) -> bool:
        """
        Cập nhật output của job.

        Args:
            job_id: ID của job
            output: Kết quả đã xử lý

        Returns:
            True nếu cập nhật thành công, False nếu job không tồn tại
        """
        job = self.get_job(job_id)
        if job:
            job.output = output
            print(f"[JobManager] Updated job {job_id} output")
            return True
        return False

    def get_jobs_by_worker(self, worker_id: str) -> list[JobData]:
        """
        Lấy tất cả jobs của một worker.

        Args:
            worker_id: ID của worker

        Returns:
            List các JobData được xử lý bởi worker này
        """
        result = []
        for queue in self._sender_queues.values():
            for job in queue:
                if job.worker_id == worker_id:
                    result.append(job)
        return result

    def get_jobs_by_pair(self, pair_id: str) -> list[JobData]:
        """
        Lấy tất cả jobs của một pair.

        Args:
            pair_id: ID của pair

        Returns:
            List các JobData của pair này
        """
        result = []
        for queue in self._sender_queues.values():
            for job in queue:
                if job.pair_id == pair_id:
                    result.append(job)
        return result

    def get_jobs_by_status(self, status: JobStatus) -> list[JobData]:
        """
        Lấy tất cả jobs theo trạng thái.

        Args:
            status: Trạng thái cần filter

        Returns:
            List các JobData có trạng thái phù hợp
        """
        result = []
        for queue in self._sender_queues.values():
            for job in queue:
                if job.status == status:
                    result.append(job)
        return result

    def count_all_jobs(self) -> int:
        """
        Đếm tổng số jobs trong tất cả queues.

        Returns:
            Tổng số jobs
        """
        return sum(len(queue) for queue in self._sender_queues.values())

    def count_sender_jobs(self, sender_id: str) -> int:
        """
        Đếm số jobs trong queue của sender.

        Args:
            sender_id: ID của sender

        Returns:
            Số lượng jobs trong queue
        """
        if sender_id in self._sender_queues:
            return len(self._sender_queues[sender_id])
        return 0

    def get_all_sender_ids(self) -> list[str]:
        """
        Lấy danh sách tất cả sender IDs có queue.

        Returns:
            List các sender IDs
        """
        return list(self._sender_queues.keys())

    def process_completed_jobs(self, sender_id: str, completed_job_id: str) -> list[JobData]:
        """
        Xử lý cascade các completed jobs từ đầu queue theo thứ tự FIFO.

        Logic xử lý bất đồng bộ:
        - Jobs được xử lý bất đồng bộ, có thể hoàn thành không theo thứ tự
        - Job ở cuối queue có thể hoàn thành trước job ở đầu
        - Nhưng PHẢI đảm bảo emit về receiver theo đúng thứ tự FIFO

        Quy trình:
        1. Tìm job vừa được mark COMPLETED
        2. Nếu job KHÔNG phải phần tử đầu tiên (index > 0):
           - Chỉ giữ status = COMPLETED, không emit
           - Return empty list
        3. Nếu job là phần tử đầu tiên (index = 0):
           - Remove job này và emit
           - Tiếp tục check các jobs tiếp theo:
             - Nếu COMPLETED: remove và emit
             - Nếu chưa COMPLETED: dừng lại
           - Return list các jobs cần emit (theo thứ tự)

        Args:
            sender_id: ID của sender
            completed_job_id: ID của job vừa được mark COMPLETED

        Returns:
            List các JobData cần emit về receiver (theo thứ tự FIFO)
            Empty list nếu job chưa thể emit (đang chờ các job trước nó)

        Example:
            Queue: [Job1(IN_PROGRESS), Job2(IN_PROGRESS), Job3(IN_PROGRESS)]

            # Job3 hoàn thành trước
            process_completed_jobs("sender1", "job3") -> []
            Queue: [Job1(IN_PROGRESS), Job2(IN_PROGRESS), Job3(COMPLETED)]

            # Job1 hoàn thành
            process_completed_jobs("sender1", "job1") -> [Job1]
            Queue: [Job2(IN_PROGRESS), Job3(COMPLETED)]

            # Job2 hoàn thành
            process_completed_jobs("sender1", "job2") -> [Job2, Job3]
            Queue: []
        """
        if sender_id not in self._sender_queues:
            print(f"[JobManager] ⚠️ Sender {sender_id} has no queue")
            return []

        queue = self._sender_queues[sender_id]
        if not queue:
            print(f"[JobManager] ⚠️ Sender {sender_id} queue is empty")
            return []

        # Tìm vị trí của job trong queue
        job_index = -1
        for i, job in enumerate(queue):
            if job.id == completed_job_id:
                job_index = i
                break

        if job_index == -1:
            print(f"[JobManager] ⚠️ Job {completed_job_id} not found in sender {sender_id} queue")
            return []

        # Nếu job KHÔNG phải phần tử đầu tiên
        if job_index > 0:
            print(
                f"[JobManager] Job {completed_job_id} is at position {job_index}, "
                f"waiting for {job_index} job(s) ahead to complete"
            )
            return []

        # Job là phần tử đầu tiên → xử lý cascade
        jobs_to_emit: list[JobData] = []

        # Remove và collect tất cả jobs COMPLETED liên tiếp từ đầu queue
        while queue and queue[0].status == JobStatus.COMPLETED:
            job = queue.pop(0)
            jobs_to_emit.append(job)
            print(
                f"[JobManager] ✅ Job {job.id} ready to emit (position 0, COMPLETED)"
            )

        # Xóa queue nếu rỗng
        if len(queue) == 0:
            del self._sender_queues[sender_id]
            print(f"[JobManager] Removed empty queue for sender {sender_id}")
        else:
            print(
                f"[JobManager] Sender {sender_id} queue size after processing: {len(queue)}"
            )

        print(
            f"[JobManager] 📤 Returning {len(jobs_to_emit)} job(s) to emit in FIFO order"
        )

        return jobs_to_emit
