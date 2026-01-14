"""
JobData - Data model cho active job

Model lưu trữ thông tin về job đang được xử lý.
"""
from dataclasses import dataclass


@dataclass
class JobData:
    """
    Data model cho mỗi active job.

    Attributes:
        worker_id: ID của worker đang xử lý job
        sender_id: ID của sender đã gửi request
        pair_id: ID của cặp sender-receiver
    """

    worker_id: str
    sender_id: str
    pair_id: str

    def to_dict(self) -> dict:
        """
        Convert JobData sang dict để serialize.

        Returns:
            Dict representation của JobData
        """
        return {
            "worker_id": self.worker_id,
            "sender_id": self.sender_id,
            "pair_id": self.pair_id,
        }
