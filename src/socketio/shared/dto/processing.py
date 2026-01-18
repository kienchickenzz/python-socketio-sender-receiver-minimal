"""
DTOs for PROCESSING FLOW

Định nghĩa các DTO cho luồng xử lý dữ liệu:
- Sender gửi data lên Server (REQUEST_PROCESSING)
- Server phân công cho Worker (WORKER_JOB)
- Worker trả kết quả về Server (WORKER_RESULT)
- Server forward kết quả cho Receiver (PROCESSING_RESULT)
"""
from typing import Any
from src.socketio.shared.dto.DtoBase import DtoBase


class RequestProcessingDto(DtoBase):
    """
    DTO cho sự kiện REQUEST_PROCESSING (Sender → Server).

    Sender gửi data cần xử lý lên server.

    Fields:
        pair_id: ID của cặp sender-receiver
        sender_id: ID của sender
        receiver_id: ID của receiver
        data: Dữ liệu cần xử lý (list of numbers)
    """
    pair_id: str
    sender_id: str
    receiver_id: str
    data: list[Any]


class WorkerJobDto(DtoBase):
    """
    DTO cho sự kiện WORKER_JOB (Server → Worker).

    Server phân công job cho worker xử lý.

    Fields:
        pair_id: ID của cặp sender-receiver
        sender_id: ID của sender
        receiver_id: ID của receiver
        worker_id: ID của worker được chọn
        data: Dữ liệu cần xử lý (list of numbers)
    """
    pair_id: str
    sender_id: str
    receiver_id: str
    worker_id: str
    data: list[Any]


class WorkerResultDto(DtoBase):
    """
    DTO cho sự kiện WORKER_RESULT (Worker → Server).

    Worker trả kết quả đã xử lý về server.

    Fields:
        pair_id: ID của cặp sender-receiver
        sender_id: ID của sender
        receiver_id: ID của receiver
        worker_id: ID của worker đã xử lý
        original_data: Dữ liệu gốc
        result: Kết quả đã xử lý (sorted data)
    """
    pair_id: str
    sender_id: str
    receiver_id: str
    worker_id: str
    original_data: list[Any]
    result: list[Any]


class ProcessingResultDto(DtoBase):
    """
    DTO cho sự kiện PROCESSING_RESULT (Server → Receiver).

    Server forward kết quả từ worker cho receiver.

    Fields:
        pair_id: ID của cặp sender-receiver
        sender_id: ID của sender
        worker_id: ID của worker đã xử lý
        original_data: Dữ liệu gốc
        result: Kết quả đã xử lý (sorted data)
    """
    pair_id: str
    sender_id: str
    worker_id: str
    original_data: list[Any]
    result: list[Any]
