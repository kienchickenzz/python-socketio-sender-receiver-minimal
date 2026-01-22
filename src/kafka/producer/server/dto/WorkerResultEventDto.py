"""
WorkerResultEventDto - DTO cho sự kiện worker result publish lên Kafka.

Được sử dụng khi worker trả kết quả về server,
SocketIO handler sẽ tạo DTO này và publish lên Kafka.
"""
from typing import Any

from src.kafka.producer.shared.dto.KafkaEventDto import KafkaEventDto
from src.kafka.producer.shared.enum.KafkaTopic import KafkaTopic
from src.kafka.producer.server.enum.ServerTopic import ServerTopic


class WorkerResultEventDto(KafkaEventDto):
    """
    DTO cho sự kiện worker result publish lên Kafka.

    Được publish khi worker emit worker-result event.
    Consumer sẽ nhận message này và xử lý business logic:
    - Tìm job theo sender_id và job_id
    - Update job output và status
    - Xử lý cascade completed jobs (FIFO ordering)

    Fields:
        client_sid: Socket ID của worker (để tracking)
        job_id: ID của job được tạo bởi JobManager
        pair_id: ID của cặp sender-receiver
        sender_id: ID của sender
        receiver_id: ID của receiver
        worker_id: ID của worker đã xử lý
        original_data: Dữ liệu gốc
        result: Kết quả đã xử lý (sorted data)

    Example:
        dto = WorkerResultEventDto(
            client_sid="socket-abc",
            job_id="job-123",
            pair_id="pair-123",
            sender_id="sender-123",
            receiver_id="receiver-456",
            worker_id="worker-789",
            original_data=[5, 3, 1, 4, 2],
            result=[1, 2, 3, 4, 5]
        )
        publisher.publish(dto)
    """

    client_sid: str
    job_id: str
    pair_id: str
    sender_id: str
    receiver_id: str
    worker_id: str
    original_data: list[Any]
    result: list[Any]

    @classmethod
    def get_topic(cls) -> KafkaTopic:
        """
        Trả về topic cho sự kiện worker result.

        Returns:
            KafkaTopic: ServerTopic.WORKER_RESULT
        """
        return ServerTopic.WORKER_RESULT
