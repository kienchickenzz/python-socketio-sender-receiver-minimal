"""
WorkerResultHandler - Xử lý khi worker trả về kết quả

Handler chỉ publish sự kiện vào Kafka, không xử lý logic trực tiếp.
Logic xử lý (tìm job, update status, emit về receiver) được chuyển sang Kafka consumer.
"""
from socketio import AsyncServer
from pydantic import ValidationError

from src.kafka.producer.KafkaEventPublisher import KafkaEventPublisher
from src.kafka.producer.server.dto.WorkerResultEventDto import WorkerResultEventDto

from src.socketio.shared.dto.processing import WorkerResultDto

from src.socketio.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio.socketio_server.main.enum.MainNamespace import MainNamespaces


class WorkerResultHandler(IEventHandler):
    """
    Handler xử lý sự kiện worker-result.

    Chỉ publish event vào Kafka, không xử lý logic trực tiếp.
    Logic xử lý được thực hiện bởi Kafka WorkerResultConsumerHandler.

    Flow:
        1. Nhận sự kiện worker-result từ SocketIO
        2. Validate data format với WorkerResultDto (client DTO)
        3. Tạo WorkerResultEventDto (Kafka DTO) và publish
        4. Kafka consumer sẽ:
           - Tìm job theo sender_id và job_id
           - Update job output và status = COMPLETED
           - Xử lý cascade completed jobs (FIFO ordering)
           - Cập nhật worker status về ACTIVE
           - Publish emit events để gửi kết quả về receiver
    """

    event = MainEvents.WORKER_RESULT
    namespace = MainNamespaces.ROOT

    def __init__(self, event_publisher: KafkaEventPublisher):
        """
        Khởi tạo handler với Kafka publisher được inject từ bên ngoài.

        Args:
            event_publisher (KafkaEventPublisher): Generic publisher để publish events
        """
        self._publisher = event_publisher

    async def handle(self, sio: AsyncServer, client_sid: str | None, data=None):
        """
        Publish sự kiện worker result vào Kafka.

        Args:
            sio (AsyncServer): SocketIO AsyncServer instance
            client_sid (str | None): Socket ID của worker
            data: Dict chứa:
                - job_id: ID của job được tạo bởi JobManager
                - pair_id: ID của cặp sender-receiver
                - sender_id: ID của sender
                - receiver_id: ID của receiver
                - worker_id: ID của worker đã xử lý
                - original_data: Data gốc
                - result: Kết quả đã xử lý (sorted data)

        Returns:
            None: fire-and-forget
        """
        if not data:
            print(f"[WorkerResultHandler] No data received from {client_sid}")
            return

        # Validate data format từ client
        try:
            client_dto = WorkerResultDto(**data)
        except ValidationError as e:
            print(f"[WorkerResultHandler] Invalid data format from {client_sid}: {e}")
            return

        print(f"\n{'='*60}")
        print(f"[WorkerResultHandler] WORKER RESULT RECEIVED")
        print(f"[WorkerResultHandler] Job ID: {client_dto.job_id}")
        print(f"[WorkerResultHandler] Pair ID: {client_dto.pair_id}")
        print(f"[WorkerResultHandler] Sender ID: {client_dto.sender_id}")
        print(f"[WorkerResultHandler] Receiver ID: {client_dto.receiver_id}")
        print(f"[WorkerResultHandler] Worker ID: {client_dto.worker_id}")
        print(f"[WorkerResultHandler] Client SID: {client_sid}")
        print(f"[WorkerResultHandler] Publishing to Kafka...")
        print(f"{'='*60}\n")

        # Chuyển đổi client DTO sang Kafka DTO và publish
        kafka_dto = WorkerResultEventDto(
            client_sid=client_sid,
            job_id=client_dto.job_id,
            pair_id=client_dto.pair_id,
            sender_id=client_dto.sender_id,
            receiver_id=client_dto.receiver_id,
            worker_id=client_dto.worker_id,
            original_data=client_dto.original_data,
            result=client_dto.result,
        )
        self._publisher.publish(kafka_dto)

        print(f"[WorkerResultHandler] Published to Kafka topic: {kafka_dto.get_topic().value}")
        print(f"{'='*60}\n")
