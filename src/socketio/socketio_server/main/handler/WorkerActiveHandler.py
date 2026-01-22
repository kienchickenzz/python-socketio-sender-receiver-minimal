"""
WorkerActiveHandler - Xử lý khi worker báo active

Handler chỉ publish sự kiện vào Kafka, không xử lý logic trực tiếp.
Logic xử lý (thêm/update worker trong pool) được chuyển sang Kafka consumer.
"""
from socketio import AsyncServer
from pydantic import ValidationError

from src.socketio.socketio_server.main.kafka_producer.KafkaEventPublisher import KafkaEventPublisher
from src.socketio.socketio_server.main.kafka_producer.server.dto.WorkerActiveEventDto import WorkerActiveEventDto

from src.socketio.shared.dto.connection import WorkerActiveDto

from src.socketio.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio.socketio_server.main.enum.MainNamespace import MainNamespaces


class WorkerActiveHandler(IEventHandler):
    """
    Handler xử lý sự kiện worker_active.

    Chỉ publish event vào Kafka, không xử lý logic trực tiếp.
    Logic xử lý được thực hiện bởi Kafka WorkerActiveConsumerHandler.

    Flow:
        1. Nhận sự kiện worker_active từ SocketIO
        2. Validate data format với WorkerActiveDto (client DTO)
        3. Tạo WorkerActiveEventDto (Kafka DTO) và publish
        4. Kafka consumer sẽ thêm/update worker trong pool
    """

    event = MainEvents.WORKER_ACTIVE
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
        Publish sự kiện worker active vào Kafka.

        Args:
            sio (AsyncServer): SocketIO AsyncServer instance
            client_sid (str | None): Socket ID của worker
            data: Dict chứa:
                - session_id: ID của worker

        Returns:
            None (fire-and-forget)
        """
        if not data:
            print(f"[WorkerActiveHandler] No data received from {client_sid}")
            return

        # Validate data format từ client
        try:
            client_dto = WorkerActiveDto(**data)
        except ValidationError as e:
            print(f"[WorkerActiveHandler] Invalid data format from {client_sid}: {e}")
            return

        print(f"\n{'='*60}")
        print(f"[WorkerActiveHandler] WORKER ACTIVE")
        print(f"[WorkerActiveHandler] Session ID: {client_dto.session_id}")
        print(f"[WorkerActiveHandler] Client SID: {client_sid}")
        print(f"[WorkerActiveHandler] Publishing to Kafka...")
        print(f"{'='*60}\n")

        # Chuyển đổi client DTO sang Kafka DTO và publish
        kafka_dto = WorkerActiveEventDto(session_id=client_dto.session_id)
        self._publisher.publish(kafka_dto)

        print(f"[WorkerActiveHandler] Published to Kafka topic: {kafka_dto.get_topic().value}")
        print(f"{'='*60}\n")
