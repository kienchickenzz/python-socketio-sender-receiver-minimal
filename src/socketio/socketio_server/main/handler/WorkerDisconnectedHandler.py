"""
WorkerDisconnectedHandler - Xử lý khi worker ngắt kết nối

Handler chỉ publish sự kiện vào Kafka, không xử lý logic trực tiếp.
Logic xử lý cleanup được chuyển sang Kafka consumer.
"""
from datetime import datetime
from socketio import AsyncServer

from src.socketio.socketio_server.main.kafka_producer.KafkaEventPublisher import KafkaEventPublisher
from src.socketio.socketio_server.main.kafka_producer.server.dto.WorkerDisconnectedEventDto import WorkerDisconnectedEventDto

from src.socketio.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio.socketio_server.main.enum.MainNamespace import MainNamespaces


class WorkerDisconnectedHandler(IEventHandler):
    """
    Handler xử lý sự kiện worker-disconnected.

    Chỉ publish event vào Kafka, không xử lý logic cleanup trực tiếp.
    Logic cleanup được xử lý bởi Kafka WorkerDisconnectConsumerHandler.

    Flow:
        1. Nhận sự kiện worker disconnect từ SocketIO
        2. Tạo WorkerDisconnectedEventDto (Kafka DTO) với timestamp
        3. Publish lên Kafka
        4. Kafka consumer sẽ xử lý logic cleanup (xóa worker khỏi pool)
    """

    event = MainEvents.WORKER_DISCONNECTED
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
        Publish sự kiện worker disconnected vào Kafka.

        Args:
            sio (AsyncServer): SocketIO AsyncServer instance
            client_sid (str | None): Socket ID của worker đã disconnect
            data: Data từ event (không sử dụng)

        Returns:
            None (fire-and-forget)
        """
        if not client_sid:
            print(f"[WorkerDisconnectedHandler] No client_sid provided")
            return

        timestamp = datetime.now().isoformat()

        print(f"\n{'='*60}")
        print(f"[WorkerDisconnectedHandler] WORKER DISCONNECTED")
        print(f"[WorkerDisconnectedHandler] Client SID: {client_sid}")
        print(f"[WorkerDisconnectedHandler] Timestamp: {timestamp}")
        print(f"[WorkerDisconnectedHandler] Publishing to Kafka...")
        print(f"{'='*60}\n")

        # Tạo Kafka DTO và publish
        kafka_dto = WorkerDisconnectedEventDto(
            worker_id=client_sid,
            timestamp=timestamp,
        )
        self._publisher.publish(kafka_dto)

        print(f"[WorkerDisconnectedHandler] Published to Kafka topic: {kafka_dto.get_topic().value}")
        print(f"{'='*60}\n")
