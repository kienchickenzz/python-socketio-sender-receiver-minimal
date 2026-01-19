"""
SenderDisconnectedHandler - Xử lý khi sender ngắt kết nối

Handler chỉ publish sự kiện vào Kafka, không xử lý logic trực tiếp.
Logic xử lý cleanup được chuyển sang Kafka consumer.
"""
from datetime import datetime
from socketio import AsyncServer

from src.kafka.producer.KafkaEventPublisher import KafkaEventPublisher
from src.kafka.producer.server.dto.SenderDisconnectedEventDto import SenderDisconnectedEventDto

from src.socketio.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio.socketio_server.main.enum.MainNamespace import MainNamespaces


class SenderDisconnectedHandler(IEventHandler):
    """
    Handler xử lý sự kiện sender-disconnected.

    Chỉ publish event vào Kafka, không xử lý logic cleanup trực tiếp.
    Logic cleanup được xử lý bởi Kafka SenderDisconnectConsumerHandler.

    Flow:
        1. Nhận sự kiện sender disconnect từ SocketIO
        2. Tạo SenderDisconnectedEventDto (Kafka DTO) với timestamp
        3. Publish lên Kafka
        4. Kafka consumer sẽ xử lý logic cleanup
    """

    event = MainEvents.SENDER_DISCONNECTED
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
        Publish sự kiện sender disconnected vào Kafka.

        Args:
            sio (AsyncServer): SocketIO AsyncServer instance
            client_sid (str | None): Socket ID của sender đã disconnect
            data: Data từ event (không sử dụng)

        Returns:
            None (fire-and-forget)
        """
        if not client_sid:
            print(f"[SenderDisconnectedHandler] No client_sid provided")
            return

        timestamp = datetime.now().isoformat()

        print(f"\n{'='*60}")
        print(f"[SenderDisconnectedHandler] SENDER DISCONNECTED")
        print(f"[SenderDisconnectedHandler] Client SID: {client_sid}")
        print(f"[SenderDisconnectedHandler] Timestamp: {timestamp}")
        print(f"[SenderDisconnectedHandler] Publishing to Kafka...")
        print(f"{'='*60}\n")

        # Tạo Kafka DTO và publish
        kafka_dto = SenderDisconnectedEventDto(
            sender_id=client_sid,
            timestamp=timestamp,
        )
        self._publisher.publish(kafka_dto)

        print(f"[SenderDisconnectedHandler] Published to Kafka topic: {kafka_dto.get_topic().value}")
        print(f"{'='*60}\n")
