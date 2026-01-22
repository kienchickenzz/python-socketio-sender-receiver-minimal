"""
ReceiverReadyHandler - Xử lý khi receiver sẵn sàng

Handler chỉ publish sự kiện vào Kafka, không xử lý logic trực tiếp.
Logic xử lý (thêm receiver vào pool) được chuyển sang Kafka consumer.
"""
from socketio import AsyncServer
from pydantic import ValidationError

from src.socketio.socketio_server.main.kafka_producer.KafkaEventPublisher import KafkaEventPublisher
from src.socketio.socketio_server.main.kafka_producer.server.dto.ReceiverReadyEventDto import ReceiverReadyEventDto

from src.socketio.shared.dto.connection import ReceiverReadyDto

from src.socketio.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio.socketio_server.main.enum.MainNamespace import MainNamespaces


class ReceiverReadyHandler(IEventHandler):
    """
    Handler xử lý sự kiện receiver_ready.

    Chỉ publish event vào Kafka, không xử lý logic trực tiếp.
    Logic xử lý được thực hiện bởi Kafka ReceiverReadyConsumerHandler.

    Flow:
        1. Nhận sự kiện receiver_ready từ SocketIO
        2. Validate data format với ReceiverReadyDto (client DTO)
        3. Tạo ReceiverReadyEventDto (Kafka DTO) và publish
        4. Kafka consumer sẽ thêm receiver vào pool
    """

    event = MainEvents.RECEIVER_READY
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
        Publish sự kiện receiver ready vào Kafka.

        Args:
            sio (AsyncServer): SocketIO AsyncServer instance
            client_sid (str | None): Socket ID của receiver
            data: Dict chứa:
                - session_id: ID của receiver

        Returns:
            None (fire-and-forget)
        """
        if not data:
            print(f"[ReceiverReadyHandler] No data received from {client_sid}")
            return

        # Validate data format từ client
        try:
            client_dto = ReceiverReadyDto(**data)
        except ValidationError as e:
            print(f"[ReceiverReadyHandler] Invalid data format from {client_sid}: {e}")
            return

        print(f"\n{'='*60}")
        print(f"[ReceiverReadyHandler] RECEIVER READY")
        print(f"[ReceiverReadyHandler] Session ID: {client_dto.session_id}")
        print(f"[ReceiverReadyHandler] Client SID: {client_sid}")
        print(f"[ReceiverReadyHandler] Publishing to Kafka...")
        print(f"{'='*60}\n")

        # Chuyển đổi client DTO sang Kafka DTO và publish
        kafka_dto = ReceiverReadyEventDto(session_id=client_dto.session_id)
        self._publisher.publish(kafka_dto)

        print(f"[ReceiverReadyHandler] Published to Kafka topic: {kafka_dto.get_topic().value}")
        print(f"{'='*60}\n")
