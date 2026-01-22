"""
SenderPairRequestHandler - Xử lý khi sender yêu cầu pair với receiver

Handler chỉ publish sự kiện vào Kafka, không xử lý logic trực tiếp.
Logic xử lý (pairing sender với receiver) được chuyển sang Kafka consumer.
"""
from socketio import AsyncServer
from pydantic import ValidationError

from src.kafka.producer.KafkaEventPublisher import KafkaEventPublisher
from src.kafka.producer.server.dto.SenderPairRequestEventDto import (
    SenderPairRequestEventDto,
)

from src.socketio.shared.dto.connection import SenderPairRequestDto

from src.socketio.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio.socketio_server.main.enum.MainNamespace import MainNamespaces


class SenderPairRequestHandler(IEventHandler):
    """
    Handler xử lý sự kiện sender-pair-request.

    Chỉ publish event vào Kafka, không xử lý logic trực tiếp.
    Logic xử lý được thực hiện bởi Kafka SenderPairRequestConsumerHandler.

    Flow:
        1. Nhận sự kiện sender-pair-request từ SocketIO
        2. Validate data format với SenderPairRequestDto (client DTO)
        3. Tạo SenderPairRequestEventDto (Kafka DTO) và publish
        4. Kafka consumer sẽ:
           - Thêm sender vào pool
           - Tìm receiver available
           - Tạo pair nếu có
           - Publish emit events về SocketIO server
    """

    event = MainEvents.SENDER_PAIR_REQUEST
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
        Publish sự kiện sender pair request vào Kafka.

        Args:
            sio (AsyncServer): SocketIO AsyncServer instance
            client_sid (str | None): Socket ID của sender
            data: Dict chứa:
                - session_id: ID của sender

        Returns:
            None (fire-and-forget)
        """
        if not data:
            print(f"[SenderPairRequestHandler] No data received from {client_sid}")
            return

        # Validate data format từ client
        try:
            client_dto = SenderPairRequestDto(**data)
        except ValidationError as e:
            print(f"[SenderPairRequestHandler] Invalid data format from {client_sid}: {e}")
            return

        print(f"\n{'='*60}")
        print(f"[SenderPairRequestHandler] SENDER PAIR REQUEST")
        print(f"[SenderPairRequestHandler] Session ID: {client_dto.session_id}")
        print(f"[SenderPairRequestHandler] Client SID: {client_sid}")
        print(f"[SenderPairRequestHandler] Publishing to Kafka...")
        print(f"{'='*60}\n")

        # Chuyển đổi client DTO sang Kafka DTO và publish
        kafka_dto = SenderPairRequestEventDto(
            session_id=client_dto.session_id,
            client_sid=client_sid,
        )
        self._publisher.publish(kafka_dto)

        print(f"[SenderPairRequestHandler] Published to Kafka topic: {kafka_dto.get_topic().value}")
        print(f"{'='*60}\n")
