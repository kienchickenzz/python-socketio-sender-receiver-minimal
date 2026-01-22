"""
RequestProcessingHandler - Xử lý khi sender gửi request xử lý data

Handler chỉ publish sự kiện vào Kafka, không xử lý logic trực tiếp.
Logic xử lý (chọn worker, dispatch job) được chuyển sang Kafka consumer.
"""
from socketio import AsyncServer
from pydantic import ValidationError

from src.socketio.socketio_server.main.kafka_producer.KafkaEventPublisher import KafkaEventPublisher
from src.socketio.socketio_server.main.kafka_producer.server.dto.RequestProcessingEventDto import (
    RequestProcessingEventDto,
)

from src.socketio.shared.dto.processing import RequestProcessingDto

from src.socketio.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio.socketio_server.main.enum.MainNamespace import MainNamespaces


class RequestProcessingHandler(IEventHandler):
    """
    Handler xử lý sự kiện request-processing.

    Chỉ publish event vào Kafka, không xử lý logic trực tiếp.
    Logic xử lý được thực hiện bởi Kafka RequestProcessingConsumerHandler.

    Flow:
        1. Nhận sự kiện request-processing từ SocketIO
        2. Validate data format với RequestProcessingDto (client DTO)
        3. Tạo RequestProcessingEventDto (Kafka DTO) và publish
        4. Kafka consumer sẽ:
           - Chọn worker ACTIVE ngẫu nhiên
           - Dispatch job cho worker
           - Tracking job trong JobManager
    """

    event = MainEvents.REQUEST_PROCESSING
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
        Publish sự kiện request processing vào Kafka.

        Args:
            sio (AsyncServer): SocketIO AsyncServer instance
            client_sid (str | None): Socket ID của sender
            data: Dict chứa:
                - pair_id: ID của cặp sender-receiver
                - sender_id: ID của sender
                - receiver_id: ID của receiver
                - data: Dữ liệu cần xử lý (list of numbers)

        Returns:
            None (fire-and-forget)
        """
        if not data:
            print(f"[RequestProcessingHandler] No data received from {client_sid}")
            return

        # Validate data format từ client
        try:
            client_dto = RequestProcessingDto(**data)
        except ValidationError as e:
            print(f"[RequestProcessingHandler] Invalid data format from {client_sid}: {e}")
            return

        print(f"\n{'='*60}")
        print(f"[RequestProcessingHandler] REQUEST PROCESSING")
        print(f"[RequestProcessingHandler] Pair ID: {client_dto.pair_id}")
        print(f"[RequestProcessingHandler] Sender ID: {client_dto.sender_id}")
        print(f"[RequestProcessingHandler] Receiver ID: {client_dto.receiver_id}")
        print(f"[RequestProcessingHandler] Data length: {len(client_dto.data)}")
        print(f"[RequestProcessingHandler] Client SID: {client_sid}")
        print(f"[RequestProcessingHandler] Publishing to Kafka...")
        print(f"{'='*60}\n")

        # Chuyển đổi client DTO sang Kafka DTO và publish
        kafka_dto = RequestProcessingEventDto(
            client_sid=client_sid,
            pair_id=client_dto.pair_id,
            sender_id=client_dto.sender_id,
            receiver_id=client_dto.receiver_id,
            data=client_dto.data,
        )
        self._publisher.publish(kafka_dto)

        print(f"[RequestProcessingHandler] Published to Kafka topic: {kafka_dto.get_topic().value}")
        print(f"{'='*60}\n")
