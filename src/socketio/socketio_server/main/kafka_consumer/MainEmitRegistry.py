"""
MainEmitRegistry - Registry cho Main Server emit consumers.

Đăng ký và quản lý các emit consumer handlers:
- PairRequestSuccessEmitHandler: Emit khi pairing thành công
- PairRequestFailedEmitHandler: Emit khi pairing thất bại
- WorkerJobEmitHandler: Emit job tới worker để xử lý

Nhận AsyncServer và event_loop để emit SocketIO events từ Kafka consumer threads.
"""
import asyncio

from socketio import AsyncServer

from src.socketio.socketio_server.shared.kafka_consumer.base.BaseEmitRegistry import (
    BaseEmitRegistry,
)
from src.socketio.socketio_server.shared.kafka_consumer.interface.IEmitHandler import (
    IEmitHandler,
)
from src.socketio.socketio_server.shared.kafka_consumer.infrastructure.KafkaConsumerFactory import (
    KafkaConsumerFactory,
)
from src.socketio.socketio_server.shared.kafka_consumer.infrastructure.DeadLetterPublisher import (
    DeadLetterPublisher,
)

from src.socketio.socketio_server.main.kafka_consumer.handler.PairRequestSuccessEmitHandler import (
    PairRequestSuccessEmitHandler,
)
from src.socketio.socketio_server.main.kafka_consumer.handler.PairRequestFailedEmitHandler import (
    PairRequestFailedEmitHandler,
)
from src.socketio.socketio_server.main.kafka_consumer.handler.WorkerJobEmitHandler import (
    WorkerJobEmitHandler,
)


class MainEmitRegistry(BaseEmitRegistry):
    """
    Registry cho Main Server emit consumers.

    Đăng ký tất cả emit handlers để nhận events từ Kafka và emit về clients.
    Mỗi handler sẽ có 2 threads: main consumer và DLQ consumer.

    Handlers:
        - PairRequestSuccessEmitHandler: Emit pair-request-success về client
        - PairRequestFailedEmitHandler: Emit pair-request-failed về client
        - WorkerJobEmitHandler: Emit worker-job về worker

    Example:
        # Khởi tạo ở application root
        config = KafkaConfig()
        consumer_factory = KafkaConsumerFactory(config)
        producer_factory = KafkaProducerFactory(config)
        dlq_publisher = DeadLetterPublisher(producer_factory.get_instance(), serializer)

        sio = AsyncServer(async_mode="asgi")
        event_loop = asyncio.get_event_loop()

        registry = MainEmitRegistry(
            consumer_factory=consumer_factory,
            dlq_publisher=dlq_publisher,
            sio=sio,
            event_loop=event_loop,
        )
        registry.register_all()  # Blocks until shutdown signal
    """

    def __init__(
        self,
        consumer_factory: KafkaConsumerFactory,
        dlq_publisher: DeadLetterPublisher,
        sio: AsyncServer,
        event_loop: asyncio.AbstractEventLoop,
    ):
        """
        Khởi tạo MainEmitRegistry với các dependencies.

        Args:
            consumer_factory (KafkaConsumerFactory): Factory để tạo consumers
            dlq_publisher (DeadLetterPublisher): Publisher để gửi failed messages vào DLQ
            sio (AsyncServer): SocketIO AsyncServer instance để emit events
            event_loop (asyncio.AbstractEventLoop): Event loop để chạy async handlers
        """
        print("[MainEmitRegistry] Initializing...")

        # Gọi parent __init__ để đăng ký handlers
        super().__init__(consumer_factory, dlq_publisher, sio, event_loop)

        print("[MainEmitRegistry] Initialized successfully")

    def _create_handlers(self) -> list[IEmitHandler]:
        """
        Tạo danh sách emit handlers cho main server.

        Returns:
            list[IEmitHandler]: Danh sách handler instances
        """
        return [
            PairRequestSuccessEmitHandler(),
            PairRequestFailedEmitHandler(),
            WorkerJobEmitHandler(),
        ]
