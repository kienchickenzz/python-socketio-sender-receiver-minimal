"""
Main server entry point

Khởi tạo SocketIO server với FastAPI, bao gồm:
- Khởi tạo các state managers (singleton)
- Khởi tạo Kafka event publisher (để publish events lên Kafka)
- Khởi tạo Kafka emit consumer (để nhận events từ Kafka và emit về client)
- Inject dependencies vào MainEventRegistry và MainEmitRegistry
- Mount SocketIO app vào FastAPI
"""
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from socketio import AsyncServer, ASGIApp

from src.socketio.socketio_server.main.registry import MainEventRegistry
from src.socketio.socketio_server.main.manager.ReceiverManager import ReceiverManager
from src.socketio.socketio_server.main.manager.SenderManager import SenderManager
from src.socketio.socketio_server.main.manager.PairManager import PairManager
from src.socketio.socketio_server.main.manager.WorkerManager import WorkerManager
from src.socketio.socketio_server.main.manager.JobManager import JobManager

from src.kafka.shared.base.JsonSerializer import JsonSerializer

from src.kafka.producer.infrastructure.KafkaConfig import KafkaConfig
from src.kafka.producer.infrastructure.KafkaProducerFactory import KafkaProducerFactory
from src.kafka.producer.KafkaEventPublisher import KafkaEventPublisher

from src.socketio.socketio_server.shared.kafka_consumer.infrastructure.KafkaConfig import (
    KafkaConfig as EmitKafkaConfig,
)
from src.socketio.socketio_server.shared.kafka_consumer.infrastructure.KafkaConsumerFactory import (
    KafkaConsumerFactory as EmitConsumerFactory,
)
from src.socketio.socketio_server.shared.kafka_consumer.infrastructure.KafkaProducerFactory import (
    KafkaProducerFactory as EmitProducerFactory,
)
from src.socketio.socketio_server.shared.kafka_consumer.infrastructure.DeadLetterPublisher import (
    DeadLetterPublisher as EmitDLQPublisher,
)
from src.socketio.socketio_server.shared.kafka_consumer.base.JsonSerializer import (
    JsonSerializer as EmitSerializer,
)
from src.socketio.socketio_server.main.kafka_consumer.MainEmitRegistry import (
    MainEmitRegistry,
)


# Global reference để shutdown có thể access
_emit_registry: MainEmitRegistry | None = None


def create_app() -> FastAPI:
    """
    Create FastAPI application with Socket.IO mounted.

    Returns:
        FastAPI: Application instance với SocketIO đã được mount
    """
    global _emit_registry

    # Create SocketIO server
    sio = AsyncServer(
        async_mode='asgi',
        cors_allowed_origins='*',
        logger=False,
        engineio_logger=False
    )

    # Initialize State Managers
    receiver_manager = ReceiverManager()
    sender_manager = SenderManager()
    pair_manager = PairManager()
    worker_manager = WorkerManager()
    job_manager = JobManager()

    print("[run_server] State managers initialized")

    # Initialize Kafka Event Publisher (để publish events lên Kafka)
    kafka_config = KafkaConfig()
    kafka_producer = KafkaProducerFactory(kafka_config).get_instance()
    serializer = JsonSerializer()
    event_publisher = KafkaEventPublisher(kafka_producer, serializer)

    print("[run_server] Kafka event publisher initialized")

    # Initialize Kafka Emit Consumer Infrastructure
    emit_config = EmitKafkaConfig()
    emit_consumer_factory = EmitConsumerFactory(emit_config)
    emit_producer = EmitProducerFactory(emit_config).get_instance()
    emit_serializer = EmitSerializer()
    emit_dlq_publisher = EmitDLQPublisher(emit_producer, emit_serializer)

    print("[run_server] Kafka emit consumer infrastructure initialized")

    # Lifespan context manager
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """
        FastAPI lifespan để manage startup/shutdown.

        Startup: Start emit consumers
        Shutdown: Stop emit consumers gracefully
        """
        global _emit_registry

        # Get current event loop
        event_loop = asyncio.get_event_loop()

        # Create and start emit registry
        _emit_registry = MainEmitRegistry(
            consumer_factory=emit_consumer_factory,
            dlq_publisher=emit_dlq_publisher,
            sio=sio,
            event_loop=event_loop,
        )
        _emit_registry.start()

        print("[run_server] Emit consumers started")

        yield

        # Shutdown
        if _emit_registry:
            _emit_registry.stop()
        print("[run_server] Emit consumers stopped")

    # Create FastAPI app with lifespan
    app = FastAPI(
        title="SocketIO Server",
        version="1.0.0",
        lifespan=lifespan,
    )

    # Register SocketIO Event Handlers
    MainEventRegistry(
        sio=sio,
        receiver_manager=receiver_manager,
        sender_manager=sender_manager,
        pair_manager=pair_manager,
        worker_manager=worker_manager,
        job_manager=job_manager,
        event_publisher=event_publisher,
    )

    # Create Socket.IO ASGI app
    socket_app = ASGIApp(sio, app)

    # Mount Socket.IO app at root path
    app.mount("/", socket_app)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost", "*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    return app
