"""
Main server entry point

Khởi tạo SocketIO server với FastAPI, bao gồm:
- Khởi tạo các state managers (singleton)
- Khởi tạo Kafka event publisher
- Inject dependencies vào MainEventRegistry
- Mount SocketIO app vào FastAPI
"""
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


def create_app() -> FastAPI:
    """
    Create FastAPI application with Socket.IO mounted.

    Returns:
        FastAPI: Application instance với SocketIO đã được mount
    """
    # Create FastAPI app
    app = FastAPI(title="SocketIO Server", version="1.0.0")

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

    # Initialize Kafka Event Publisher
    kafka_config = KafkaConfig()
    kafka_producer = KafkaProducerFactory(kafka_config).get_instance()
    serializer = JsonSerializer()
    event_publisher = KafkaEventPublisher(kafka_producer, serializer)

    print("[run_server] Kafka event publisher initialized")

    # Register Event Handlers with Injected Dependencies
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

    # Mount Socket.IO app at /socket.io path
    app.mount("/", socket_app)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost", "*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    return app
