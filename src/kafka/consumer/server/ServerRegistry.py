"""
ServerRegistry - Registry cho Server domain Kafka consumers

Đăng ký và quản lý các consumer handlers cho server events:
- SenderDisconnectConsumerHandler: Xử lý khi sender disconnect
- ReceiverReadyConsumerHandler: Xử lý khi receiver sẵn sàng
"""
from src.kafka.consumer.shared.base.BaseEventRegistry import BaseEventRegistry
from src.kafka.consumer.shared.interface.IEventHandler import IEventHandler
from src.kafka.consumer.infrastructure.KafkaConsumerFactory import KafkaConsumerFactory
from src.kafka.consumer.infrastructure.DeadLetterPublisher import DeadLetterPublisher

from src.kafka.consumer.server.handler.SenderDisconnectConsumerHandler import (
    SenderDisconnectConsumerHandler,
)
from src.kafka.consumer.server.handler.ReceiverReadyConsumerHandler import (
    ReceiverReadyConsumerHandler,
)

from src.socketio.socketio_server.main.manager.ReceiverManager import ReceiverManager
from src.socketio.socketio_server.main.manager.SenderManager import SenderManager
from src.socketio.socketio_server.main.manager.PairManager import PairManager
from src.socketio.socketio_server.main.manager.JobManager import JobManager
from src.socketio.socketio_server.main.manager.WorkerManager import WorkerManager


class ServerRegistry(BaseEventRegistry):
    """
    Registry cho Server domain Kafka consumers.

    Đăng ký tất cả handlers xử lý server events từ SocketIO.
    Mỗi handler sẽ có 2 threads: main consumer và DLQ consumer.

    Nhận managers từ bên ngoài (dependency injection) và truyền vào handlers.

    Handlers:
        - SenderDisconnectConsumerHandler: Cleanup khi sender disconnect
        - ReceiverReadyConsumerHandler: Thêm receiver vào pool khi ready

    Example:
        # Khởi tạo managers ở application root
        receiver_manager = ReceiverManager()
        sender_manager = SenderManager()
        ...

        # Inject vào registry
        registry = ServerRegistry(
            consumer_factory=consumer_factory,
            dlq_publisher=dlq_publisher,
            receiver_manager=receiver_manager,
            sender_manager=sender_manager,
            pair_manager=pair_manager,
            job_manager=job_manager,
        )
        registry.register_all()  # Blocks until shutdown signal
    """

    def __init__(
        self,
        consumer_factory: KafkaConsumerFactory,
        dlq_publisher: DeadLetterPublisher,
        receiver_manager: ReceiverManager,
        sender_manager: SenderManager,
        pair_manager: PairManager,
        job_manager: JobManager,
        worker_manager: WorkerManager,
    ):
        """
        Khởi tạo ServerRegistry với các managers được inject từ bên ngoài.

        Args:
            consumer_factory (KafkaConsumerFactory): Factory để tạo consumers
            dlq_publisher (DeadLetterPublisher): Publisher để gửi failed messages vào DLQ
            receiver_manager (ReceiverManager): Manager quản lý receivers
            sender_manager (SenderManager): Manager quản lý senders
            pair_manager (PairManager): Manager quản lý pairs
            job_manager (JobManager): Manager quản lý jobs
        """
        self._receiver_manager = receiver_manager
        self._sender_manager = sender_manager
        self._pair_manager = pair_manager
        self._job_manager = job_manager
        self._worker_manager = worker_manager

        print("[ServerRegistry] Managers injected successfully")

        # Gọi parent __init__ để đăng ký handlers
        super().__init__(consumer_factory, dlq_publisher)

    def _create_handlers(self) -> list[IEventHandler]:
        """
        Tạo danh sách handlers cho server domain với managers được inject.

        Returns:
            list[IEventHandler]: Danh sách handler instances
        """
        return [
            SenderDisconnectConsumerHandler(
                sender_manager=self._sender_manager,
                receiver_manager=self._receiver_manager,
                pair_manager=self._pair_manager,
                job_manager=self._job_manager,
            ),
            ReceiverReadyConsumerHandler(
                receiver_manager=self._receiver_manager,
            ),
        ]
