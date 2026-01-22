"""
ServerRegistry - Registry cho Server domain Kafka consumers

Đăng ký và quản lý các consumer handlers cho server events:
- SenderDisconnectConsumerHandler: Xử lý khi sender disconnect
- ReceiverReadyConsumerHandler: Xử lý khi receiver sẵn sàng
- WorkerActiveConsumerHandler: Xử lý khi worker báo active
- SenderPairRequestConsumerHandler: Xử lý khi sender yêu cầu pair
- RequestProcessingConsumerHandler: Xử lý khi sender gửi request xử lý
- WorkerResultConsumerHandler: Xử lý khi worker trả kết quả

Nhận KafkaEmitPublisher để các handlers có thể publish emit events
cho SocketIO server emit về client.
"""
from src.kafka.consumer.shared.base.BaseEventRegistry import BaseEventRegistry
from src.kafka.consumer.shared.interface.IEventHandler import IEventHandler
from src.kafka.consumer.shared.infrastructure.KafkaConsumerFactory import KafkaConsumerFactory
from src.kafka.consumer.shared.infrastructure.DeadLetterPublisher import DeadLetterPublisher
from src.kafka.consumer.shared.kafka_publisher.base.KafkaEmitPublisher import KafkaEmitPublisher

from src.kafka.consumer.server.handler.SenderDisconnectConsumerHandler import (
    SenderDisconnectConsumerHandler,
)
from src.kafka.consumer.server.handler.ReceiverReadyConsumerHandler import (
    ReceiverReadyConsumerHandler,
)
from src.kafka.consumer.server.handler.WorkerActiveConsumerHandler import (
    WorkerActiveConsumerHandler,
)
from src.kafka.consumer.server.handler.SenderPairRequestConsumerHandler import (
    SenderPairRequestConsumerHandler,
)
from src.kafka.consumer.server.handler.RequestProcessingConsumerHandler import (
    RequestProcessingConsumerHandler,
)
from src.kafka.consumer.server.handler.WorkerResultConsumerHandler import (
    WorkerResultConsumerHandler,
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

    Nhận managers và emit_publisher từ bên ngoài (dependency injection).
    emit_publisher cho phép handlers publish emit events để SocketIO server
    emit về client.

    Handlers:
        - SenderDisconnectConsumerHandler: Cleanup khi sender disconnect
        - ReceiverReadyConsumerHandler: Thêm receiver vào pool khi ready
        - WorkerActiveConsumerHandler: Thêm/update worker khi active
        - SenderPairRequestConsumerHandler: Xử lý pairing sender với receiver
        - RequestProcessingConsumerHandler: Dispatch job cho worker xử lý
        - WorkerResultConsumerHandler: Xử lý kết quả từ worker, FIFO ordering

    Example:
        # Khởi tạo ở application root
        receiver_manager = ReceiverManager()
        sender_manager = SenderManager()
        emit_publisher = KafkaEmitPublisher(producer, serializer)
        ...

        # Inject vào registry
        registry = ServerRegistry(
            consumer_factory=consumer_factory,
            dlq_publisher=dlq_publisher,
            emit_publisher=emit_publisher,
            receiver_manager=receiver_manager,
            sender_manager=sender_manager,
            pair_manager=pair_manager,
            job_manager=job_manager,
            worker_manager=worker_manager,
        )
        registry.register_all()  # Blocks until shutdown signal
    """

    def __init__(
        self,
        consumer_factory: KafkaConsumerFactory,
        dlq_publisher: DeadLetterPublisher,
        emit_publisher: KafkaEmitPublisher,
        receiver_manager: ReceiverManager,
        sender_manager: SenderManager,
        pair_manager: PairManager,
        job_manager: JobManager,
        worker_manager: WorkerManager,
    ):
        """
        Khởi tạo ServerRegistry với các dependencies được inject từ bên ngoài.

        Args:
            consumer_factory (KafkaConsumerFactory): Factory để tạo consumers
            dlq_publisher (DeadLetterPublisher): Publisher để gửi failed messages vào DLQ
            emit_publisher (KafkaEmitPublisher): Publisher để publish emit events
            receiver_manager (ReceiverManager): Manager quản lý receivers
            sender_manager (SenderManager): Manager quản lý senders
            pair_manager (PairManager): Manager quản lý pairs
            job_manager (JobManager): Manager quản lý jobs
            worker_manager (WorkerManager): Manager quản lý workers
        """
        self._emit_publisher = emit_publisher
        self._receiver_manager = receiver_manager
        self._sender_manager = sender_manager
        self._pair_manager = pair_manager
        self._job_manager = job_manager
        self._worker_manager = worker_manager

        print("[ServerRegistry] Dependencies injected successfully")

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
            WorkerActiveConsumerHandler(
                worker_manager=self._worker_manager,
            ),
            SenderPairRequestConsumerHandler(
                emit_publisher=self._emit_publisher,
                sender_manager=self._sender_manager,
                receiver_manager=self._receiver_manager,
                pair_manager=self._pair_manager,
            ),
            RequestProcessingConsumerHandler(
                emit_publisher=self._emit_publisher,
                worker_manager=self._worker_manager,
                job_manager=self._job_manager,
            ),
            WorkerResultConsumerHandler(
                emit_publisher=self._emit_publisher,
                worker_manager=self._worker_manager,
                job_manager=self._job_manager,
            ),
        ]
