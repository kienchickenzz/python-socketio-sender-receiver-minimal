"""
BaseEventRegistry - Registry pattern cho Kafka consumer handlers

Trách nhiệm:
- Quản lý danh sách handlers
- Dùng factory để tạo consumer cho từng handler (main + DLQ)
- Mỗi handler chạy 2 threads: main consumer và DLQ consumer
- Graceful shutdown tất cả threads
"""
import signal
import threading
from abc import ABC, abstractmethod
from kafka import KafkaConsumer

from src.consumer.infrastructure.KafkaConsumerFactory import KafkaConsumerFactory
from src.consumer.infrastructure.DeadLetterPublisher import DeadLetterPublisher
from src.consumer.shared.interface.IEventHandler import IEventHandler
from src.consumer.shared.interface.IDLQHandler import IDLQHandler
from src.consumer.shared.model.DLQMessage import DLQMessage, ErrorInfo


class BaseEventRegistry(ABC):
    """
    Base Registry cho Kafka event handlers.

    Mỗi handler sẽ có 2 consumers:
    - Main consumer: xử lý business logic
    - DLQ consumer: xử lý messages fail

    Subclass phải implement _create_handlers() để định nghĩa handlers.

    Example:
        config = KafkaConfig(bootstrap_servers="localhost:9092")
        consumer_factory = KafkaConsumerFactory(config)
        dlq_publisher = DeadLetterPublisher(producer, serializer)

        registry = PredictionRegistry(consumer_factory, dlq_publisher)
        registry.register_all()
    """

    # Poll timeout cho main consumer (ms)
    MAIN_POLL_TIMEOUT_MS = 1000

    # Poll timeout cho DLQ consumer - dài hơn để tiết kiệm resource (ms)
    DLQ_POLL_TIMEOUT_MS = 5000

    def __init__(
        self,
        consumer_factory: KafkaConsumerFactory,
        dlq_publisher: DeadLetterPublisher,
    ) -> None:
        """
        Khởi tạo registry.

        Args:
            consumer_factory (KafkaConsumerFactory): Factory để tạo consumers
            dlq_publisher (DeadLetterPublisher): Publisher để gửi failed messages vào DLQ
        """
        self._factory = consumer_factory
        self._dlq_publisher = dlq_publisher
        self._handlers: dict[str, IEventHandler] = {}
        self._consumers: list[KafkaConsumer] = []
        self._threads: list[threading.Thread] = []
        self._running = False

        # Get handlers from subclass
        handlers = self._create_handlers()
        for handler in handlers:
            self._handlers[handler.topic.value] = handler

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    @abstractmethod
    def _create_handlers(self) -> list[IEventHandler]:
        """
        Tạo và trả về danh sách handlers.

        Subclass PHẢI implement method này.
        Mỗi handler PHẢI implement cả IEventHandler và IDLQHandler.

        Returns:
            list[IEventHandler]: Danh sách handler instances
        """
        pass

    def _signal_handler(self, signum, frame) -> None:
        """Handle shutdown signals."""
        self._running = False

    def _run_main_consumer(
        self,
        handler: IEventHandler,
        consumer: KafkaConsumer,
    ) -> None:
        """
        Polling loop cho main consumer (chạy trong thread riêng).

        Khi handle() fail, message sẽ được gửi vào DLQ topic.

        Args:
            handler (IEventHandler): Handler để xử lý messages
            consumer (KafkaConsumer): Consumer để poll
        """
        try:
            while self._running:
                records = consumer.poll(timeout_ms=self.MAIN_POLL_TIMEOUT_MS)
                if not records:
                    continue

                for topic_partition, messages in records.items():
                    for msg in messages:
                        try:
                            handler.handle(msg.value)
                        except Exception as e:
                            self._send_to_dlq(handler, msg.value, e)
                        finally:
                            consumer.commit()
        finally:
            consumer.close()

    def _run_dlq_consumer(
        self,
        handler: IDLQHandler,
        consumer: KafkaConsumer,
    ) -> None:
        """
        Polling loop cho DLQ consumer (chạy trong thread riêng).

        Poll interval dài hơn main consumer để tiết kiệm resource.

        Args:
            handler (IDLQHandler): Handler để xử lý DLQ messages
            consumer (KafkaConsumer): Consumer để poll DLQ topic
        """
        try:
            while self._running:
                records = consumer.poll(timeout_ms=self.DLQ_POLL_TIMEOUT_MS)
                if not records:
                    continue

                for topic_partition, messages in records.items():
                    for msg in messages:
                        try:
                            dlq_data = msg.value
                            # TODO: Validate dlq_data structure with DTO for type safety
                            original_message = dlq_data.get("original_message", {})
                            error_info = dlq_data.get("error_info", {})
                            handler.handle_dlq(original_message, error_info)
                        except Exception as e:
                            print(f"DLQ handler error: {e}")
                        finally:
                            consumer.commit()
        finally:
            consumer.close()

    def _send_to_dlq(
        self,
        handler: IEventHandler,
        original_message: dict,
        exception: Exception,
    ) -> None:
        """
        Gửi failed message vào DLQ topic.

        Args:
            handler (IEventHandler): Handler đã fail
            original_message (dict): Message gốc
            exception (Exception): Exception đã xảy ra
        """
        error_info = ErrorInfo.from_exception(
            exception=exception,
            original_topic=handler.topic.value,
            handler_name=handler.__class__.__name__,
        )
        dlq_message = DLQMessage(
            original_message=original_message,
            error_info=error_info,
        )

        # Handler phải implement IDLQHandler
        if isinstance(handler, IDLQHandler):
            self._dlq_publisher.publish(handler.dlq_topic, dlq_message)

    def _register_handler(self, handler: IEventHandler) -> None:
        """
        Tạo 2 consumers và 2 threads cho handler (main + DLQ).

        Args:
            handler (IEventHandler): Handler cần register
        """
        # Thread 1: Main consumer
        main_consumer = self._factory.create(handler)
        self._consumers.append(main_consumer)

        main_thread = threading.Thread(
            target=self._run_main_consumer,
            args=(handler, main_consumer),
            name=f"main-{handler.topic.value}",
            daemon=True,
        )
        self._threads.append(main_thread)

        # Thread 2: DLQ consumer (handler phải implement IDLQHandler)
        if isinstance(handler, IDLQHandler):
            dlq_consumer = self._factory.create_dlq(handler)
            self._consumers.append(dlq_consumer)

            dlq_thread = threading.Thread(
                target=self._run_dlq_consumer,
                args=(handler, dlq_consumer),
                name=f"dlq-{handler.dlq_topic.value}",
                daemon=True,
            )
            self._threads.append(dlq_thread)

    def register_all(self) -> None:
        """
        Start tất cả handlers trong các threads riêng biệt.

        Mỗi handler sẽ có 2 threads: main consumer và DLQ consumer.
        Method này sẽ block cho đến khi nhận signal shutdown.
        """
        self._running = True

        for handler in self._handlers.values():
            self._register_handler(handler)

        # Start all threads
        for thread in self._threads:
            thread.start()

        # Block main thread, chờ shutdown signal
        try:
            while self._running:
                threading.Event().wait(1)
        finally:
            self._shutdown()

    def _shutdown(self) -> None:
        """Graceful shutdown tất cả threads."""
        self._running = False
        for thread in self._threads:
            thread.join(timeout=5)
