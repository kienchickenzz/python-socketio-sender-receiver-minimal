"""
BaseEmitRegistry - Registry pattern cho Kafka emit consumer handlers.

Trách nhiệm:
- Quản lý danh sách emit handlers
- Dùng factory để tạo consumer cho từng handler (main + DLQ)
- Mỗi handler chạy 2 threads: main consumer và DLQ consumer
- Truyền AsyncServer cho handlers để emit SocketIO events
- Graceful shutdown tất cả threads

Registry này được thiết kế để nhúng vào SocketIO server (embedded mode),
không chạy standalone vì cần sio instance và event_loop từ server.
"""
import asyncio
import threading
from abc import ABC, abstractmethod

from kafka import KafkaConsumer
from socketio import AsyncServer

from src.socketio.socketio_server.shared.kafka_consumer.infrastructure.KafkaConsumerFactory import KafkaConsumerFactory
from src.socketio.socketio_server.shared.kafka_consumer.infrastructure.DeadLetterPublisher import DeadLetterPublisher
from src.socketio.socketio_server.shared.kafka_consumer.interface.IEmitHandler import IEmitHandler
from src.socketio.socketio_server.shared.kafka_consumer.interface.IDLQHandler import IDLQHandler
from src.socketio.socketio_server.shared.kafka_consumer.model.DLQMessage import DLQMessage, ErrorInfo


class BaseEmitRegistry(ABC):
    """
    Base Registry cho Kafka emit handlers.

    Mỗi handler sẽ có 2 consumers:
    - Main consumer: xử lý emit logic
    - DLQ consumer: xử lý messages fail

    Handlers nhận AsyncServer để emit SocketIO events.
    Event loop được truyền vào để chạy async handlers từ synchronous threads.

    Subclass phải implement _create_handlers() để định nghĩa handlers.

    Example:
        # Trong FastAPI lifespan
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            emit_registry = MainEmitRegistry(...)
            emit_registry.start()
            yield
            emit_registry.stop()
    """

    # Poll timeout cho main consumer (ms)
    MAIN_POLL_TIMEOUT_MS = 1000

    # Poll timeout cho DLQ consumer - dài hơn để tiết kiệm resource (ms)
    DLQ_POLL_TIMEOUT_MS = 5000

    def __init__(
        self,
        consumer_factory: KafkaConsumerFactory,
        dlq_publisher: DeadLetterPublisher,
        sio: AsyncServer,
        event_loop: asyncio.AbstractEventLoop,
    ) -> None:
        """
        Khởi tạo registry.

        Args:
            consumer_factory (KafkaConsumerFactory): Factory để tạo consumers
            dlq_publisher (DeadLetterPublisher): Publisher để gửi failed messages vào DLQ
            sio (AsyncServer): SocketIO AsyncServer instance để emit events
            event_loop (asyncio.AbstractEventLoop): Event loop để chạy async handlers
        """
        self._factory = consumer_factory
        self._dlq_publisher = dlq_publisher
        self._sio = sio
        self._event_loop = event_loop
        self._handlers: dict[str, IEmitHandler] = {}
        self._consumers: list[KafkaConsumer] = []
        self._threads: list[threading.Thread] = []
        self._running = False

        # Get handlers from subclass
        handlers = self._create_handlers()
        for handler in handlers:
            self._handlers[handler.topic.value] = handler

    @abstractmethod
    def _create_handlers(self) -> list[IEmitHandler]:
        """
        Tạo và trả về danh sách handlers.

        Subclass PHẢI implement method này.
        Mỗi handler PHẢI implement cả IEmitHandler và IDLQHandler.

        Returns:
            list[IEmitHandler]: Danh sách handler instances
        """
        pass

    def _run_main_consumer(
        self,
        handler: IEmitHandler,
        consumer: KafkaConsumer,
    ) -> None:
        """
        Polling loop cho main consumer (chạy trong thread riêng).

        Khi handle() fail, message sẽ được gửi vào DLQ topic.
        Handler.handle() là async function, được chạy qua event_loop.

        Args:
            handler (IEmitHandler): Handler để xử lý messages
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
                            # Run async handler from synchronous thread context
                            future = asyncio.run_coroutine_threadsafe(
                                handler.handle(self._sio, msg.value),
                                self._event_loop,
                            )
                            future.result()
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
        handler: IEmitHandler,
        original_message: dict,
        exception: Exception,
    ) -> None:
        """
        Gửi failed message vào DLQ topic.

        Args:
            handler (IEmitHandler): Handler đã fail
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

        if isinstance(handler, IDLQHandler):
            self._dlq_publisher.publish(handler.dlq_topic, dlq_message)

    def _register_handler(self, handler: IEmitHandler) -> None:
        """
        Tạo 2 consumers và 2 threads cho handler (main + DLQ).

        Args:
            handler (IEmitHandler): Handler cần register
        """
        # Thread 1: Main consumer
        main_consumer = self._factory.create(handler)
        self._consumers.append(main_consumer)

        main_thread = threading.Thread(
            target=self._run_main_consumer,
            args=(handler, main_consumer),
            name=f"emit-main-{handler.topic.value}",
            daemon=True,
        )
        self._threads.append(main_thread)

        # Thread 2: DLQ consumer
        if isinstance(handler, IDLQHandler):
            dlq_consumer = self._factory.create_dlq(handler)
            self._consumers.append(dlq_consumer)

            dlq_thread = threading.Thread(
                target=self._run_dlq_consumer,
                args=(handler, dlq_consumer),
                name=f"emit-dlq-{handler.dlq_topic.value}",
                daemon=True,
            )
            self._threads.append(dlq_thread)

    def start(self) -> None:
        """
        Start tất cả emit consumer threads.

        Non-blocking - threads chạy background, method return ngay.
        Gọi method này trong FastAPI startup event.
        """
        self._running = True

        for handler in self._handlers.values():
            self._register_handler(handler)

        for thread in self._threads:
            thread.start()

        print(f"[BaseEmitRegistry] Started {len(self._threads)} consumer threads")

    def stop(self) -> None:
        """
        Graceful shutdown tất cả threads.

        Gọi method này trong FastAPI shutdown event.
        """
        print("[BaseEmitRegistry] Stopping...")
        self._running = False

        for thread in self._threads:
            thread.join(timeout=5)

        print("[BaseEmitRegistry] Stopped")
