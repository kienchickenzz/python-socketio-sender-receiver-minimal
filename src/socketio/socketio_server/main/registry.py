"""
MainEventRegistry - Registry cho Main Server

Kế thừa từ BaseEventRegistry và implement _create_handlers()
để định nghĩa các handlers riêng cho main server.
"""
from socketio import AsyncServer

from src.socketio.socketio_server.shared.base.BaseEventRegistry import BaseEventRegistry
from src.socketio.socketio_server.shared.interface.IEventHandler import IEventHandler

from src.socketio.socketio_server.main.handler.ConnectHandler import ConnectHandler
# from src.socketio.socketio_server.main.handler.DisconnectHandler import DisconnectHandler
from src.socketio.socketio_server.main.handler.ReceiverReadyHandler import ReceiverReadyHandler
from src.socketio.socketio_server.main.handler.SenderPairRequestHandler import SenderPairRequestHandler
from src.socketio.socketio_server.main.handler.RequestProcessingHandler import RequestProcessingHandler
from src.socketio.socketio_server.main.handler.WorkerActiveHandler import WorkerActiveHandler
from src.socketio.socketio_server.main.handler.WorkerResultHandler import WorkerResultHandler
from src.socketio.socketio_server.main.handler.SenderDisconnectedHandler import SenderDisconnectedHandler
from src.socketio.socketio_server.main.manager.ReceiverManager import ReceiverManager
from src.socketio.socketio_server.main.manager.SenderManager import SenderManager
from src.socketio.socketio_server.main.manager.PairManager import PairManager
from src.socketio.socketio_server.main.manager.WorkerManager import WorkerManager
from src.socketio.socketio_server.main.manager.JobManager import JobManager

from src.socketio.socketio_server.main.enum.MainEvent import MainEvents

from src.kafka.producer.KafkaEventPublisher import KafkaEventPublisher


class MainEventRegistry(BaseEventRegistry):
    """
    Registry cho Main Server, quản lý các event handlers.

    Kế thừa từ BaseEventRegistry và implement abstract method _create_handlers().
    Nhận managers và event_publisher từ bên ngoài (dependency injection).
    """

    def __init__(
        self,
        sio: AsyncServer,
        # Managers
        receiver_manager: ReceiverManager,
        sender_manager: SenderManager,
        pair_manager: PairManager,
        worker_manager: WorkerManager,
        job_manager: JobManager,
        # Kafka Event Publisher
        event_publisher: KafkaEventPublisher,
    ):
        """
        Initialize MainEventRegistry với các dependencies được inject từ bên ngoài.

        Args:
            sio (AsyncServer): SocketIO AsyncServer instance
            receiver_manager (ReceiverManager): Manager quản lý receivers
            sender_manager (SenderManager): Manager quản lý senders
            pair_manager (PairManager): Manager quản lý pairs
            worker_manager (WorkerManager): Manager quản lý workers
            job_manager (JobManager): Manager quản lý jobs
            event_publisher (KafkaEventPublisher): Publisher để publish events lên Kafka
        """
        self._receiver_manager = receiver_manager
        self._sender_manager = sender_manager
        self._pair_manager = pair_manager
        self._worker_manager = worker_manager
        self._job_manager = job_manager
        self._event_publisher = event_publisher

        print("[MainEventRegistry] Dependencies injected successfully")

        # Gọi parent __init__ để đăng ký handlers
        super().__init__(sio)

    def _create_handlers(self) -> list[IEventHandler]:
        """
        Tạo và trả về danh sách các event handlers cho Main Server.

        Handlers cần Kafka publisher được inject qua constructor.

        Returns:
            list[IEventHandler]: Danh sách handler instances
        """
        return [
            ConnectHandler(),
            # DisconnectHandler(),
            ReceiverReadyHandler(event_publisher=self._event_publisher),
            SenderDisconnectedHandler(event_publisher=self._event_publisher),
            SenderPairRequestHandler(),
            RequestProcessingHandler(),
            WorkerActiveHandler(event_publisher=self._event_publisher),
            WorkerResultHandler(),
        ]

    def _create_wrapper(self, handler: IEventHandler):
        """
        Tạo wrapper function để execute handler với managers injected có điều kiện.

        Dependency injection có điều kiện:
        - ReceiverManager: DISCONNECT, RECEIVER_READY, SENDER_PAIR_REQUEST
        - SenderManager: DISCONNECT, SENDER_PAIR_REQUEST
        - PairManager: SENDER_PAIR_REQUEST
        - WorkerManager: REQUEST_PROCESSING, WORKER_ACTIVE, WORKER_RESULT
        - JobManager: REQUEST_PROCESSING, WORKER_RESULT

        Args:
            handler: Handler instance

        Returns:
            Async wrapper function
        """

        # Xác định các events cần inject managers
        # Lưu ý: WORKER_ACTIVE không cần manager vì đã chuyển logic sang Kafka consumer
        events_need_manager = {
            MainEvents.DISCONNECT,
            MainEvents.RECEIVER_READY,
            MainEvents.SENDER_PAIR_REQUEST,
            MainEvents.REQUEST_PROCESSING,
            MainEvents.WORKER_RESULT,
        }

        # Check xem handler này có cần managers không
        needs_manager = handler.event in events_need_manager


        # Wrapper function PHẢI nhận đúng signature: async def wrapper(sid: str, data=None)
        # Lý do:
        # - SocketIO luôn truyền 2 parameters khi trigger event: (sid, data)
        # - sid (str): Socket ID của connection
        # - data: Event data có thể là dict, string, hoặc None tùy event:
        #   + CONNECT event: data là dict hoặc None
        #   + DISCONNECT event: data là string (disconnect reason như "client namespace disconnect")
        #   + Custom events (receiver_ready): data là dict từ client emit
        # - KHÔNG dùng mutable default argument (data: dict = {}) vì đây là anti-pattern trong Python
        async def wrapper(sid: str, data=None):
            """
            Wrapper function nhận event từ SocketIO và inject managers có điều kiện.

            Args:
                sid: Socket ID của connection (được SocketIO truyền vào)
                data: Event data (optional, có thể là dict, string, hoặc None)
            """
            try:
                # Chỉ inject managers cho handlers cần thiết
                if needs_manager:
                    # PHẢI kiểm tra type của data trước khi thực hiện dict assignment
                    # Lý do:
                    # - data có thể KHÔNG phải là dict
                    # - Ví dụ: với DISCONNECT event, data là string (disconnect reason)
                    # - Nếu không check type mà cố gắng: data["key"] = value
                    #   sẽ bị lỗi: TypeError: 'str' object does not support item assignment

                    if data is None:
                        # Data là None -> tạo dict mới
                        data = {}
                    elif not isinstance(data, dict):
                        # Data không phải dict (ví dụ: string từ disconnect)
                        # Wrap nó vào dict để giữ lại data gốc
                        data = {"_original_data": data}

                    # Inject managers có điều kiện dựa trên event type
                    # Chỉ inject manager nào cần thiết cho event cụ thể

                    # ReceiverManager: cần cho DISCONNECT, RECEIVER_READY, SENDER_PAIR_REQUEST
                    if handler.event in {
                        MainEvents.DISCONNECT,
                        MainEvents.RECEIVER_READY,
                        MainEvents.SENDER_PAIR_REQUEST,
                    }:
                        data["__receiver_manager__"] = self._receiver_manager

                    # SenderManager: cần cho DISCONNECT, SENDER_PAIR_REQUEST
                    if handler.event in {
                        MainEvents.DISCONNECT,
                        MainEvents.SENDER_PAIR_REQUEST,
                    }:
                        data["__sender_manager__"] = self._sender_manager

                    # PairManager: chỉ cần cho SENDER_PAIR_REQUEST
                    if handler.event == MainEvents.SENDER_PAIR_REQUEST:
                        data["__pair_manager__"] = self._pair_manager

                    # WorkerManager: cần cho REQUEST_PROCESSING, WORKER_RESULT
                    # Lưu ý: WORKER_ACTIVE không cần vì đã chuyển logic sang Kafka consumer
                    if handler.event in {
                        MainEvents.REQUEST_PROCESSING,
                        MainEvents.WORKER_RESULT,
                    }:
                        data["__worker_manager__"] = self._worker_manager

                    # JobManager: cần cho REQUEST_PROCESSING, WORKER_RESULT
                    if handler.event in {
                        MainEvents.REQUEST_PROCESSING,
                        MainEvents.WORKER_RESULT,
                    }:
                        data["__job_manager__"] = self._job_manager

                # Execute handler với đầy đủ parameters: (sio, sid, data)
                await handler.handle(self._sio, sid, data)

            except Exception as e:
                print(f"Error in handler {handler.__class__.__name__}: {e}")
                raise

        return wrapper
    