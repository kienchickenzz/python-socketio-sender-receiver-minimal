"""
MainEventRegistry - Registry cho Main Server

Kế thừa từ BaseEventRegistry và implement _create_handlers()
để định nghĩa các handlers riêng cho main server.
"""
from socketio import AsyncServer

from src.socketio.socketio_server.shared.base.BaseEventRegistry import BaseEventRegistry
from src.socketio.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio.socketio_server.main.handler.ConnectHandler import ConnectHandler
from src.socketio.socketio_server.main.handler.DisconnectHandler import DisconnectHandler
from src.socketio.socketio_server.main.handler.ReceiverReadyHandler import ReceiverReadyHandler
from src.socketio.socketio_server.main.handler.SenderPairRequestHandler import SenderPairRequestHandler
from src.socketio.socketio_server.main.handler.RequestProcessingHandler import RequestProcessingHandler
from src.socketio.socketio_server.main.handler.WorkerActiveHandler import WorkerActiveHandler
from src.socketio.socketio_server.main.handler.WorkerResultHandler import WorkerResultHandler
from src.socketio.socketio_server.main.manager.ReceiverManager import ReceiverManager
from src.socketio.socketio_server.main.manager.SenderManager import SenderManager
from src.socketio.socketio_server.main.manager.PairManager import PairManager
from src.socketio.socketio_server.main.manager.WorkerManager import WorkerManager
from src.socketio.socketio_server.main.manager.JobManager import JobManager
from src.socketio.socketio_server.main.enum.MainEvent import MainEvents


class MainEventRegistry(BaseEventRegistry):
    """
    Registry cho Main Server, quản lý các event handlers.

    Kế thừa từ BaseEventRegistry và implement abstract method _create_handlers().
    Quản lý ReceiverManager, SenderManager, PairManager, WorkerManager và inject
    có điều kiện vào handlers dựa trên event type.
    """

    def __init__(self, sio: AsyncServer):
        """
        Initialize MainEventRegistry với ReceiverManager, SenderManager, PairManager, WorkerManager và JobManager.

        Args:
            sio: SocketIO AsyncServer instance
        """
        # Khởi tạo ReceiverManager (singleton)
        self._receiver_manager = ReceiverManager()
        print("[MainEventRegistry] ReceiverManager initialized")

        # Khởi tạo SenderManager (singleton)
        self._sender_manager = SenderManager()
        print("[MainEventRegistry] SenderManager initialized")

        # Khởi tạo PairManager (singleton)
        self._pair_manager = PairManager()
        print("[MainEventRegistry] PairManager initialized")

        # Khởi tạo WorkerManager (singleton)
        self._worker_manager = WorkerManager()
        print("[MainEventRegistry] WorkerManager initialized")

        # Khởi tạo JobManager (singleton)
        self._job_manager = JobManager()
        print("[MainEventRegistry] JobManager initialized")

        # Gọi parent __init__ để đăng ký handlers
        super().__init__(sio)

    def _create_handlers(self) -> list[IEventHandler]:
        """
        Tạo và trả về danh sách các event handlers cho Main Server.

        Returns:
            List of main server event handlers
        """
        return [
            ConnectHandler(),
            DisconnectHandler(),
            ReceiverReadyHandler(),
            SenderPairRequestHandler(),
            RequestProcessingHandler(),
            WorkerActiveHandler(),
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
        events_need_manager = {
            MainEvents.DISCONNECT,
            MainEvents.RECEIVER_READY,
            MainEvents.SENDER_PAIR_REQUEST,
            MainEvents.WORKER_ACTIVE,
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

                    # WorkerManager: cần cho REQUEST_PROCESSING, WORKER_ACTIVE, WORKER_RESULT
                    if handler.event in {
                        MainEvents.REQUEST_PROCESSING,
                        MainEvents.WORKER_ACTIVE,
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
    