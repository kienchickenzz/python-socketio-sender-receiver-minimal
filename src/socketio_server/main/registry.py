"""
MainEventRegistry - Registry cho Main Server

Kế thừa từ BaseEventRegistry và implement _create_handlers()
để định nghĩa các handlers riêng cho main server.
"""
from socketio import AsyncServer

from src.socketio_server.shared.base.BaseEventRegistry import BaseEventRegistry
from src.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio_server.main.handler.ConnectHandler import ConnectHandler
from src.socketio_server.main.handler.DisconnectHandler import DisconnectHandler
from src.socketio_server.main.handler.ReceiverReadyHandler import ReceiverReadyHandler
from src.socketio_server.main.manager.ReceiverManager import ReceiverManager
from src.socketio_server.main.enum.MainEvent import MainEvents


class MainEventRegistry(BaseEventRegistry):
    """
    Registry cho Main Server, quản lý các event handlers.

    Kế thừa từ BaseEventRegistry và implement abstract method _create_handlers().
    Thêm ReceiverManager và inject vào data cho handlers.
    """

    def __init__(self, sio: AsyncServer):
        """
        Initialize MainEventRegistry với ReceiverManager.

        Args:
            sio: SocketIO AsyncServer instance
        """
        # Khởi tạo ReceiverManager (singleton)
        self._receiver_manager = ReceiverManager()
        print("[MainEventRegistry] ReceiverManager initialized")

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
        ]

    def _create_wrapper(self, handler: IEventHandler):
        """
        Tạo wrapper function để execute handler với ReceiverManager injected.

        Chỉ inject manager cho các handler cần thiết (DISCONNECT, RECEIVER_READY).

        Args:
            handler: Handler instance

        Returns:
            Async wrapper function
        """

        # Xác định các events cần inject ReceiverManager
        events_need_manager = {
            MainEvents.DISCONNECT,
            MainEvents.RECEIVER_READY,
        }

        # Check xem handler này có cần manager không
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
            Wrapper function nhận event từ SocketIO và inject ReceiverManager nếu cần.

            Args:
                sid: Socket ID của connection (được SocketIO truyền vào)
                data: Event data (optional, có thể là dict, string, hoặc None)
            """
            try:
                # Chỉ inject ReceiverManager cho handlers cần thiết
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

                    # Inject manager với key đặc biệt
                    data["__receiver_manager__"] = self._receiver_manager

                # Execute handler với đầy đủ parameters: (sio, sid, data)
                await handler.handle(self._sio, sid, data)

            except Exception as e:
                print(f"Error in handler {handler.__class__.__name__}: {e}")
                raise

        return wrapper
    