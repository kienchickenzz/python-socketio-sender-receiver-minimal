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

        Override để inject ReceiverManager vào data trước khi gọi handler.
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

        async def wrapper(sid: str, data: dict = {}):
            """
            Wrapper function nhận event từ SocketIO và inject ReceiverManager nếu cần.

            Args:
                sid: Socket ID
                data: Event data (optional)
            """
            try:
                # Chỉ inject ReceiverManager cho handlers cần thiết
                if needs_manager:

                    # Inject manager với key đặc biệt
                    data["__receiver_manager__"] = self._receiver_manager

                # Execute handler
                await handler.handle(self._sio, sid, data)

            except Exception as e:
                print(f"Error in handler {handler.__class__.__name__}: {e}")
                raise

        return wrapper
    