"""
EventRegistry - Registry để quản lý và đăng ký event handlers với SocketIO

Trách nhiệm:
- Quản lý lifecycle của handlers (register, unregister, lookup)
- Đăng ký handlers với SocketIO AsyncServer
- Tạo wrapper để execute handlers
- Error handling và logging
"""
from abc import ABC, abstractmethod
from socketio import AsyncServer

from src.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio_server.shared.enum.BaseEvent import SocketEvent
from src.socketio_server.shared.enum.BaseNamespace import Namespace
from src.socketio_server.main.enum.MainEvent import MainEvents


class BaseEventRegistry(ABC):
    """
    Abstract Registry quản lý event handlers và đăng ký chúng với SocketIO.

    Subclass PHẢI implement `_create_handlers()` để định nghĩa handlers riêng.

    Usage:
        class ChatEventRegistry(BaseEventRegistry):
            def _create_handlers(self) -> list[IEventHandler]:
                return [
                    MessageHandler(),
                    ConnectHandler(),
                    DisconnectHandler(),
                ]

        sio = socketio.AsyncServer()
        registry = ChatEventRegistry(sio)
    """

    def __init__(self, sio: AsyncServer):
        """
        Initialize EventRegistry

        Args:
            sio: SocketIO AsyncServer instance
        """
        # Storage: key = (namespace, event_name), value = handler instance
        self._handlers: dict[tuple[str, str], IEventHandler] = {}
        self._sio = sio

        # Get handlers from subclass implementation
        handlers = self._create_handlers()

        # Register all handlers
        for handler in handlers:
            self._register_handler(handler)

    @abstractmethod
    def _create_handlers(self) -> list[IEventHandler]:
        """
        Tạo và trả về danh sách các event handlers.

        Subclass PHẢI implement method này để định nghĩa
        các handlers riêng của domain.

        Returns:
            List of IEventHandler instances

        Example:
            def _create_handlers(self) -> list[IEventHandler]:
                return [
                    MessageHandler(),
                    ConnectHandler(),
                    DisconnectHandler(),
                ]
        """
        pass

    # ----------
    # Internal API: Registry Core
    # ----------

    def _register_handler(self, handler: IEventHandler) -> None:
        """
        Đăng ký một handler vào registry

        Args:
            handler: Instance của IEventHandler

        Raises:
            ValueError: Nếu handler invalid hoặc duplicate
        """
        # Create key
        key = (handler.namespace.value, handler.event.value)

        # Check duplicate
        if key in self._handlers:
            raise ValueError(
                f"Handler đã tồn tại cho event '{handler.event.value}' "
                f"trong namespace '{handler.namespace.value}'"
            )

        # Store handler
        self._handlers[key] = handler

        print(
            f"Registered handler: {handler.__class__.__name__} "
            f"for {handler.namespace.value}:{handler.event.value}"
        )

        # Nếu SocketIO đã attach, đăng ký handler luôn
        if self._sio is not None:
            self._register_with_socketio(handler)

    # ----------
    # Public API: Lookup
    # ----------

    def get_handler(self, namespace: Namespace, event: SocketEvent) -> IEventHandler:
        """
        Lấy handler đã đăng ký theo namespace và event

        Args:
            namespace: Namespace
            event: SocketEvent

        Returns:
            Handler instance

        Raises:
            KeyError: Nếu không tìm thấy handler
        """
        key = (namespace.value, event.value)
        handler = self._handlers.get(key)
        if handler is None:
            raise KeyError(
                f"Không tìm thấy handler cho event '{event.value}' "
                f"trong namespace '{namespace.value}'"
            )
        return handler

    def get_all_handlers(self) -> list[IEventHandler]:
        """
        Lấy tất cả handlers đã đăng ký

        Returns:
            List các handler instances
        """
        return list(self._handlers.values())

    # ----------
    # Internal: SocketIO Registration
    # ----------

    def _register_with_socketio(self, handler: IEventHandler) -> None:
        """
        Đăng ký một handler với SocketIO server

        Args:
            handler: Handler instance
        """
        if self._sio is None:
            raise RuntimeError("SocketIO server chưa được attach")

        # Tạo wrapper function
        wrapper = self._create_wrapper(handler)

        # Register với SocketIO
        if handler.namespace.value == "/":
            self._sio.on(handler.event.value)(wrapper)
        else:
            self._sio.on(handler.event.value, namespace=handler.namespace.value)(wrapper)

        print(f"Registered with SocketIO: {handler.namespace.value}:{handler.event.value}")

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
    