"""
EventRegistry - Registry để quản lý và đăng ký event handlers với SocketIO Client

Trách nhiệm:
- Quản lý lifecycle của handlers (register, unregister, lookup)
- Đăng ký handlers với SocketIO AsyncClient
- Tạo wrapper để execute handlers
- Error handling và logging
"""
from abc import ABC, abstractmethod
from socketio import AsyncClient

from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.shared.enum.BaseEvent import SocketEvent, BaseEvents
from src.socketio_client.shared.enum.BaseNamespace import Namespace


class BaseEventRegistry(ABC):
    """
    Abstract Registry quản lý event handlers và đăng ký chúng với SocketIO Client.

    Subclass PHẢI implement `_create_handlers()` để định nghĩa handlers riêng.

    Usage:
        class ChatEventRegistry(BaseEventRegistry):
            def _create_handlers(self) -> list[IEventHandler]:
                return [
                    MessageReceivedHandler(),
                    ConnectHandler(),
                    DisconnectHandler(),
                ]

        sio = socketio.AsyncClient()
        registry = ChatEventRegistry(sio)
    """

    def __init__(self, sio: AsyncClient):
        """
        Initialize EventRegistry

        Args:
            sio: SocketIO AsyncClient instance
        """
        # Storage: key = (namespace, event_name), value = handler instance
        self._handlers: dict[tuple[str, str], IEventHandler] = {}
        self._sio = sio

        # Session ID từ server (được set bởi ConnectionConfirmedHandler)
        self.session_id: str | None = None

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
                    MessageReceivedHandler(),
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
        Đăng ký một handler với SocketIO client

        Args:
            handler: Handler instance
        """
        if self._sio is None:
            raise RuntimeError("SocketIO client chưa được attach")

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
        Tạo wrapper function để execute handler

        Args:
            handler: Handler instance

        Returns:
            Async wrapper function
        """

        async def wrapper(data: dict = {}):
            """
            Wrapper function nhận event từ SocketIO

            Args:
                data: Event data (optional)
            """
            try:
                print(
                    f"Handling {handler.event.value} "
                    f"in {handler.namespace.value}"
                )

                # Execute handler với session_id hiện tại
                result = await handler.handle(self._sio, self.session_id, data)

                # CHỈ update session_id nếu handler là CONNECTION_CONFIRMED
                if handler.event == BaseEvents.CONNECTION_CONFIRMED and result is not None:
                    self.session_id = result
                    print(f"[Registry] Session ID updated: {result}")

            except Exception as e:
                print(f"Error in handler {handler.__class__.__name__}: {e}")
                # Emit error event to server (optional)
                await self._sio.emit(
                    "client_error",
                    {"message": str(e), "event": handler.event.value},
                    namespace=handler.namespace.value,
                )
                raise

        return wrapper
