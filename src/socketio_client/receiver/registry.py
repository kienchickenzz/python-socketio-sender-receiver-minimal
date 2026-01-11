"""
ReceiverEventRegistry - Registry cho Receiver Client

Kế thừa từ BaseEventRegistry và implement _create_handlers()
để định nghĩa các handlers riêng cho receiver client.
"""
from src.socketio_client.shared.base.BaseEventRegistry import BaseEventRegistry
from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.receiver.handler.ConnectHandler import ConnectHandler
from src.socketio_client.receiver.handler.ConnectionConfirmedHandler import (
    ConnectionConfirmedHandler,
)
from src.socketio_client.receiver.handler.DisconnectHandler import DisconnectHandler


class ReceiverEventRegistry(BaseEventRegistry):
    """
    Registry cho Receiver Client, quản lý các event handlers.

    Kế thừa từ BaseEventRegistry và implement abstract method _create_handlers().
    """

    def _create_handlers(self) -> list[IEventHandler]:
        """
        Tạo và trả về danh sách các event handlers cho Receiver Client.

        Returns:
            List of receiver event handlers
        """
        return [
            ConnectHandler(),
            ConnectionConfirmedHandler(),
            DisconnectHandler(),
        ]
