"""
SenderEventRegistry - Registry cho Sender Client

Kế thừa từ BaseEventRegistry và implement _create_handlers()
để định nghĩa các handlers riêng cho sender client.
"""
from src.socketio_client.shared.base.BaseEventRegistry import BaseEventRegistry
from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.sender.handler.ConnectHandler import ConnectHandler
from src.socketio_client.sender.handler.ConnectionConfirmedHandler import (
    ConnectionConfirmedHandler,
)
from src.socketio_client.sender.handler.DisconnectHandler import DisconnectHandler


class SenderEventRegistry(BaseEventRegistry):
    """
    Registry cho Sender Client, quản lý các event handlers.

    Kế thừa từ BaseEventRegistry và implement abstract method _create_handlers().
    """

    def _create_handlers(self) -> list[IEventHandler]:
        """
        Tạo và trả về danh sách các event handlers cho Sender Client.

        Returns:
            List of sender event handlers
        """
        return [
            ConnectHandler(),
            ConnectionConfirmedHandler(),
            DisconnectHandler(),
        ]
