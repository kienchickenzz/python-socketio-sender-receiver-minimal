"""
WorkerEventRegistry - Registry cho Worker Client

Kế thừa từ BaseEventRegistry và implement _create_handlers()
để định nghĩa các handlers riêng cho worker client.
"""
from src.socketio_client.shared.base.BaseEventRegistry import BaseEventRegistry
from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.worker.handler.ConnectHandler import ConnectHandler
from src.socketio_client.worker.handler.ConnectionConfirmedHandler import (
    ConnectionConfirmedHandler,
)
from src.socketio_client.worker.handler.DisconnectHandler import DisconnectHandler
from src.socketio_client.worker.handler.WorkerJobHandler import WorkerJobHandler


class WorkerEventRegistry(BaseEventRegistry):
    """
    Registry cho Worker Client, quản lý các event handlers.

    Kế thừa từ BaseEventRegistry và implement abstract method _create_handlers().
    """

    def _create_handlers(self) -> list[IEventHandler]:
        """
        Tạo và trả về danh sách các event handlers cho Worker Client.

        Returns:
            List of worker event handlers
        """
        return [
            ConnectHandler(),
            ConnectionConfirmedHandler(),
            DisconnectHandler(),
            WorkerJobHandler(),
        ]
