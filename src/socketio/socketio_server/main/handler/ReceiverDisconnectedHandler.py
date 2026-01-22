"""
ReceiverDisconnectedHandler - Xử lý khi receiver ngắt kết nối

Handler xử lý sự kiện receiver-disconnected từ server khi receiver disconnect.
"""
from socketio import AsyncServer

from src.socketio.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio.socketio_server.main.enum.MainNamespace import MainNamespaces


class ReceiverDisconnectedHandler(IEventHandler):
    """
    Handler xử lý sự kiện receiver-disconnected.

    Stateless handler - xử lý khi receiver disconnect khỏi server.
    Logic sẽ bao gồm:
    - Xóa receiver khỏi ReceiverManager
    - Xử lý pair nếu receiver đang trong pair
    - Thông báo sender nếu cần
    """

    event = MainEvents.RECEIVER_DISCONNECTED
    namespace = MainNamespaces.ROOT

    async def handle(self, sio: AsyncServer, client_sid: str | None, data=None):
        """
        Xử lý khi receiver disconnect.

        Args:
            sio: SocketIO AsyncServer instance
            client_sid: Socket ID của receiver đã disconnect
            data: Dict chứa:
                - __receiver_manager__: ReceiverManager instance (injected by registry)
                - __pair_manager__: PairManager instance (injected by registry)

        Returns:
            None (fire-and-forget)
        """
        if not client_sid:
            print(f"[ReceiverDisconnectedHandler] No client_sid provided")
            return

        print(f"\n{'='*60}")
        print(f"[ReceiverDisconnectedHandler] 🔴 RECEIVER DISCONNECTED")
        print(f"[ReceiverDisconnectedHandler] Client SID: {client_sid}")
        print(f"{'='*60}\n")

        # TODO: Implement disconnect logic
        # 1. Get receiver_manager and pair_manager from data
        # 2. Find receiver by client_sid
        # 3. Check if receiver is in a pair
        # 4. If paired, handle pair cleanup and notify sender
        # 5. Remove receiver from ReceiverManager
        # 6. Log final state

        print(f"[ReceiverDisconnectedHandler] ⚠️ Handler logic not yet implemented")
        print(f"{'='*60}\n")
