"""
SenderDisconnectedHandler - Xử lý khi sender ngắt kết nối

Handler xử lý sự kiện sender-disconnected từ server khi sender disconnect.
"""
from socketio import AsyncServer

from src.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio_server.main.enum.MainNamespace import MainNamespaces
from src.socketio_server.main.manager.SenderManager import SenderManager
from src.socketio_server.main.manager.PairManager import PairManager


class SenderDisconnectedHandler(IEventHandler):
    """
    Handler xử lý sự kiện sender-disconnected.

    Stateless handler - xử lý khi sender disconnect khỏi server.
    Logic sẽ bao gồm:
    - Xóa sender khỏi SenderManager
    - Xử lý pair nếu sender đang trong pair
    - Thông báo receiver nếu cần
    """

    event = MainEvents.SENDER_DISCONNECTED
    namespace = MainNamespaces.ROOT

    async def handle(self, sio: AsyncServer, client_sid: str | None, data=None):
        """
        Xử lý khi sender disconnect.

        Args:
            sio: SocketIO AsyncServer instance
            client_sid: Socket ID của sender đã disconnect
            data: Dict chứa:
                - __sender_manager__: SenderManager instance (injected by registry)
                - __pair_manager__: PairManager instance (injected by registry)

        Returns:
            None (fire-and-forget)
        """
        if not client_sid:
            print(f"[SenderDisconnectedHandler] No client_sid provided")
            return

        print(f"\n{'='*60}")
        print(f"[SenderDisconnectedHandler] 🔴 SENDER DISCONNECTED")
        print(f"[SenderDisconnectedHandler] Client SID: {client_sid}")
        print(f"{'='*60}\n")

        # TODO: Implement disconnect logic
        # 1. Get sender_manager and pair_manager from data
        # 2. Find sender by client_sid
        # 3. Check if sender is in a pair
        # 4. If paired, handle pair cleanup and notify receiver
        # 5. Remove sender from SenderManager
        # 6. Log final state

        print(f"[SenderDisconnectedHandler] ⚠️ Handler logic not yet implemented")
        print(f"{'='*60}\n")
