"""
SenderPairRequestHandler - Xử lý khi sender yêu cầu pair với receiver

Handler xử lý sự kiện sender-pair-request từ sender client.
"""
from socketio import AsyncServer

from src.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio_server.main.enum.MainNamespace import MainNamespaces
from src.socketio_server.main.enum.SenderStatus import SenderStatus
from src.socketio_server.main.enum.ReceiverStatus import ReceiverStatus
from src.socketio_server.main.manager.SenderManager import SenderManager
from src.socketio_server.main.manager.ReceiverManager import ReceiverManager
from src.socketio_server.main.manager.PairManager import PairManager


class SenderPairRequestHandler(IEventHandler):
    """
    Handler xử lý sự kiện sender-pair-request.

    Stateless handler - không lưu trữ state, lấy managers từ data.

    Logic:
    1. Thêm sender vào SenderManager
    2. Tìm receiver IDLE (chưa paired)
    3. Nếu có → tạo pair, emit pair-request-success
    4. Nếu không → emit pair-request-failed
    """

    event = MainEvents.SENDER_PAIR_REQUEST
    namespace = MainNamespaces.ROOT

    async def handle(self, sio: AsyncServer, client_sid: str | None, data=None):
        """
        Xử lý khi sender emit sender-pair-request event.

        Args:
            sio: SocketIO AsyncServer instance
            sid: Socket ID của sender
            data: Dict chứa:
                - session_id: ID của sender
                - __sender_manager__: SenderManager instance (injected by registry)
                - __receiver_manager__: ReceiverManager instance (injected by registry)
                - __pair_manager__: PairManager instance (injected by registry)

        Returns:
            None (fire-and-forget)
        """
        if not data:
            print(f"[SenderPairRequestHandler] No data received from {client_sid}")
            return

        # Lấy managers từ data (injected by registry)
        sender_manager: SenderManager = data.get("__sender_manager__")
        receiver_manager: ReceiverManager = data.get("__receiver_manager__")
        pair_manager: PairManager = data.get("__pair_manager__")

        if not sender_manager or not receiver_manager or not pair_manager:
            print(f"[SenderPairRequestHandler] Required managers not found in data")
            return

        # Lấy session_id từ data
        session_id = data.get("session_id")
        if not session_id:
            print(f"[SenderPairRequestHandler] session_id not found in data from {client_sid}")
            return

        # Thêm sender vào pool với status ACTIVE
        sender_manager.add_sender(session_id, SenderStatus.ACTIVE)
        print(
            f"[SenderPairRequestHandler] Sender {session_id} (client_sid: {client_sid}) is now ACTIVE"
        )

        # Tìm receiver IDLE (chưa được pair)
        idle_receivers = receiver_manager.get_receivers_by_status(ReceiverStatus.IDLE)

        # Lọc ra những receiver chưa được pair
        available_receiver_id = None
        for receiver_id in idle_receivers.keys():
            if not pair_manager.is_receiver_paired(receiver_id):
                available_receiver_id = receiver_id
                break

        if available_receiver_id:
            # Có receiver available → tạo pair
            pair_id = pair_manager.add_pair(session_id, available_receiver_id)

            # Update receiver status sang ACTIVE
            receiver_manager.update_status(available_receiver_id, ReceiverStatus.ACTIVE)

            print(
                f"[SenderPairRequestHandler] Successfully paired sender {session_id} "
                f"with receiver {available_receiver_id} (pair_id: {pair_id})"
            )

            # Emit success cho sender
            await sio.emit(
                MainEvents.PAIR_REQUEST_SUCCESS.value,
                {
                    "pair_id": str(pair_id),  # Convert UUID to string
                    "sender_id": session_id,
                    "receiver_id": available_receiver_id,
                },
                room=client_sid,
                namespace=self.namespace.value,
            )

            print(f"[SenderPairRequestHandler] Emitted pair-request-success to sender {session_id}")

            # Emit success cho receiver
            await sio.emit(
                MainEvents.PAIR_REQUEST_SUCCESS.value,
                {
                    "pair_id": str(pair_id),  # Convert UUID to string
                    "sender_id": session_id,
                    "receiver_id": available_receiver_id,
                },
                room=available_receiver_id,
                namespace=self.namespace.value,
            )

            print(f"[SenderPairRequestHandler] Emitted pair-request-success to receiver {available_receiver_id}")
        else:
            # Không có receiver available → fail
            print(
                f"[SenderPairRequestHandler] No available receiver for sender {session_id}"
            )

            # Emit failed cho sender
            await sio.emit(
                MainEvents.PAIR_REQUEST_FAILED.value,
                {
                    "sender_id": session_id,
                    "reason": "No available receiver",
                },
                room=client_sid,
                namespace=self.namespace.value,
            )

            print(f"[SenderPairRequestHandler] Emitted pair-request-failed to sender {session_id}")
