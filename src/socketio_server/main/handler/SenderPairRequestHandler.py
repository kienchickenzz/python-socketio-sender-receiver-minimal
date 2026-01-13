"""
SenderPairRequestHandler - Xử lý khi sender yêu cầu pair với receiver

Handler xử lý sự kiện sender-pair-request từ sender client.
"""
from socketio import AsyncServer
from pydantic import ValidationError

from src.socketio_server.shared.interface.IEventHandler import IEventHandler
from src.socketio_server.main.enum.MainEvent import MainEvents
from src.socketio_server.main.enum.MainNamespace import MainNamespaces
from src.socketio_server.main.enum.SenderStatus import SenderStatus
from src.socketio_server.main.enum.ReceiverStatus import ReceiverStatus
from src.socketio_server.main.manager.SenderManager import SenderManager
from src.socketio_server.main.manager.ReceiverManager import ReceiverManager
from src.socketio_server.main.manager.PairManager import PairManager
from src.shared.dto.connection import SenderPairRequestDto
from src.shared.dto.pairing import PairRequestSuccessDto, PairRequestFailedDto


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
                - dto.session_id: ID của sender
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

        # Deserialize data thành DTO
        try:
            dto = SenderPairRequestDto(**data)
        except ValidationError as e:
            print(f"[SenderPairRequestHandler] Invalid data format from {client_sid}: {e}")
            return

        # Thêm sender vào pool với status ACTIVE
        sender_manager.add_sender(dto.session_id, SenderStatus.ACTIVE)
        print(
            f"[SenderPairRequestHandler] Sender {dto.session_id} (client_sid: {client_sid}) is now ACTIVE"
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
            pair_id = pair_manager.add_pair(dto.session_id, available_receiver_id)

            # Update receiver status sang ACTIVE
            receiver_manager.update_status(available_receiver_id, ReceiverStatus.ACTIVE)

            print(
                f"[SenderPairRequestHandler] Successfully paired sender {dto.session_id} "
                f"with receiver {available_receiver_id} (pair_id: {pair_id})"
            )

            # Tạo DTO và serialize
            success_dto = PairRequestSuccessDto(
                pair_id=str(pair_id),  # Convert UUID to string
                sender_id=dto.session_id,
                receiver_id=available_receiver_id,
            )
            payload = success_dto.model_dump(by_alias=True)

            # Emit success cho sender
            await sio.emit(
                MainEvents.PAIR_REQUEST_SUCCESS.value,
                payload,
                room=client_sid,
                namespace=self.namespace.value,
            )

            print(f"[SenderPairRequestHandler] Emitted pair-request-success to sender {dto.session_id}")

            # Emit success cho receiver
            await sio.emit(
                MainEvents.PAIR_REQUEST_SUCCESS.value,
                payload,
                room=available_receiver_id,
                namespace=self.namespace.value,
            )

            print(f"[SenderPairRequestHandler] Emitted pair-request-success to receiver {available_receiver_id}")
        else:
            # Không có receiver available → fail
            print(
                f"[SenderPairRequestHandler] No available receiver for sender {dto.session_id}"
            )

            # Tạo DTO và serialize
            failed_dto = PairRequestFailedDto(
                sender_id=dto.session_id,
                reason="No available receiver",
            )
            payload = failed_dto.model_dump(by_alias=True)

            # Emit failed cho sender
            await sio.emit(
                MainEvents.PAIR_REQUEST_FAILED.value,
                payload,
                room=client_sid,
                namespace=self.namespace.value,
            )

            print(f"[SenderPairRequestHandler] Emitted pair-request-failed to sender {dto.session_id}")
