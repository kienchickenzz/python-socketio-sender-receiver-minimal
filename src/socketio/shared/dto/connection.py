"""
Connection DTOs - Data Transfer Objects cho CONNECTION & REGISTRATION FLOW

Module này chứa tất cả DTOs liên quan đến connection và registration,
được sử dụng bởi cả server và clients (sender, receiver, worker).

DTOs:
    - ConnectionConfirmedDto: Server → Client (sau khi client connect)
    - ReceiverReadyDto: Receiver → Server (báo receiver sẵn sàng)
    - WorkerActiveDto: Worker → Server (báo worker active)
    - SenderPairRequestDto: Sender → Server (yêu cầu pair với receiver)
"""
from src.socketio.shared.dto.DtoBase import DtoBase


class ConnectionConfirmedDto(DtoBase):
    """
    DTO cho CONNECTION_CONFIRMED event.

    Server emit event này cho client sau khi client connect thành công.
    Client nhận được session_id (client_sid) để identify chính mình.

    Event flow:
        Client connects → Server emits CONNECTION_CONFIRMED → Client receives session_id

    Usage:
        Server (emit):
            dto = ConnectionConfirmedDto(client_sid="abc-123")
            await sio.emit("connection_confirmed", dto.model_dump(by_alias=True))
            # → {"clientSid": "abc-123"}

        Client (receive):
            dto = ConnectionConfirmedDto(**data)
            session_id = dto.client_sid

    Used by:
        - Server: BaseConnectHandler
        - Sender: ConnectionConfirmedHandler
        - Receiver: ConnectionConfirmedHandler
        - Worker: ConnectionConfirmedHandler
    """

    client_sid: str


class ReceiverReadyDto(DtoBase):
    """
    DTO cho RECEIVER_READY event.

    Receiver emit event này lên server sau khi nhận được CONNECTION_CONFIRMED.
    Server sẽ thêm receiver vào pool với status IDLE và sẵn sàng nhận pairing.

    Event flow:
        Receiver receives CONNECTION_CONFIRMED →
        Receiver emits RECEIVER_READY →
        Server adds receiver to pool as IDLE

    Usage:
        Client (emit):
            dto = ReceiverReadyDto(session_id="receiver-123")
            await sio.emit("receiver_ready", dto.model_dump(by_alias=True))
            # → {"sessionId": "receiver-123"}

        Server (receive):
            dto = ReceiverReadyDto(**data)
            receiver_id = dto.session_id

    Used by:
        - Receiver: ConnectionConfirmedHandler (emit)
        - Server: ReceiverReadyHandler (receive)
    """

    session_id: str


class WorkerActiveDto(DtoBase):
    """
    DTO cho WORKER_ACTIVE event.

    Worker emit event này lên server sau khi nhận được CONNECTION_CONFIRMED.
    Server sẽ thêm worker vào pool với status ACTIVE hoặc update status từ IDLE → ACTIVE.

    Event flow:
        Worker receives CONNECTION_CONFIRMED →
        Worker emits WORKER_ACTIVE →
        Server adds/updates worker in pool as ACTIVE

    Server logic:
        - Nếu worker chưa tồn tại → add mới với status ACTIVE
        - Nếu worker đã tồn tại với status IDLE → update sang ACTIVE
        - Nếu worker đã ACTIVE → không làm gì

    Usage:
        Client (emit):
            dto = WorkerActiveDto(session_id="worker-456")
            await sio.emit("worker_active", dto.model_dump(by_alias=True))
            # → {"sessionId": "worker-456"}

        Server (receive):
            dto = WorkerActiveDto(**data)
            worker_id = dto.session_id

    Used by:
        - Worker: ConnectionConfirmedHandler (emit)
        - Server: WorkerActiveHandler (receive)
    """

    session_id: str


class SenderPairRequestDto(DtoBase):
    """
    DTO cho SENDER_PAIR_REQUEST event.

    Sender emit event này lên server sau khi nhận được CONNECTION_CONFIRMED.
    Server sẽ thêm sender vào pool và tìm receiver IDLE để pair.

    Event flow:
        Sender receives CONNECTION_CONFIRMED →
        Sender emits SENDER_PAIR_REQUEST →
        Server finds available receiver and creates pair

    Usage:
        Client (emit):
            dto = SenderPairRequestDto(session_id="sender-789")
            await sio.emit("sender-pair-request", dto.model_dump(by_alias=True))
            # → {"sessionId": "sender-789"}

        Server (receive):
            dto = SenderPairRequestDto(**data)
            sender_id = dto.session_id

    Used by:
        - Sender: ConnectionConfirmedHandler (emit)
        - Server: SenderPairRequestHandler (receive)
    """

    session_id: str
