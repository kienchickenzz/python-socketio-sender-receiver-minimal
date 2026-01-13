"""
Pairing DTOs - Data Transfer Objects cho PAIRING FLOW

Module này chứa tất cả DTOs liên quan đến pairing giữa sender và receiver,
được sử dụng bởi cả server và clients (sender, receiver).

DTOs:
    - PairRequestSuccessDto: Server → Sender & Receiver (pairing thành công)
    - PairRequestFailedDto: Server → Sender (pairing thất bại)
"""
from src.shared.dto.DtoBase import DtoBase


class PairRequestSuccessDto(DtoBase):
    """
    DTO cho PAIR_REQUEST_SUCCESS event.

    Server emit event này cho cả sender và receiver khi pairing thành công.
    Server tìm được receiver IDLE chưa paired và tạo pair mới.

    Event flow:
        Sender requests pairing →
        Server finds available receiver →
        Server creates pair →
        Server emits PAIR_REQUEST_SUCCESS to both sender AND receiver

    Usage:
        Server (emit):
            dto = PairRequestSuccessDto(
                pair_id="uuid-string",
                sender_id="sender-123",
                receiver_id="receiver-456"
            )
            # Emit to sender
            await sio.emit("pair-request-success", dto.model_dump(by_alias=True), room=sender_sid)
            # Emit to receiver
            await sio.emit("pair-request-success", dto.model_dump(by_alias=True), room=receiver_id)
            # → {"pairId": "uuid-string", "senderId": "sender-123", "receiverId": "receiver-456"}

        Client (receive):
            dto = PairRequestSuccessDto(**data)
            print(f"Paired! Pair ID: {dto.pair_id}")

    Used by:
        - Server: SenderPairRequestHandler (emit to both)
        - Sender: PairRequestSuccessHandler (receive, start sending data)
        - Receiver: PairRequestSuccessHandler (receive, log info)

    Fields:
        pair_id (str): UUID của pair (đã convert sang string)
        sender_id (str): Session ID của sender
        receiver_id (str): Session ID của receiver
    """

    pair_id: str
    sender_id: str
    receiver_id: str


class PairRequestFailedDto(DtoBase):
    """
    DTO cho PAIR_REQUEST_FAILED event.

    Server emit event này cho sender khi không tìm được receiver available.
    Sender nhận được sẽ disconnect hoặc retry.

    Event flow:
        Sender requests pairing →
        Server finds NO available receiver →
        Server emits PAIR_REQUEST_FAILED to sender

    Reasons for failure:
        - "No available receiver": Không có receiver nào IDLE và chưa paired
        - "Receiver busy": Tất cả receivers đều đã được paired
        - Other custom reasons

    Usage:
        Server (emit):
            dto = PairRequestFailedDto(
                sender_id="sender-123",
                reason="No available receiver"
            )
            await sio.emit("pair-request-failed", dto.model_dump(by_alias=True), room=sender_sid)
            # → {"senderId": "sender-123", "reason": "No available receiver"}

        Client (receive):
            dto = PairRequestFailedDto(**data)
            print(f"Pairing failed for {dto.sender_id}: {dto.reason}")
            await sio.disconnect()

    Used by:
        - Server: SenderPairRequestHandler (emit when no receiver available)
        - Sender: PairRequestFailedHandler (receive, disconnect)

    Fields:
        sender_id (str): Session ID của sender
        reason (str): Lý do thất bại (human-readable message)
    """

    sender_id: str
    reason: str
