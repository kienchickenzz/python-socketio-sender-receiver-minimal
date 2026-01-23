"""
ChatEvents - Events riêng cho Chat Client

Kế thừa từ BaseEvents và thêm các events cụ thể cho chat client.
"""
from src.socketio.socketio_client.shared.enum.BaseEvent import BaseEvents, SocketEvent


class ReceiverEvent(BaseEvents):
    """
    Socket events dành riêng cho Chat Client.

    Kế thừa các base events (CONNECT, DISCONNECT, CONNECT_ERROR, ERROR)
    từ BaseEvents và thêm các events riêng cho chức năng chat client.
    """

    RECEIVER_READY = SocketEvent("receiver_ready")
    PAIR_REQUEST_SUCCESS = SocketEvent("pair-request-success")
    PROCESSING_RESULT = SocketEvent("processing-result")
    RECEIVER_DISCONNECTED = SocketEvent("receiver-disconnected")
    SENDER_DISCONNECTED = SocketEvent("sender-disconnected")
