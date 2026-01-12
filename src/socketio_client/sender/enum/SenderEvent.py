"""
ChatEvents - Events riêng cho Chat Client

Kế thừa từ BaseEvents và thêm các events cụ thể cho chat client.
"""
from src.socketio_client.shared.enum.BaseEvent import BaseEvents, SocketEvent


class SenderEvent(BaseEvents):
    """
    Socket events dành riêng cho Chat Client.

    Kế thừa các base events (CONNECT, DISCONNECT, CONNECT_ERROR, ERROR)
    từ BaseEvents và thêm các events riêng cho chức năng chat client.
    """

    SENDER_PAIR_REQUEST = SocketEvent("sender-pair-request")
    PAIR_REQUEST_SUCCESS = SocketEvent("pair-request-success")
    PAIR_REQUEST_FAILED = SocketEvent("pair-request-failed")
    REQUEST_PROCESSING = SocketEvent("request-processing")
    PROCESSING_ACKNOWLEDGED = SocketEvent("processing-acknowledged")
    