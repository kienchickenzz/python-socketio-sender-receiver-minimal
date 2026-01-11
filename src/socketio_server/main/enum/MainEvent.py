"""
ChatEvents - Events riêng cho Chat Server

Kế thừa từ BaseEvents và thêm các events cụ thể cho chat.
"""
from src.socketio_server.shared.enum.BaseEvent import BaseEvents, SocketEvent


class MainEvents(BaseEvents):
    """
    Socket events dành riêng cho Chat Server.

    Kế thừa các base events (CONNECT, DISCONNECT, ERROR) từ BaseEvents
    và thêm các events riêng cho chức năng chat.
    """

    RECEIVER_READY = SocketEvent("receiver_ready")