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
    SENDER_PAIR_REQUEST = SocketEvent("sender-pair-request")

    PAIR_REQUEST_SUCCESS = SocketEvent("pair-request-success")
    PAIR_REQUEST_FAILED = SocketEvent("pair-request-failed")

    REQUEST_PROCESSING = SocketEvent("request-processing")

    WORKER_ACTIVE = SocketEvent("worker_active")
    
    WORKER_JOB = SocketEvent("worker-job")
    WORKER_RESULT = SocketEvent("worker-result")
    PROCESSING_RESULT = SocketEvent("processing-result")

    RECEIVER_DISCONNECTED = SocketEvent("receiver-disconnected")
    SENDER_DISCONNECTED = SocketEvent("sender-disconnected")