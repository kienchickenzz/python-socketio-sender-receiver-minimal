"""
WorkerEvent - Events riêng cho Worker Client

Kế thừa từ BaseEvents và thêm các events cụ thể cho worker client.
"""
from src.socketio.socketio_client.shared.enum.BaseEvent import BaseEvents, SocketEvent


class WorkerEvent(BaseEvents):
    """
    Socket events dành riêng cho Worker Client.

    Kế thừa các base events (CONNECT, DISCONNECT, CONNECT_ERROR, ERROR)
    từ BaseEvents và thêm các events riêng cho worker client.
    """

    WORKER_ACTIVE = SocketEvent("worker_active")
    WORKER_JOB = SocketEvent("worker-job")
    WORKER_RESULT = SocketEvent("worker-result")
