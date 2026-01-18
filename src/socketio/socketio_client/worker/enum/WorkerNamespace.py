"""
WorkerNamespace - Namespaces riêng cho Worker Client

Kế thừa từ BaseNamespaces và thêm các namespaces cụ thể cho worker.
"""
from src.socketio.socketio_client.shared.enum.BaseNamespace import BaseNamespace, Namespace


class WorkerNamespace(BaseNamespace):
    """
    Socket namespaces dành riêng cho Worker Client.

    Kế thừa ROOT namespace từ BaseNamespaces và thêm
    các namespaces riêng cho worker.
    """

    pass
