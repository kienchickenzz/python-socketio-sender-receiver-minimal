"""
ChatNamespaces - Namespaces riêng cho Chat Server

Kế thừa từ BaseNamespaces và thêm các namespaces cụ thể cho chat.
"""
from src.socketio_server.shared.enum.BaseNamespace import BaseNamespaces, Namespace


class MainNamespaces(BaseNamespaces):
    """
    Socket namespaces dành riêng cho Chat Server.

    Kế thừa ROOT namespace từ BaseNamespaces và thêm
    các namespaces riêng cho chat.
    """

    pass
