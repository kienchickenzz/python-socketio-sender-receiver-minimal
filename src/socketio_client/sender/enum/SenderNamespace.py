"""
ChatNamespaces - Namespaces riêng cho Chat Client

Kế thừa từ BaseNamespaces và thêm các namespaces cụ thể cho chat.
"""
from src.socketio_client.shared.enum.BaseNamespace import BaseNamespace, Namespace


class SenderNamespace(BaseNamespace):
    """
    Socket namespaces dành riêng cho Chat Client.

    Kế thừa ROOT namespace từ BaseNamespaces và thêm
    các namespaces riêng cho chat.
    """

    pass
