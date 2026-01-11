"""
ConnectionConfirmedHandler - Xử lý khi server xác nhận connection và gửi session ID

Handler này nhận session ID từ server và trả về để registry cập nhật.
"""
import asyncio
from socketio import AsyncClient
from socketio.exceptions import BadNamespaceError

from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.sender.enum.SenderEvent import SenderEvent
from src.socketio_client.sender.enum.SenderNamespace import SenderNamespace


class ConnectionConfirmedHandler(IEventHandler):
    """Handler xử lý CONNECTION_CONFIRMED event và lưu session ID"""

    event = SenderEvent.CONNECTION_CONFIRMED
    namespace = SenderNamespace.ROOT

    async def handle(self, sio: AsyncClient, session_id: str | None, data: dict = {}) -> str | None:
        """
        Xử lý khi nhận CONNECTION_CONFIRMED từ server

        Args:
            sio: SocketIO AsyncClient instance
            session_id: Session ID hiện tại (sẽ là None lần đầu)
            data: Data từ server chứa session ID mới

        Returns:
            str: Session ID mới từ server để registry cập nhật
        """
        # Lấy session ID từ data
        new_session_id: str | None = data.get("client_sid") if data else None

        if new_session_id:
            print(f"[Sender] Connection confirmed with session ID: {new_session_id}")

            # Emit sender-pair-request với retry logic
            # Lý do cần retry: tương tự receiver, namespace có thể chưa ready
            max_retries = 3
            retry_delay = 0.05  # 50ms delay giữa các lần retry

            for attempt in range(max_retries):
                try:
                    await sio.emit(
                        SenderEvent.SENDER_PAIR_REQUEST.value,
                        {"session_id": new_session_id},
                        namespace=SenderNamespace.ROOT.value,
                    )
                    print(
                        f"[Sender] Emitted sender-pair-request event with session_id: {new_session_id}"
                    )
                    # Emit thành công, thoát loop
                    break

                except BadNamespaceError as e:
                    if attempt < max_retries - 1:
                        # Còn lần retry
                        print(
                            f"[Sender] Namespace not ready (attempt {attempt + 1}/{max_retries}), "
                            f"retrying in {retry_delay}s..."
                        )
                        await asyncio.sleep(retry_delay)
                    else:
                        # Hết lần retry
                        print(
                            f"[Sender] Failed to emit sender-pair-request after {max_retries} attempts: {e}"
                        )
                        # Không raise exception để không block việc lưu session_id

            # Trả về session_id mới để wrapper cập nhật vào registry
            return new_session_id
        else:
            print(f"[Sender] CONNECTION_CONFIRMED received but no session ID in data")
            await sio.disconnect()
            return None
