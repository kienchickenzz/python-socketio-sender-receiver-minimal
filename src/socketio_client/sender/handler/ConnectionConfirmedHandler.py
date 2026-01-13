"""
ConnectionConfirmedHandler - Xử lý khi server xác nhận connection và gửi session ID

Handler này nhận session ID từ server và trả về để registry cập nhật.
"""
import asyncio
from socketio import AsyncClient
from socketio.exceptions import BadNamespaceError
from pydantic import ValidationError

from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.sender.enum.SenderEvent import SenderEvent
from src.socketio_client.sender.enum.SenderNamespace import SenderNamespace
from src.shared.dto.connection import ConnectionConfirmedDto, SenderPairRequestDto


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
        # Deserialize data thành DTO
        try:
            dto = ConnectionConfirmedDto(**data) if data else None
        except ValidationError as e:
            print(f"[Sender] Invalid CONNECTION_CONFIRMED data: {e}")
            await sio.disconnect()
            return None

        if dto:
            print(f"[Sender] Connection confirmed with session ID: {dto.client_sid}")

            # Tạo DTO và serialize để emit
            pair_request_dto = SenderPairRequestDto(session_id=dto.client_sid)
            payload = pair_request_dto.model_dump(by_alias=True)

            # Emit sender-pair-request với retry logic
            # Lý do cần retry: tương tự receiver, namespace có thể chưa ready
            max_retries = 3
            retry_delay = 0.05  # 50ms delay giữa các lần retry

            for attempt in range(max_retries):
                try:
                    await sio.emit(
                        SenderEvent.SENDER_PAIR_REQUEST.value,
                        payload,
                        namespace=SenderNamespace.ROOT.value,
                    )
                    print(
                        f"[Sender] Emitted sender-pair-request event with session_id: {dto.client_sid}"
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
            return dto.client_sid
        else:
            print(f"[Sender] CONNECTION_CONFIRMED received but no session ID in data")
            await sio.disconnect()
            return None
