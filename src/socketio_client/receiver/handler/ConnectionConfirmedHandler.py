"""
ConnectionConfirmedHandler - Xử lý khi server xác nhận connection và gửi session ID

Handler này nhận session ID từ server và trả về để registry cập nhật.
"""
import asyncio
from socketio import AsyncClient
from socketio.exceptions import BadNamespaceError
from pydantic import ValidationError

from src.socketio_client.shared.interface.IEventHandler import IEventHandler
from src.socketio_client.receiver.enum.ReceiverEvent import ReceiverEvent
from src.socketio_client.receiver.enum.ReceiverNamespace import ReceiverNamespace
from src.shared.dto.connection import ConnectionConfirmedDto, ReceiverReadyDto


class ConnectionConfirmedHandler(IEventHandler):
    """Handler xử lý CONNECTION_CONFIRMED event và lưu session ID"""

    event = ReceiverEvent.CONNECTION_CONFIRMED
    namespace = ReceiverNamespace.ROOT

    async def handle(self, sio: AsyncClient, session_id: str | None, data=None):
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
            print(f"[Receiver] Invalid CONNECTION_CONFIRMED data: {e}")
            await sio.disconnect()
            return None

        if dto:
            print(f"[Receiver] Connection confirmed with session ID: {dto.client_sid}")
            # Trả về session_id mới để wrapper cập nhật vào registry

            # Vì sao cần retry:
            # - Khi nhận CONNECTION_CONFIRMED, namespace về mặt kỹ thuật đã connected
            # - Nhưng internal state của SocketIO client có thể chưa update xong
            # - Dẫn đến BadNamespaceError: "/ is not a connected namespace"
            # - Retry với delay ngắn để đợi client hoàn tất initialization

            max_retries = 3
            retry_delay = 0.05  # 50ms delay giữa các lần retry

            # Tạo DTO và serialize để emit
            ready_dto = ReceiverReadyDto(session_id=dto.client_sid)
            payload = ready_dto.model_dump(by_alias=True)

            for attempt in range(max_retries):
                try:
                    await sio.emit(
                        ReceiverEvent.RECEIVER_READY.value,
                        payload,
                        namespace=ReceiverNamespace.ROOT.value,
                    )
                    print(
                        f"[Receiver] Emitted receiver_ready event with session_id: {dto.client_sid}"
                    )
                    # Emit thành công, thoát loop
                    break

                except BadNamespaceError as e:
                    if attempt < max_retries - 1:
                        # Còn lần retry
                        print(
                            f"[Receiver] Namespace not ready (attempt {attempt + 1}/{max_retries}), "
                            f"retrying in {retry_delay}s..."
                        )
                        await asyncio.sleep(retry_delay)
                    else:
                        # Hết lần retry
                        print(
                            f"[Receiver] Failed to emit receiver_ready after {max_retries} attempts: {e}"
                        )
                        # Không raise exception để không block việc lưu session_id
                        # Session_id vẫn được trả về và lưu vào registry

            return dto.client_sid
        else:
            print(
                f"[Receiver] CONNECTION_CONFIRMED received but no session ID in data"
            )
            await sio.disconnect()
