"""
Main client entry point - DDD Architecture Demo

Nhấn 'q' + Enter để graceful disconnect (emit SENDER_DISCONNECTED trước khi ngắt).
Ctrl+C để force disconnect (không emit custom event).
"""
import asyncio

from socketio import AsyncClient

from src.socketio.socketio_client.sender.registry import SenderEventRegistry
from src.socketio.socketio_client.sender.enum.SenderEvent import SenderEvent
from src.socketio.socketio_client.sender.enum.SenderNamespace import SenderNamespace


async def wait_for_quit():
    """
    Chờ user nhấn 'q' để graceful disconnect.

    Chạy input() trong thread pool để không block event loop.

    Returns:
        True khi user nhấn 'q'
    """
    loop = asyncio.get_running_loop()
    while True:
        key = await loop.run_in_executor(None, input)
        if key.lower() == "q":
            return True


async def run_client():
    """Run SocketIO Client"""
    # Create SocketIO client
    sio = AsyncClient(logger=False, engineio_logger=False)
    registry = SenderEventRegistry(sio)

    try:
        await sio.connect("http://localhost:5000", namespaces=["/"])
        print("[Sender] Connected to the server")
        print("[Sender] Press 'q' + Enter to graceful disconnect")

        # Tạo 2 tasks song song
        wait_task = asyncio.create_task(sio.wait())
        quit_task = asyncio.create_task(wait_for_quit())

        # Chờ 1 trong 2 hoàn thành
        done, pending = await asyncio.wait(
            [wait_task, quit_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Nếu quit_task hoàn thành (user nhấn 'q')
        if quit_task in done:
            # Emit custom disconnect event TRƯỚC khi disconnect
            if sio.connected and registry.session_id:
                print("[Sender] Emitting disconnect event...")
                await sio.emit(
                    SenderEvent.SENDER_DISCONNECTED.value,
                    {"sessionId": registry.session_id},
                    namespace=SenderNamespace.ROOT.value,
                )
                # Disconnect sẽ tự động kết thúc wait_task
                await sio.disconnect()
                print("[Sender] 👋 Graceful disconnect completed")
        else:
            # Server disconnect hoặc lỗi khác
            # Cancel quit_task vì không cần nữa
            quit_task.cancel()
            try:
                await quit_task
            except asyncio.CancelledError:
                pass

    except asyncio.CancelledError:
        print("👋 Client stopped by user (Ctrl+C)")
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Make sure the server is running!")