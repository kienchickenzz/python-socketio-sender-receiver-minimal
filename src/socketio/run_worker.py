"""
Main client entry point - DDD Architecture Demo

Nhấn 'q' + Enter để graceful disconnect (emit WORKER_DISCONNECTED trước khi ngắt).
Ctrl+C để force disconnect (không emit custom event).
"""
import asyncio
from pathlib import Path

from socketio import AsyncClient

from src.logger.LoggerConfig import LoggerConfig
from src.logger.LoggerFactory import LoggerFactory

from src.socketio.socketio_client.worker.registry import WorkerEventRegistry
from src.socketio.socketio_client.worker.enum.WorkerEvent import WorkerEvent
from src.socketio.socketio_client.worker.enum.WorkerNamespace import WorkerNamespace


async def _wait_for_quit():
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
    project_root = Path(__file__).parent.parent # thư mục chứa src/
    logger = LoggerFactory(LoggerConfig(project_root=project_root)).create()

    # Create SocketIO client
    sio = AsyncClient(logger=False, engineio_logger=False)
    registry = WorkerEventRegistry(sio)

    try:
        await sio.connect("http://localhost:5000", namespaces=["/"])
        logger.info("Client connected to the server")
        logger.info("Press 'q' + Enter to graceful disconnect")

        # Tạo 2 tasks song song
        wait_task = asyncio.create_task(sio.wait())
        quit_task = asyncio.create_task(_wait_for_quit())

        # Chờ 1 trong 2 hoàn thành
        done, pending = await asyncio.wait(
            [wait_task, quit_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Nếu quit_task hoàn thành (user nhấn 'q')
        if quit_task in done:
            # Emit custom disconnect event TRƯỚC khi disconnect
            if sio.connected and registry.session_id:
                logger.info("Emitting disconnect event...")
                await sio.emit(
                    WorkerEvent.WORKER_DISCONNECTED.value,
                    {"sessionId": registry.session_id},
                    namespace=WorkerNamespace.ROOT.value,
                )
                # Disconnect sẽ tự động kết thúc wait_task
                await sio.disconnect()
                logger.info("👋 Graceful disconnect completed")
        else:
            # Server disconnect hoặc lỗi khác
            # Cancel quit_task vì không cần nữa
            quit_task.cancel()
            try:
                await quit_task
            except asyncio.CancelledError:
                pass

    except asyncio.CancelledError:
        logger.info("👋 Client stopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        logger.error("Make sure the server is running!")