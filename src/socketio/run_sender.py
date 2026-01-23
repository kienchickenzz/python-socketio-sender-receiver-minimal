"""
Main client entry point - DDD Architecture Demo
"""
import asyncio

from socketio import AsyncClient

from src.socketio.socketio_client.sender.registry import SenderEventRegistry
from src.socketio.socketio_client.sender.enum.SenderEvent import SenderEvent


async def run_client():
    """Run SocketIO Client"""
    # Create SocketIO client
    sio = AsyncClient(logger=False, engineio_logger=False)
    registry = SenderEventRegistry(sio)

    try:
        await sio.connect('http://localhost:5000', namespaces=['/'])

        # Do something here

        # Keep client running
        await sio.wait()

    except asyncio.CancelledError:
        print("👋 Client stopped by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Make sure the server is running!")
    finally:
        # Emit disconnect event to server before disconnecting
        if registry.session_id:
            print(f"[Sender] Emitting disconnect event...")
            await sio.emit(
                SenderEvent.SENDER_DISCONNECTED.value,
                {"sessionId": registry.session_id}
            )
        await sio.disconnect()