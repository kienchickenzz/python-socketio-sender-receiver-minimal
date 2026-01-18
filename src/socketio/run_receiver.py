"""
Main client entry point - DDD Architecture Demo
"""
import asyncio

from socketio import AsyncClient

from src.socketio.socketio_client.receiver.registry import ReceiverEventRegistry


async def run_client():
    """Run SocketIO Client"""
    # Create SocketIO client
    sio = AsyncClient(logger=False, engineio_logger=False)
    ReceiverEventRegistry(sio)

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
        await sio.disconnect()