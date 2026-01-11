"""
Main server entry point
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from socketio import AsyncServer, ASGIApp

from src.socketio_server.main.registry import MainEventRegistry as ServerRegistry


def create_app() -> FastAPI:
    """Create FastAPI application with Socket.IO mounted"""
    # Create FastAPI app
    app = FastAPI(title="SocketIO Server", version="1.0.0")

    # Create SocketIO server
    sio = AsyncServer(
        async_mode='asgi',
        cors_allowed_origins='*',
        logger=False,
        engineio_logger=False
    )

    # Register chat event handlers
    ServerRegistry(sio)    
    
    # Create Socket.IO ASGI app
    socket_app = ASGIApp(sio, app)

    # Mount Socket.IO app at /socket.io path
    app.mount("/", socket_app)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost", "*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    return app
