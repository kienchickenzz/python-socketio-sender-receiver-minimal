"""
Main entry point for SocketIO Server/Client

Usage:
    python -m src --server  # Run server
    python -m src --client  # Run client
"""
import argparse
import asyncio


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SocketIO Server/Client Demo")
    group = parser.add_mutually_exclusive_group(required=True)

    # Run Mode
    group.add_argument("--server", action="store_true", help="Run SocketIO server")
    group.add_argument("--receiver", action="store_true", help="Run SocketIO receiver client")
    group.add_argument("--sender", action="store_true", help="Run SocketIO sender client")

    options = parser.parse_args()

    if options.server:
        import uvicorn

        from src.run_server import create_app
        app = create_app()
        uvicorn.run(app, host="0.0.0.0", port=5000)

    elif options.receiver:
        from src.run_receiver import run_client as run_receiver
        asyncio.run(run_receiver())

    elif options.sender:
        from src.run_sender import run_client as run_sender
        asyncio.run(run_sender())