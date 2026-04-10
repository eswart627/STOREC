from pathlib import Path
import sys
import signal

from .config_loader import Config
from .logger import Logger
from .registry import DataNodeRegistry
from .server import NameNodeServer

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    print("\nReceived shutdown signal. Cleaning up...", flush=True)
    
    print("Saving registry state...", flush=True)
    if 'server' in globals():
        server.registry.save_state()
    
    print("Stopping health checker...", flush=True)
    if 'server' in globals():
        server.health_checker.running = False
    
    print("Shutting down gRPC server...", flush=True)
    if 'server' in globals():
        server.server.stop(grace=1)
    
    print("NameNode stopped gracefully.", flush=True)
    sys.exit(0)

def main():

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    base_dir = Path(__file__).resolve().parents[1]

    config_path = base_dir / "config" / "namenode.config"

    print(f"Config path: {config_path}", flush=True)  # Debug print to check config_path value
    print(f"Base directory: {base_dir}", flush=True)  # Debug print to check base_dir value

    config = Config(str(config_path))
    print(base_dir, flush=True)  # Debug print to check base_dir value}")
    logger = Logger(base_dir)

    print("Initializing DataNode registry", flush=True)  # Debug print to indicate registry initialization
    registry = DataNodeRegistry()

    print("Initializing NameNode server", flush=True)  # Debug print to indicate server initialization
    server = NameNodeServer(
        config,
        registry,
        logger,
    )

    print("Starting NameNode server", flush=True)  # Debug print to indicate server startup

    server.start()

    print("NameNode server started", flush=True)
    try:
        server.server.wait_for_termination()
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)

if __name__ == "__main__":
    main()