from pathlib import Path
import sys
import signal
from types import FrameType

from .config_loader import Config
from .logger import Logger
from .registry import DataNodeRegistry
from .server import NameNodeServer

server = None

def signal_handler(signum: int, frame: FrameType | None) -> None:
    """Handle shutdown signals"""
    print("\nReceived shutdown signal. Cleaning up...", flush=True)

    print("Stopping NameNode services...", flush=True)
    if 'server' in globals():
        server.stop()

    print("NameNode stopped gracefully.", flush=True)
    sys.exit(0)

def main(flag:int=0) -> None:
    global server

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
    restored_count = registry.load_state()
    logger.log("REGISTRY_RESTORED", f"Loaded {restored_count} DataNode record(s) from DB",0)

    print("Initializing NameNode server", flush=True)  # Debug print to indicate server initialization
    server = NameNodeServer(
        config,
        registry,
        logger,
        flag
    )

    print("Starting NameNode server", flush=True)  # Debug print to indicate server startup

    server.start()

if __name__ == "__main__":
    
    flag = 0
    if len(sys.argv) > 1:
        flag = int(sys.argv[1])
    main(flag)