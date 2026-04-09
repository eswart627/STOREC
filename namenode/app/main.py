from pathlib import Path

from namenode.app.config_loader import Config
from namenode.app.logger import Logger
from namenode.app.registry import DataNodeRegistry
from namenode.app.server import NameNodeServer


def main():

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


if __name__ == "__main__":

    main()