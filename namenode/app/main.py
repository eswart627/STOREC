from pathlib import Path

from namenode.app.config_loader import Config
from namenode.app.logger import Logger
from namenode.app.registry import DataNodeRegistry
from namenode.app.server import NameNodeServer


def main():

    base_dir = (
        Path(__file__)
        .resolve()
        .parents[1]
    )

    config_path = (
        base_dir
        / "config"
        / "namenode.config"
    )

    config = Config(str(config_path))

    logger = Logger(base_dir)

    registry = DataNodeRegistry()

    server = NameNodeServer(
        config,
        registry,
        logger,
    )

    server.start()


if __name__ == "__main__":

    main()