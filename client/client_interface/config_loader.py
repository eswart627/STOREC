import os
from configparser import ConfigParser


BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

CONFIG_PATH = os.path.join(
    BASE_DIR,
    "config",
    "client.config"
)

config = ConfigParser()

if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(
        f"Config file not found: {CONFIG_PATH}"
    )

config.read(CONFIG_PATH)


NAMENODE_ADDRESS = config.get(
    "cluster",
    "namenode_address"
)

NAMENODE_PORT = config.getint(
    "cluster",
    "namenode_port"
)

NAMENODE_TARGET = (
    f"{NAMENODE_ADDRESS}:"
    f"{NAMENODE_PORT}"
)


MAX_WORKERS = config.getint(
    "pipeline",
    "max_workers"
)


GRPC_MAX_MESSAGE = config.getint(
    "grpc",
    "grpc_max_message"
)


DEFAULT_MODE = config.get(
    "pipeline",
    "default_mode",
    fallback="parallel"
)