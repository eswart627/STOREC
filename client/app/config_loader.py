import configparser
import os
import logging


BASE_DIR = os.path.dirname(os.path.dirname(__file__)) # cd storec/client

print(f"BASE_DIR: {BASE_DIR}")

CONFIG_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "config",
    "client.config"
)

LOG_DIR = os.path.join(BASE_DIR,"logs")
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

LOG_FILE = os.path.join(LOG_DIR, "client.log")

logging.basicConfig(
    filename=LOG_FILE,
    filemode='a',
    level=logging.INFO,
    format="%(message)s",
)


config = configparser.ConfigParser()
config.read(CONFIG_PATH)

# Cluster
NAMENODE_ADDRESS = config.get("cluster","namenode_address")
NAMENODE_PORT = config.getint("cluster","namenode_port")

# Erasure coding
K = config.getint("erasure_coding","k")
M = config.getint("erasure_coding","m")
BLOCK_SIZE = config.getint("erasure_coding","block_size")
CELL_SIZE = config.getint("erasure_coding","cell_size")
DATA_PER_STRIPE = K * BLOCK_SIZE
BLOCKS_PER_STRIPE = K + M

# Multi threading parameters
MAX_WORKERS = config.getint("pipeline", "max_workers")
STRIPES_IN_PARALLEL = config.getint("pipeline", "max_inflight_stripes")

# gRPC
GRPC_MAX_MESSAGE = config.getint("grpc", "grpc_max_message")