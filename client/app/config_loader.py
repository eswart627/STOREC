import configparser
import os


CONFIG_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "config",
    "client.config"
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