import configparser
import os


class Config:
    def __init__(self, path: str):
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        parser = configparser.ConfigParser()
        parser.read(path)

        def env_or_default(key: str, default):
            value = os.getenv(key)
            if value not in (None, ""):
                return value.strip()
            if default is None:
                return None
            return str(default).strip()
        self.hostname = env_or_default(
            "NODE_HOSTNAME",
            parser.get("NODE", "hostname")
        )
        self.port = int(
            env_or_default(
                "NODE_PORT",
                parser.getint("NODE", "port")
            )
        )

        # node-id is set after the registration 
        self.node_id = None

        # namenode config
        self.namenode_host = env_or_default(
            "NAMENODE_HOST",
            parser.get("NAMENODE", "host")
        )
        self.namenode_port = int(
            env_or_default(
                "NAMENODE_PORT",
                parser.getint("NAMENODE", "port")
            )
        )

        #storage configuration
        data_dir_value = env_or_default(
            "DATA_DIR",
            parser.get("STORAGE", "data_dir")
        )
        self.data_dir = os.path.abspath(
            data_dir_value
        )
        self.capacity_bytes = int(
            env_or_default(
                "CAPACITY_BYTES",
                parser.getint(
                    "STORAGE",
                    "capacity_bytes"
                )
            )
        )

        # heartbeat config
        self.heartbeat_interval = int(
            env_or_default(
                "HEARTBEAT_INTERVAL",
                parser.getint(
                    "HEARTBEAT",
                    "interval_seconds",
                    fallback=5
                )
            )
        )

        # threading
        self.worker_threads = int(
            env_or_default(
                "WORKER_THREADS",
                parser.getint(
                    "SERVER",
                    "worker_threads"
                )
            )
        )
        # grpc
        self.grpc_max_message = int(
            env_or_default(
                "GRPC_MAX_MESSAGE",
                parser.getint(
                    "GRPC",
                    "grpc_max_message",
                    fallback=100 * 1024 * 1024
                )
            )
        )
        self.validate()

    def validate(self):
        if not self.hostname:
            raise ValueError(
                "node hostname missing"
            )
        if not self.namenode_host:
            raise ValueError(
                "namenode host missing"
            )
        if not self.namenode_port:
            raise ValueError(
                "namenode port missing"
            )
        if self.capacity_bytes <= 0:
            raise ValueError(
                "capacity must be positive"
            )