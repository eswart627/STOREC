import configparser
import os

class Config:
    def __init__(self, path: str):
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        parser = configparser.ConfigParser()
        parser.read(path)

        # node init
        # Instead of this:
        # self.node_id = parser.get("NODE", "node_id")

        # Use this:
        self.node_id = os.getenv("NODE_ID", parser.get("NODE", "node_id",fallback=None))
        self.hostname = os.getenv("NODE_HOSTNAME", parser.get("NODE", "hostname"))
        self.port = int(os.getenv("NODE_PORT", parser.getint("NODE", "port")))
        self.capacity_bytes = parser.getint(
            "STORAGE",
            "capacity_bytes"
        )
        self.heartbeat_interval = parser.getint(
            "HEARTBEAT",
            "interval_seconds",
            fallback=5
        )
        # name node
        self.namenode_host = os.getenv("NAMENODE_HOST", parser.get("NAMENODE", "host"))
        self.namenode_port = os.getenv("NAMENODE_PORT", parser.get("NAMENODE", "port"))

        #storage
        self.data_dir = parser.get(
            "STORAGE", "data_dir"
        )

        #server
        self.worker_threads = parser.getint(
            "SERVER", "worker_threads"
        )

        self.validate()

    def validate(self):
        if not self.namenode_host:
            raise ValueError("namenode host missing")
        if not self.namenode_port:
            raise ValueError("namenode port missing")