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
            return str(default).strip() if default else default

        # node init
        self.node_id = env_or_default("NODE_ID", parser.get("NODE", "node_id", fallback=None))
        self.hostname = env_or_default("NODE_HOSTNAME", parser.get("NODE", "hostname"))
        self.port = int(env_or_default("NODE_PORT", parser.getint("NODE", "port")))
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
        self.namenode_host = env_or_default("NAMENODE_HOST", parser.get("NAMENODE", "host"))
        self.namenode_port = env_or_default("NAMENODE_PORT", parser.get("NAMENODE", "port"))

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
        if not self.hostname:
            raise ValueError("node hostname missing")
        if not self.namenode_host:
            raise ValueError("namenode host missing")
        if not self.namenode_port:
            raise ValueError("namenode port missing")
