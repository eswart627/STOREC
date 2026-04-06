import configparser
import os


class Config:
    """
    Configuration loader.
    
    Attributes:
        hostname: Hostname of the node.
        port: Port number of the node.
        worker_threads: Number of worker threads for the server.
    """
    def __init__(self, path):
        """
        Initialize the configuration loader.
        
        Args:
            path: Path to the configuration file.
        """
        if not os.path.exists(path):

            raise FileNotFoundError(path)

        parser = configparser.ConfigParser()

        parser.read(path)

        self.hostname = parser.get("NODE","hostname")

        self.port = parser.getint("NODE","port")

        self.worker_threads = parser.getint("SERVER","worker_threads")
