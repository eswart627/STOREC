import configparser
import os


class Config:

    def __init__(self, path):

        if not os.path.exists(path):

            raise FileNotFoundError(path)

        parser = configparser.ConfigParser()

        parser.read(path)

        self.hostname = parser.get(
            "NODE",
            "hostname",
        )

        self.port = parser.getint(
            "NODE",
            "port",
        )

        self.worker_threads = parser.getint(
            "SERVER",
            "worker_threads",
        )