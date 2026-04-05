import os
import time


class Logger:

    def __init__(self, base_dir):

        self.log_dir = os.path.join(
            base_dir,
            "logs",
        )
        os.makedirs(
            self.log_dir,
            exist_ok=True,
        )
        self.file = os.path.join(
            self.log_dir,
            "namenode.log",
        )
    def log(
        self,
        event,
        details,
    ):

        ts = int(time.time())

        line = (
            f"{ts},"
            f"NAMENODE,"
            f"{event},"
            f"{details}\n"
        )

        with open(
            self.file,
            "a",
        ) as f:

            f.write(line)