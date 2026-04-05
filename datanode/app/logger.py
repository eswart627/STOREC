import os
import time
from datanode.app.constants import LOG_DIR_NAME, LOG_FILE_NAME

class Logger:
    def __init__(self, base_dir:str):
        self.log_dir = os.path.join(
            base_dir, "datanode",LOG_DIR_NAME
        )
        os.makedirs(self.log_dir, exist_ok=True)
        self.file_path = os.path.join(
            self.log_dir,
            LOG_FILE_NAME
        )

    def log(self, event: str, details: str):
        timestamp = int(time.time())
        line = (
            f"{timestamp},"
            f"DATANODE,"
            f"{event},"
            f"{details}\n"
        )
        with open(
            self.file_path,
            "a"
        ) as f:
            f.write(line)