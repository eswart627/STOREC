import os
import time
from datanode.app.constants import LOG_DIR_NAME, LOG_FILE_NAME

class Logger:
    def __init__(self):
        # This finds the actual folder where this logger.py file lives
        # Usually: .../storec/datanode/
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Build path to logs folder: .../storec/datanode/logs
        self.log_dir = os.path.join(current_dir, "..", LOG_DIR_NAME)
        self.log_dir = os.path.abspath(self.log_dir)  # Resolve to absolute path
        
        # Create the logs folder if it isn't there
        os.makedirs(self.log_dir, exist_ok=True)
        
        
        # Final full path to the log file
        self.file_path = os.path.join(self.log_dir, LOG_FILE_NAME)
        
        # Clear the log file at the start of execution
        with open(self.file_path, "w") as f:
            f.truncate(0)  # Truncate the file to ensure it starts empty
        print("Cleared log file at the start of execution.", flush=True)  # Debug print to confirm clearing

        print(f"Logger active. Path: {self.file_path}")

    def log(self, event: str, details: str):
        timestamp = int(time.time())
        line = (
            f"{timestamp},"
            f"DATANODE,"
            f"{event},"
            f"{details}\n"
        )
        try:
            with open(self.file_path, "a") as f:
                f.write(line)
        except Exception as e:
            print(f"Failed to write log: {e}")