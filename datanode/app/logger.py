import os
import time
from datanode.app.constants import LOG_DIR_NAME, LOG_FILE_NAME , WRITE_THROUGHPUT_LOG_FILE_NAME

class Logger:
    def __init__(self, base_dir:str):
        # This finds the actual folder where this logger.py file lives
        # Usually: .../storec/datanode/
    
        # Build path to logs folder: .../storec/datanode/data/..
        self.log_dir = os.path.join(base_dir, LOG_DIR_NAME)
 
        # Create the logs folder if it isn't there
        os.makedirs(self.log_dir, exist_ok=True)
        
        # Final full path to the log file
        self.file_path = os.path.join(self.log_dir, LOG_FILE_NAME)
        self.write_throughput_path = os.path.join(self.log_dir, WRITE_THROUGHPUT_LOG_FILE_NAME)
        # Clear the log file at the start of execution
        # Truncate the file to ensure it starts empty
        for path in [self.file_path, self.write_throughput_path]:
            with open(path, "w") as f:
                f.truncate(0)
        print(f"Logger active. Path: {self.file_path}")
        print(f"Write throughput log path: {self.write_throughput_path}")
        
       

    def log(self, event: str, details: str, is_throughput: bool = False):
        """
        Logs to datanode.log by default. 
        Set is_throughput=True to log to write_throughput.log.
        """
        
        timestamp = int(time.time())
        line = (
            f"{timestamp},"
            f"DATANODE,"
            f"{event},"
            f"{details}\n"
        )
        
        target_path = self.write_throughput_path if is_throughput else self.file_path
        
        try:
            with open(target_path, "a") as f:
                f.write(line)
        except Exception as e:
            print(f"Failed to write log: {e}")