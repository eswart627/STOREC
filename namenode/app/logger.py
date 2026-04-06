import os
import time


class Logger:
    """
    Logger for the name node.
    
    Attributes:
        log_dir: Directory to store log files.
        file: Path to the log file.
    """
    def __init__(self, base_dir):
        """
        Initialize the logger.
        
        Args:
            base_dir: Base directory for the name node.
        """
        self.log_dir = os.path.join(base_dir,"logs")
        os.makedirs(self.log_dir,exist_ok=True)
        self.file = os.path.join(self.log_dir,"namenode.log")
    
    def log(self, event, details):
        """
        Log an event.
        
        Args:
            event: Event to log.
            details: Details of the event.
        """
        ts = int(time.time())

        line = f"{ts},{event},{details}\n"

        with open(self.file,"a") as f:

            f.write(line)