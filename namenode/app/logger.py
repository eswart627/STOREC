import os
import time


class Logger:
    """
    Logger for the name node.
    
    Attributes:
        log_dir: Directory to store log files.
        file: Path to the log file.
    """
    def __init__(self, base_dir:str):
        print("Base directory for Logger:", base_dir)
        # Debug print to verify base directory for Logger
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
        print("\nLog file path:", self.file)  # Debug print to verify log file path
        # Clear the log file at the start of execution
        with open(self.file, "w") as f:
            f.truncate(0)  # Truncate the file to ensure it starts empty
        print("Cleared log file at the start of execution.", flush=True)  # Debug print to confirm clearing

    def log(self,event:str,details:str)->None:
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