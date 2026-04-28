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
        self.main_log= os.path.join(
            self.log_dir,
            "namenode.log"
        )
        self.maintainence_log = os.path.join(
            self.log_dir,
            "maintainence.log",
        )
        self.debug_log= os.path.join(
            self.log_dir,
            "debug.log"
        )
        self.file_log= os.path.join(
            self.log_dir,
            "file.log"
        )
        files=[self.main_log,self.maintainence_log,self.debug_log,self.file_log]
        print("\nLog file path:", base_dir)  # Debug print to verify log file path
        # Clear the log file at the start of execution
        for file in files:
            with open(file, "w") as f:
                f.truncate(0)  # Truncate the file to ensure it starts empty
            print("Cleared log file at the start of execution.", flush=True)  # Debug print to confirm clearing

    def log(self,event:str,details:str,mode:str=2)->None:
        """
        Log an event.
        
        Args:
            event: Event to log.
            details: Details of the event.
        """
        ts = int(time.time())
        
        line = f"{ts},{event},{details}\n"
        match mode:
            case 0:
                file=self.main_log
                pass
            case 1:
                file=self.maintainence_log
                pass
            case 2:
                file=self.debug_log
                pass
            case 3:
                file=self.file_log
                pass
            case _:
                print("No log file found\n")
                print(line)
                return
        with open(file,"a") as f:

            f.write(line)