import os
import time


class Logger:

    def __init__(self, base_dir):
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