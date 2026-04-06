import time
import threading

class DataNodeServer:
    def __init__(self, config, logger, storage):
        self.config = config
        self.logger = logger
        self.storage = storage
        self.running = False
    
    def start(self):
        self.running = True
        thread = threading.Thread(target=self._run_server, daemon=True)
        thread.start()
        self.logger.log("SERVER_START", f"port={self.config.port}")

    def _run_server(self):
        while self.running:
            time.sleep(1)
    def stop(self):
        self.running = False
        self.logger.log("SERVER_STOP", "graceful")