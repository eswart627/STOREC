import time
import threading

from proto import namenode_pb2

class HeartbeatManager:
    def __init__(self, rpc_client, config, logger, storage):
        self.rpc = rpc_client
        self.config = config
        self.logger = logger
        self.storage = storage
        self.running = False

    def start(self):
        print("Starting HeartbeatManager thread", flush=True)  # Debug print to confirm thread start
        self.running = True
        thread = threading.Thread(target=self._run, daemon=True)
        thread.start()
        print("HeartbeatManager thread started", flush=True)  # Debug print to confirm thread started

    def _run(self):
        while self.running:
            try:
                request = namenode_pb2.HeartbeatRequest(
                    heartbeat = namenode_pb2.Heartbeat(
                        node_id = self.config.node_id,
                        timestamp = int(time.time()),
                        used_bytes = 100,   # you can improve later
                        free_bytes = self.config.capacity_bytes - 100
                    )
                )

                print(f"Sending heartbeat: node_id={self.config.node_id}, timestamp={int(time.time())}", flush=True)  # Debug print before sending heartbeat
                self.rpc.stub.SendHeartbeat(request)

                print("Heartbeat sent successfully", flush=True)  # Debug print for successful heartbeat
                self.logger.log(
                    "HEARTBEAT_SENT",
                    self.config.node_id
                )

            except Exception as e:
                print(f"Heartbeat failed: {e}", flush=True)  # Debug print for failed heartbeat
                self.logger.log(
                    "HEARTBEAT_FAILED",
                    str(e)
                )

            time.sleep(self.config.heartbeat_interval)