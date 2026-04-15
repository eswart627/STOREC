import time
import threading
import sys
import os # Added for hard process termination
import shutil

from proto import namenode_pb2
from proto import common_pb2 

class HeartbeatManager:
    def __init__(self, rpc_client, config, logger, storage):
        self.rpc = rpc_client
        self.config = config
        self.logger = logger
        self.storage = storage 
        self.running = False
        
        # Production tuning
        self.max_silent_time = 90  # 1.5 minutes
        self.max_sleep = 30        
        self.base_interval = self.config.heartbeat_interval

    def start(self):
        print("Starting HeartbeatManager thread", flush=True)
        self.running = True
        thread = threading.Thread(target=self._run, daemon=True)
        thread.start()

    def _run(self):
        failed_attempts = 0
        first_failure_time = None
        print(self.config.node_id, flush=True)  # Debug print to check node_id value
        while self.running:
            try:
                
                total, used, free = shutil.disk_usage("/data")
                
                heartbeat_data = common_pb2.Heartbeat(
                    node_id = self.config.node_id,
                    timestamp = int(time.time()),
                    used_bytes = used, 
                    free_bytes = free
                )
                request = namenode_pb2.HeartbeatRequest(heartbeat=heartbeat_data)

                # Attempt RPC
                response = self.rpc.stub.SendHeartbeat(request)

                if response.status.success:
                    if failed_attempts > 0:
                        recovery_msg = f"RECONNECTED after {failed_attempts} failed attempts."
                        print(f"\n{recovery_msg}", flush=True)
                        self.logger.log("HEARTBEAT_RECOVERED", recovery_msg)
                    
                    failed_attempts = 0
                    first_failure_time = None
                    self.logger.log("HEARTBEAT_SENT", f"ACK: {response.status.message}")
                    
                    time.sleep(self.base_interval)

            except Exception:
                failed_attempts += 1
                current_time = time.time()
                
                if first_failure_time is None:
                    first_failure_time = current_time
                    self.logger.log("HEARTBEAT_FAILED", "NameNode unreachable. Starting retries.")

                elapsed = int(current_time - first_failure_time)
                
                # Critical Timeout Check
                if elapsed > self.max_silent_time:
                    error_msg = f"CRITICAL: NameNode unreachable for {elapsed}s. Shutting down."
                    print(f"\n{error_msg}", flush=True)
                    self.logger.log("NODE_SHUTDOWN", error_msg)
                    
                    # USE os._exit(1) TO KILL THE ENTIRE PROCESS
                    # sys.exit(1) only kills the current thread; os._exit(1) terminates the 
                    # entire program, which is necessary since the main thread is in a loop.
                    os._exit(1)

                # Exponential Backoff calculation
                backoff = min(self.max_sleep, self.base_interval * (2 ** (failed_attempts - 1)))
                
                print(f"[RETRY {failed_attempts}] Waiting {backoff}s... (Total silence: {elapsed}s)", flush=True)
                
                if failed_attempts % 5 == 0:
                    self.logger.log("HEARTBEAT_RETRYING", f"Attempt {failed_attempts}, still no connection.")

                time.sleep(backoff)