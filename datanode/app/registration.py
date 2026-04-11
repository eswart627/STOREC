import time
import datetime
import os
import pytz  # Add this import for timezone handling

from proto import common_pb2
from proto import namenode_pb2

# This class is to manage the data node's identity (node_id) and persist it across restarts
class DataNodeIdentity:
    def __init__(self, storage_path="/data/node_id.txt"):
        self.storage_path = storage_path

    def get_id(self):
        if os.path.exists(self.storage_path):
            with open(self.storage_path, "r") as f:
                return f.read().strip()
        return None

    def save_id(self, node_id):
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        with open(self.storage_path, "w") as f:
            f.write(node_id)
            

class RegistrationManager:
    def __init__(self, rpc_client, config, logger):
        self.rpc = rpc_client
        self.config = config
        self.logger = logger
    def register(self):
        identity_store = DataNodeIdentity()
        existing_id = identity_store.get_id()
        
        node = common_pb2.Node(
            hostname = self.config.hostname,
            port = self.config.port
        )
        request = namenode_pb2.RegisterRequest(
            node = node,
            node_id=existing_id if existing_id else "",
            capacity_bytes = self.config.capacity_bytes
        )
        while True:
            try:
                response = self.rpc.stub.RegisterDataNode(request)
                if response.status.success:
                    # Store the ID returned by NameNode (works for both New and Re-registration)
                    assigned_id = response.node_id
                    identity_store.save_id(assigned_id)
                    self.config.node_id = assigned_id # Update config in memory
                
                    current_time = datetime.datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%H:%M:%S")
                    self.logger.log("REGISTER_SUCCESS", f"ID: {assigned_id} at {current_time}")
                    return assigned_id
            except Exception as e:
                print(f"!!! REGISTRATION FAILED: {e}", flush=True) # ADD THIS
                self.logger.log("REGISTER_FAILED", str(e))
                time.sleep(5)