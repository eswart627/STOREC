import time
import datetime
import os
import pytz

from proto import common_pb2
from proto import namenode_pb2


# FIXED: storage path now depends on port
class DataNodeIdentity:
    def __init__(self, port, storage_dir="./data"):
        self.storage_path = os.path.join(storage_dir, f"node_id_{port}.txt")

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
        # FIX: pass port → unique identity per datanode
        identity_store = DataNodeIdentity(port=self.config.port)

        existing_id = identity_store.get_id()
        
        node = common_pb2.Node(
            
            hostname=self.config.hostname,
            port=self.config.port
        )

        request = namenode_pb2.RegisterRequest(
            node=node,
            node_id=existing_id if existing_id else "",
            capacity_bytes=self.config.capacity_bytes
        )

        while True:
            try:
                print("DEBUG: sending node_id =", existing_id)  # optional debug

                response = self.rpc.stub.RegisterDataNode(request)

                if response.status.success:
                    assigned_id = response.node_id

                    # save unique ID per node
                    identity_store.save_id(assigned_id)

                    self.config.node_id = assigned_id
                
                    current_time = datetime.datetime.now(
                        pytz.timezone("Asia/Kolkata")
                    ).strftime("%H:%M:%S")

                    self.logger.log(
                        "REGISTER_SUCCESS",
                        f"ID: {assigned_id} at {current_time}"
                    )

                    return assigned_id

            except Exception as e:
                print(f"!!! REGISTRATION FAILED: {e}", flush=True)
                self.logger.log("REGISTER_FAILED", str(e))
                time.sleep(5)