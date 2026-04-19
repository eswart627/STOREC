import time
import datetime
import os
import pytz

from proto import common_pb2
from proto import namenode_pb2

class DataNodeIdentity:
    def __init__(self, base_storage_dir, port):
        self.base_storage_dir = base_storage_dir
        self.node_dir = os.path.join(
            base_storage_dir,
            f"node_{port}"
        )
        self.meta_path = os.path.join(
            self.node_dir,
            "node.meta"
        )

    def find_existing_id(self):
        if not os.path.exists(self.meta_path):
            return None
        with open(self.meta_path, "r") as f:
            for line in f:
                if line.startswith("node_id="):
                    return line.split("=")[1].strip()
        return None

    def save_id(self, node_id):
        os.makedirs(
            self.node_dir,
            exist_ok=True
        )
        with open(self.meta_path, "w") as f:
            f.write(f"node_id={node_id}\n")
        return self.node_dir

class RegistrationManager:
    def __init__(self, rpc_client, config):
        self.rpc = rpc_client
        self.config = config

    def register(self):
        identity_store = DataNodeIdentity(
            base_storage_dir = self.config.data_dir,
            port = self.config.port
        )
        existing_id = identity_store.find_existing_id() 
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
                print("DEBUG: sending node_id =", existing_id)
                response = self.rpc.stub.RegisterDataNode(request)
                if response.status.success:
                    assigned_id = response.node_id

                    # save unique ID per node
                    identity_store.save_id(assigned_id)

                    self.config.node_id = assigned_id
                    self.config.node_storage_dir = identity_store.node_dir

                    current_time = datetime.datetime.now(
                        pytz.timezone("Asia/Kolkata")
                    ).strftime("%H:%M:%S")

                    print(
                        "REGISTER_SUCCESS",
                        f"ID: {assigned_id} at {current_time}"
                    )
                    return assigned_id

            except Exception as e:
                print(f"ERROR: REGISTRATION FAILED: {e}", flush=True)
                print("REGISTER_FAILED", str(e))
                time.sleep(5)