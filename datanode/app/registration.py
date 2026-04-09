import time
import datetime
import pytz  # Add this import for timezone handling

from proto import common_pb2
from proto import namenode_pb2

class RegistrationManager:
    def __init__(self, rpc_client, config, logger):
        self.rpc = rpc_client
        self.config = config
        self.logger = logger
    def register(self):
        node = common_pb2.NodeId(
            node_id = self.config.node_id,
            hostname = self.config.hostname,
            port = self.config.port
        )
        request = namenode_pb2.RegisterRequest(
            node = node,
            capacity_bytes = self.config.capacity_bytes
        )
        while True:
            try:
                response = self.rpc.stub.RegisterDataNode(request)
                if response.status.success:
                    current_time = datetime.datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%H:%M:%S")  # Format time as HH:MM:SS in IST
                    self.logger.log(
                        "REGISTER_SUCCESS",
                        f"{self.config.node_id} at {current_time}"
                    )
                    return
            except Exception as e:
                self.logger.log(
                    "REGISTER_FAILED",
                    str(e)
                )
            time.sleep(5)