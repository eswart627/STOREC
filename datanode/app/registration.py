import time

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
                    self.logger.log(
                        "REGISTER_SUCCESS",
                        self.config.node_id
                    )
                    return
            except Exception as e:
                self.logger.log(
                    "REGISTER_FAILED",
                    str(e)
                )
            time.sleep(5)