import grpc
import time

from proto import namenode_pb2_grpc

class RPCClient:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.channel = None
        self.stub = None
    def connect(self):
        while True:
            try:
                target = (
                    f"{self.config.namenode_host}:"
                    f"{self.config.namenode_port}"
                )
                self.channel = grpc.insecure_channel(target)
                self.stub = (
                    namenode_pb2_grpc
                    .NameNodeServiceStub(self.channel)
                )
                self.logger.log(
                    "RPC_CONNECTED",
                    target
                )
                return
            except Exception as e:
                self.logger.log(
                    "RPC_CONNECTION_FAILED",
                    str(e)
                )
            time.sleep(5)
    # def register(self):
    #     node = common_pb2.NodeId(
    #         node_id=self.config.node_id,
    #         hostname = self.config.hostname,
    #         port = self.config.port
    #     )
    #     request = (
    #         namenode_pb2.RegisterRequest(
    #             node=node,
    #             capacity_bytes=self.config.capacity_bytes
    #         )
    #     )
    #     while True:
    #         try:
    #             response = (
    #                 self.stub
    #                 .RegisterDataNode(request)
    #             )
    #             if response.status.success:
    #                 self.logger.log(
    #                     "REGISTER_SUCCESS",
    #                     self.config.node_id
    #                 )
    #             return
    #         except Exception as e:
    #             self.logger.log(
    #                 "REGISTER_FAILED",
    #                 str(e)
    #             )
    #         time.sleep(5)