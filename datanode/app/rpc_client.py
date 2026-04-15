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
   