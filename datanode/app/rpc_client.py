import grpc
import time

from proto import namenode_pb2_grpc

class RPCClient:
    def __init__(self, config):
        self.config = config
        
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
                grpc.channel_ready_future(self.channel).result(timeout=5)
                self.stub = (
                    namenode_pb2_grpc
                    .NameNodeServiceStub(self.channel)
                )
                print("RPC_CONNECTED", target)
                return
            except Exception as e:
                print("RPC_CONNECTION_FAILED",str(e))
                print("RPC_RETRYING","Retrying in 5 seconds")
                time.sleep(5)
   