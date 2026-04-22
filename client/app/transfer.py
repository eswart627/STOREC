import grpc

import proto.common_pb2 as common_pb2
import proto.datanode_pb2 as datanode_pb2
import proto.datanode_pb2_grpc as datanode_pb2_grpc
from .config_loader import GRPC_MAX_MESSAGE


class DataNodeClient:
    def __init__(self, address, port):
        target = f"{address}:{port}"
        self.channel = grpc.insecure_channel(
            target,
            options=[
                ("grpc.max_send_message_length", GRPC_MAX_MESSAGE),
                ("grpc.max_receive_message_length", GRPC_MAX_MESSAGE)
            ]
        )   
        self.stub = (
            datanode_pb2_grpc.DataNodeServiceStub(
                self.channel
            )
        )

    def write_block(self,block_id,data):
        block = common_pb2.Block(
            block_id=block_id,
            data_bytes=data,
            block_size=len(data)
        )
        request = (
            datanode_pb2.WriteBlockRequest(
                block=block
            )
        )
        response = self.stub.WriteBlock(iter([request]))
        #return response.status.success
        return response