import grpc

import proto.datanode_pb2 as datanode_pb2
import proto.datanode_pb2_grpc as datanode_pb2_grpc


class DataNodeClient:

    def __init__(
        self,
        address,
        port
    ):

        target = f"{address}:{port}"

        self.channel = grpc.insecure_channel(
            target
        )

        self.stub = (
            datanode_pb2_grpc.DataNodeServiceStub(
                self.channel
            )
        )

    def write_block(
        self,
        block_id,
        data
    ):

        block = datanode_pb2.Block(
            block_id=block_id,
            block_size=len(data)
        )

        request = (
            datanode_pb2.WriteBlockRequest(
                block=block
            )
        )

        response = self.stub.WriteBlock(
            request
        )

        return response.success