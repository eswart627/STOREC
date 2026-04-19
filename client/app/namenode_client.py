import grpc

import proto.namenode_pb2 as namenode_pb2
import proto.namenode_pb2_grpc as namenode_pb2_grpc
import proto.common_pb2 as common_pb2

from .config_loader import (
    NAMENODE_ADDRESS,
    NAMENODE_PORT,
    K,
    M,
    BLOCK_SIZE
)


class NameNodeClient:
    def __init__(self):
        target = f"{NAMENODE_ADDRESS}:{NAMENODE_PORT}"
        self.channel = grpc.insecure_channel(target)
        self.stub = (
            namenode_pb2_grpc.NameNodeServiceStub(
                self.channel
            )
        )

    def allocate_blocks(self,file_name,file_size):
        stripe_size = K * BLOCK_SIZE

        file_meta = common_pb2.File(
            file_name=file_name,
            file_size=file_size
        )

        request = namenode_pb2.AllocateBlocksRequest(
            file_details=file_meta,
            stripe_size=stripe_size,
            data_blocks_k=K,
            parity_blocks_m=M
        )

        response = self.stub.AllocateBlocks(request)
        return response

    def commit_file(self,file_name,file_size,block_ids):
        file_meta = common_pb2.File(
            file_name=file_name,
            file_size=file_size
        )

        request = namenode_pb2.CommitFileRequest(
            file_details=file_meta,
            total_blocks=len(block_ids),
            block_ids=block_ids
        )

        response = self.stub.CommitFile(request)

        if not response.status.success:
            raise Exception(
                f"Commit failed: "
                f"{response.status.message}"
            )