import grpc
import time
import os

import proto.common_pb2 as common_pb2
import proto.datanode_pb2 as datanode_pb2
import proto.datanode_pb2_grpc as datanode_pb2_grpc
from .config_loader import GRPC_MAX_MESSAGE


class DataNodeClient:
    def __init__(self, address, port):
        self.target = f"{address}:{port}"
        self.channel = grpc.insecure_channel(
            self.target,
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
    
    def read_block(self, block_id):
        request = datanode_pb2.ReadBlockRequest(
            block_id=block_id
        )

        responses = self.stub.ReadBlock(request)

        data = bytearray()

        for response in responses:
            if not response.status.success:
                raise Exception(response.status.message)

            data.extend(response.block.data_bytes)

        return bytes(data)
    
    def delete_block(self, block_id):
        request = datanode_pb2.DeleteBlockRequest(
            block_id=block_id
        )

        response = self.stub.DeleteBlock(request)

        if not response.status.success:
            raise Exception(response.status.message)

        return True

    def measure_network_throughput(self, test_size_mb=10):
        """Measure raw gRPC channel capacity"""
        # Generate test data
        test_data = os.urandom(test_size_mb * 1024 * 1024)
        
        # Measure pure network transfer
        start_time = time.time()
        
        # Send data without disk operations (just measure network)
        block = common_pb2.Block(
            block_id="network_test",
            data_bytes=test_data,
            block_size=len(test_data)
        )
        request = datanode_pb2.WriteBlockRequest(block=block)
        
        # This measures network + gRPC overhead only
        response = self.stub.WriteBlock(iter([request]))
        
        end_time = time.time()
        transfer_time = end_time - start_time
        
        # Calculate raw channel capacity
        network_throughput_mbps = test_size_mb / transfer_time
        
        return {
            'test_size_mb': test_size_mb,
            'transfer_time_seconds': transfer_time,
            'network_throughput_mbps': network_throughput_mbps,
            'target': self.target
        }