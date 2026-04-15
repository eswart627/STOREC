import grpc
import os
import argparse
from proto import datanode_pb2, datanode_pb2_grpc

def download_file(block_id, ip_address, port):
    server_addr = f"{ip_address}:{port}"
    # Using larger message length to handle PDF/large block retrieval
    options = [
        ('grpc.max_send_message_length', 60 * 1024 * 1024),
        ('grpc.max_receive_message_length', 60 * 1024 * 1024),
    ]
    
    channel = grpc.insecure_channel(server_addr, options=options)
    stub = datanode_pb2_grpc.DataNodeServiceStub(channel)

    print(f"Connecting to DataNode at {server_addr} to read: {block_id}")

    try:
        request = datanode_pb2.ReadBlockRequest(block_id=block_id)
        response = stub.ReadBlock(request)

        if response.status.success:
            # Saves it as 'recovered_' + original name in the current directory
            output_name = f"recovered_{block_id}"
            with open(output_name, "wb") as f:
                f.write(response.block.data_bytes)
            
            print("-" * 30)
            print(f"✅ READ SUCCESS!")
            print(f"File saved locally as: {output_name}")
            print(f"Size: {response.block.size_bytes} bytes")
            print("-" * 30)
        else:
            print(f"❌ Server Error: {response.status.message}")

    except grpc.RpcError as e:
        print(f"❌ gRPC Error: {e.code()}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file_id", help="The name/ID of the block to retrieve")
    parser.add_argument("--ip", default="127.0.0.1")
    parser.add_argument("--port", default="50052")

    args = parser.parse_args()
    download_file(args.file_id, args.ip, args.port)