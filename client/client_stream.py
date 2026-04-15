import grpc
import os
import argparse
from proto import datanode_pb2, datanode_pb2_grpc, common_pb2

def generate_chunks(file_path, block_id, chunk_size=1024*1024):
    """Generator to read file in chunks for gRPC streaming."""
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found")
        return

    with open(file_path, "rb") as f:
        while True:
            chunk_data = f.read(chunk_size) # Read 1MB at a time (you can adjust this size atmost to 4MB for gRPC)
            if not chunk_data:
                break
            
            # 1. Matches common.proto: message Block
            #create the sub-message
            block_msg = common_pb2.Block(
                block_id=str(block_id),
                size_bytes=int(len(chunk_data)),
                data_bytes=chunk_data
            )
            
            # 2. Matches datanode.proto: message WriteBlockRequest { Block block = 1; }
            yield datanode_pb2.WriteBlockRequest(block=block_msg)

def upload_file(file_path, ip_address, port):
    server_addr = f"{ip_address}:{port}"
    
    # Standard options
    options = [
        ('grpc.max_send_message_length', 60 * 1024 * 1024),
        ('grpc.max_receive_message_length', 60 * 1024 * 1024),
    ]
    
    channel = grpc.insecure_channel(server_addr, options=options)
    stub = datanode_pb2_grpc.DataNodeServiceStub(channel)
    
    # Use just the filename as the block_id
    block_id = os.path.basename(file_path)
    
    print(f"Connecting to DataNode at {server_addr}...")
    
    try:
        # Pass the generator (iterator) to the stub
        response = stub.WriteBlock(generate_chunks(file_path, block_id))
        
        if response.status.success:
            print("-" * 30)
            print(f"✅ SUCCESS!")
            print(f"Block ID: {response.block_id}")
            print(f"Node ID : {response.node.node_id}")
            print(f"Message : {response.status.message}")
            print("-" * 30)
        else:
            print(f"❌ Server Rejected: {response.status.message}")
            
    except grpc.RpcError as e:
        print(f"❌ gRPC Error: {e.code()}")
        print(f"Details: {e.details()}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    parser.add_argument("--ip", default="127.0.0.1")
    parser.add_argument("--port", default="50052")

    args = parser.parse_args()
    upload_file(args.file, args.ip, args.port)