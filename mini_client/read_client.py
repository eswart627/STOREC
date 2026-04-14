import grpc
import sys
import os
from pathlib import Path

# Add project root to sys.path to allow imports from proto
project_root = str(Path(__file__).resolve().parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from proto import namenode_pb2
from proto import namenode_pb2_grpc
from proto import common_pb2

def get_file_metadata(host='192.168.1.166', port=50051, file_name='test_file.txt'):
    address = f"{host}:{port}"
    print(f"Connecting to NameNode at {address}...")
    
    channel = grpc.insecure_channel(address)
    stub = namenode_pb2_grpc.NameNodeServiceStub(channel)
    
    print(f"\n--- Getting Metadata for {file_name} ---")
    
    metadata_request = namenode_pb2.GetFileMetadataRequest(
        file_details=common_pb2.File(
            file_name=file_name,
            file_size=1024  # Size not needed for metadata request
        )
    )
    
    try:
        metadata_response = stub.GetFileMetadata(metadata_request)
        print(f"GetFileMetadata Status: Success=True")
        print(f"File: {metadata_response.file_details.file_name}")
        print(f"File Size: {metadata_response.file_details.file_size} bytes")
        print(f"Stripe Size: {metadata_response.stripe_size} bytes")
        print(f"Data Blocks (k): {metadata_response.data_blocks_k}")
        print(f"Parity Blocks (m): {metadata_response.parity_blocks_m}")
        print(f"Number of Block Groups: {len(metadata_response.block_groups)}")
        
        print("\n--- Block Groups ---")
        for i, block_group in enumerate(metadata_response.block_groups):
            print(f"Stripe ID: {block_group.stripe_id}")
            for placement in block_group.placement:
                print(f"  Block ID: {placement.block_id} -> Node: {placement.node.node_id} ({placement.node.node.hostname}:{placement.node.node.port})")
        
    except grpc.RpcError as e:
        print(f"RPC failed: {e.code()} - {e.details()}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Read file metadata from NameNode.")
    parser.add_argument("--host", default="192.168.1.166", help="NameNode hostname (default: 192.168.1.166)")
    parser.add_argument("--port", default="50051", help="NameNode port (default: 50051)")
    parser.add_argument("--file", default="test_file.txt", help="File name to read metadata for (default: test_file.txt)")
    
    args = parser.parse_args()
    get_file_metadata(host=args.host, port=args.port, file_name=args.file)
