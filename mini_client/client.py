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

def test_namenode_methods(host='localhost', port=50051):
    address = f"{host}:{port}"
    print(f"Connecting to NameNode at {address}...")
    
    channel = grpc.insecure_channel(address)
    stub = namenode_pb2_grpc.NameNodeServiceStub(channel)
    
    file_name = "text_file.txt"
    file_size = 1024 * 1024  # 1 MB
    
    # 1. Ask for DataNodes (AllocateBlocks)
    print(f"\n--- Requesting AllocateBlocks for {file_name} ---")
    allocate_request = namenode_pb2.AllocateBlocksRequest(
        file_details=common_pb2.File(
            file_name=file_name,
            file_size=file_size
        ),
        stripe_size=64 * 1024, # 64 KB stripes
        data_blocks_k=6,
        parity_blocks_m=3
    )
    
    try:
        allocate_response = stub.AllocateBlocks(allocate_request)
        print("AllocateBlocks Response received.")
        
        all_block_ids = []
        for bg in allocate_response.block_groups:
            print(f"Stripe ID: {bg.stripe_id}")
            for placement in bg.placement:
                print(f"  Block ID: {placement.block_id} -> Node: {placement.node.node_id} ({placement.node.node.hostname}:{placement.node.node.port})")
                all_block_ids.append(placement.block_id)
        
        # 2. Send CommitFile request
        if all_block_ids:
            print(f"\n--- Committing File {file_name} with {len(all_block_ids)} blocks ---")
            commit_request = namenode_pb2.CommitFileRequest(
                file_details=common_pb2.File(
                    file_name=file_name,
                    file_size=file_size
                ),
                total_blocks=len(all_block_ids),
                block_ids=all_block_ids
            )
            
            commit_response = stub.CommitFile(commit_request)
            print(f"CommitFile Status: Success={commit_response.status.success}, Message='{commit_response.status.message}'")
        else:
            print("\nNo blocks were allocated. Skipping CommitFile.")
            
    except grpc.RpcError as e:
        print(f"RPC failed: {e.code()} - {e.details()}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Mini Client to test NameNode methods.")
    parser.add_argument("--host", default="192.168.1.166", help="NameNode hostname (default: 192.168.1.166)")
    parser.add_argument("--port", default="50051", help="NameNode port (default: 50051)")
    
    args = parser.parse_args()
    test_namenode_methods(host=args.host, port=args.port)
