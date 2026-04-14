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

def delete_file(host='192.168.1.166', port=50051, file_name='test_file.txt'):
    address = f"{host}:{port}"
    print(f"Connecting to NameNode at {address}...")
    
    channel = grpc.insecure_channel(address)
    stub = namenode_pb2_grpc.NameNodeServiceStub(channel)
    
    print(f"\n--- Deleting File {file_name} ---")
    
    delete_request = namenode_pb2.DeleteFileRequest(
        file_details=common_pb2.File(
            file_name=file_name,
            file_size=0  # Size not needed for delete
        )
    )
    
    try:
        delete_response = stub.DeleteFile(delete_request)
        print(f"DeleteFile Status: Success={delete_response.status.success}, Message='{delete_response.status.message}'")
        
    except grpc.RpcError as e:
        print(f"RPC failed: {e.code()} - {e.details()}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Delete file from NameNode.")
    parser.add_argument("--host", default="192.168.1.166", help="NameNode hostname (default: 192.168.1.166)")
    parser.add_argument("--port", default="50051", help="NameNode port (default: 50051)")
    parser.add_argument("--file", default="test_file.txt", help="File name to delete (default: test_file.txt)")
    
    args = parser.parse_args()
    delete_file(host=args.host, port=args.port, file_name=args.file)
