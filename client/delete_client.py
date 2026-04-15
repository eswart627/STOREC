import grpc
import argparse
from proto import datanode_pb2, datanode_pb2_grpc

def delete_file(block_id, ip_address, port):
    server_addr = f"{ip_address}:{port}"
    channel = grpc.insecure_channel(server_addr)
    stub = datanode_pb2_grpc.DataNodeServiceStub(channel)

    print(f"Connecting to DataNode at {server_addr} to delete: {block_id}")

    try:
        request = datanode_pb2.DeleteBlockRequest(block_id=block_id)
        response = stub.DeleteBlock(request)

        if response.status.success:
            print("-" * 30)
            print(f"✅ DELETE SUCCESS!")
            print(f"Block {block_id} removed from DataNode volume.")
            print("-" * 30)
        else:
            print(f"❌ Server Error: {response.status.message}")

    except grpc.RpcError as e:
        print(f"❌ gRPC Error: {e.code()}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file_id", help="The name/ID of the block to delete")
    parser.add_argument("--ip", default="127.0.0.1")
    parser.add_argument("--port", default="50052")

    args = parser.parse_args()
    delete_file(args.file_id, args.ip, args.port)