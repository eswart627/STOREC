import grpc
import os
import argparse
import sys

# Handling imports for module-style execution
try:
    from proto import datanode_pb2, datanode_pb2_grpc
except ImportError:
    # Fallback if not running as a module
    import proto.datanode_pb2 as datanode_pb2
    import proto.datanode_pb2_grpc as datanode_pb2_grpc

def download_file(block_id, ip_address, port):
    server_addr = f"{ip_address}:{port}"
    
    # Increase message length limits for the channel
    options = [
        ('grpc.max_send_message_length', 100 * 1024 * 1024),
        ('grpc.max_receive_message_length', 100 * 1024 * 1024),
    ]
    
    channel = grpc.insecure_channel(server_addr, options=options)
    stub = datanode_pb2_grpc.DataNodeServiceStub(channel)

    print(f"📡 Connecting to DataNode at {server_addr}")
    print(f"📥 Requesting: {block_id}")

    try:
        request = datanode_pb2.ReadBlockRequest(block_id=block_id)
        
        # This is now an ITERATOR because the server is streaming
        response_iterator = stub.ReadBlock(request)

        output_name = f"recovered_{block_id}"
        total_bytes = 0
        
        with open(output_name, "wb") as f:
            for response in response_iterator:
                if response.status.success:
                    f.write(response.block.data_bytes)
                    total_bytes += len(response.block.data_bytes)
                    # Simple progress indicator for large files
                    print(f"\rReceived: {total_bytes / (1024*1024):.2f} MB", end="")
                else:
                    print(f"\n❌ Server Error: {response.status.message}")
                    if os.path.exists(output_name):
                        os.remove(output_name)
                    return

        print(f"\n" + "-" * 30)
        print(f"✅ READ SUCCESS!")
        print(f"File saved as: {output_name}")
        print(f"Final Size: {total_bytes} bytes")
        print("-" * 30)

    except grpc.RpcError as e:
        print(f"\n❌ gRPC Error: {e.code()} - {e.details()}")
    except Exception as e:
        print(f"\n❌ Unexpected Error: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DataNode Read Client")
    parser.add_argument("file_id", help="The name/ID of the block (e.g., ISS-.pdf)")
    parser.add_argument("--ip", default="127.0.0.1", help="DataNode IP address")
    parser.add_argument("--port", default="50052", help="DataNode Port")

    args = parser.parse_args()
    download_file(args.file_id, args.ip, args.port)