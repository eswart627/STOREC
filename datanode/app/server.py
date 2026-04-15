import os
import grpc
import time
from concurrent import futures
from proto import datanode_pb2
from proto import datanode_pb2_grpc
from proto import common_pb2

class DataNodeService(datanode_pb2_grpc.DataNodeServiceServicer):
    def __init__(self,config,storage, logger):
        self.config = config
        self.storage = storage
        self.logger = logger

    def WriteBlock(self, request_iterator, context):
        block_id = None
        tmp_path = None
        final_path = None
        start_time = time.time()
        
        try:
            f = None
            for request in request_iterator:
                block = request.block
                
                # Setup paths on the very first chunk
                if block_id is None:
                    block_id = block.block_id
                    tmp_path = os.path.join(self.storage.tmp_dir, f"{block_id}.tmp")
                    final_path = os.path.join(self.storage.chunks_dir, block_id)
                    f = open(tmp_path, "wb")
                    self.logger.log("RPC_RECEIVE", f"Streaming started for: {block_id}")

                f.write(block.data_bytes)
            
            if f:
                f.close()
                # Atomically move from tmp to chunks
                os.rename(tmp_path, final_path)
                end_time = time.time()
                duration = end_time - start_time
                self.logger.log("WRITE_SUCCESS", f"Block {block_id} stored in volume. in {duration:.4f} seconds")

            # Prepare response
            node_info = common_pb2.Node(hostname=self.config.hostname, port=self.config.port)
            node_id_wrapper = common_pb2.NodeId(node_id=self.config.node_id, node=node_info)

            return datanode_pb2.WriteBlockResponse(
                status=common_pb2.Status(success=True, message="Block stored successfully"),
                node=node_id_wrapper,
                block_id=block_id
            )
        except Exception as e:
            if 'f' in locals() and f: f.close()
            if tmp_path and os.path.exists(tmp_path): os.remove(tmp_path)
            
            self.logger.log("WRITE_ERROR", str(e))
            return datanode_pb2.WriteBlockResponse(
                status=common_pb2.Status(success=False, message=str(e))
            )

    def ReadBlock(self, request, context):
        block_path = os.path.join(self.storage.chunks_dir, request.block_id)
        if not os.path.exists(block_path):
            return datanode_pb2.ReadBlockResponse(
                status=common_pb2.Status(success=False, message="Block not found")
            )
        
        with open(block_path, "rb") as f:
            data = f.read()
            
        return datanode_pb2.ReadBlockResponse(
            status=common_pb2.Status(success=True),
            block=common_pb2.Block(block_id=request.block_id, size_bytes=len(data), data_bytes=data)
        )

    def DeleteBlock(self, request, context):
        block_path = os.path.join(self.storage.chunks_dir, request.block_id)
        if os.path.exists(block_path):
            os.remove(block_path)
        return datanode_pb2.DeleteBlockResponse(status=common_pb2.Status(success=True))
    
    
class DataNodeServer:
    def __init__(self, config, logger, storage):
        self.config = config
        self.logger = logger
        self.storage = storage
        self.server = None

    def start(self):
        # We use the thread pool count from your config (e.g., 10 threads)
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=self.config.worker_threads))
        datanode_pb2_grpc.add_DataNodeServiceServicer_to_server(
            DataNodeService(self.config,self.storage, self.logger), self.server
        )
        
        # [::] allows connections from any PC on the network
        address = f"[::]:{self.config.port}"
        self.server.add_insecure_port(address)
        self.server.start()
        
        self.logger.log("SERVER_START", f"Listening on {address}")
        print(f"DataNode gRPC Server active on {address}", flush=True)

    def stop(self):
        if self.server:
            self.server.stop(0)