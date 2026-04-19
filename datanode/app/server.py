import os
import grpc
import time
from concurrent import futures

from numpy import block

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
        f = None
        try:
            for request in request_iterator:
                block = request.block
                # Setup paths on the very first chunk
                if block_id is None:
                    block_id = block.block_id
                    incoming_size = block.block_size
                    used = self.storage.get_used_bytes()
                    if(used + incoming_size > self.config.capacity_bytes):
                        self.logger.log(
                                "WRITE_REJECTED",
                                f"Capacity exceeded "
                                f"for block {block_id}"
                            )
                        return (datanode_pb2.WriteBlockResponse(
                                        status=
                                        common_pb2.Status(
                                            success=False,
                                            message=
                                            "Insufficient storage capacity"
                                        )
                                    )
                                )
                tmp_path = os.path.join(self.storage.tmp_dir, f"{block_id}.tmp")
                final_path = os.path.join(self.storage.chunks_dir, block_id)
                f = open(tmp_path, "wb")
                self.logger.log("RPC_RECEIVE", f"Streaming started for: {block_id}")
                f.write(block.data_bytes)
                
            if f:
                f.close()
                os.replace(tmp_path, final_path)
                duration = time.time() - start_time
                self.logger.log("WRITE_SUCCESS", f"Block {block_id} stored in volume. in {duration:.4f} seconds")

            node_info = common_pb2.Node(
                hostname = self.config.hostname,
                port = self.config.port
            )

            node_id_wrapper = common_pb2.NodeId(
                node_id = self.config.node_id,
                node = node_info
            )

            return datanode_pb2.WriteBlockResponse(
                status=common_pb2.Status(
                    success=True,
                    message="Block stored successfully"
                ),
                node = node_id_wrapper,
                block_id = block_id
            )
        except Exception as e:
            if f:
                f.close()
            if (tmp_path and os.path.exists(tmp_path)):
                os.remove(tmp_path)
            self.logger.log("WRITE ERROR", str(e))
            return datanode_pb2.WriteBlockResponse(
                status=common_pb2.Status(success=False, message=str(e))
            )

    def ReadBlock(self, request, context):
        block_path = os.path.join(self.storage.chunks_dir, request.block_id)
    
        if not os.path.exists(block_path):
           # You can yield a single error response and return
            yield datanode_pb2.ReadBlockResponse(
                status=common_pb2.Status(success=False, message="Block not found")
             )
            return
        self.logger.log(
                "READ_REQUEST",
                f"Block {request.block_id}"
            )

        CHUNK_SIZE = 1024 * 1024  # 1MB chunks
    
        try:
           with open(block_path, "rb") as f:
              while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                
                # We yield each chunk individually
                yield datanode_pb2.ReadBlockResponse(
                    status=common_pb2.Status(success=True),
                    block=common_pb2.Block(
                        block_id=request.block_id,  
                        data_bytes=chunk,
                        block_size=len(chunk)
                    )
                )
        except Exception as e:
            self.logger.log("READ_ERROR", str(e))
        
    def DeleteBlock(self, request, context):
        block_path = os.path.join(self.storage.chunks_dir, request.block_id)
        if os.path.exists(block_path):
            os.remove(block_path)
            self.logger.log(
                "DELETE_SUCCESS",
                request.block_id
            )
        else:
            self.logger.log(
                "DELETE_MISS",
                request.block_id
            )
        return datanode_pb2.DeleteBlockResponse(status=common_pb2.Status(success=True))
    
    
class DataNodeServer:
    def __init__(self, config, logger, storage):
        self.config = config
        self.logger = logger
        self.storage = storage
        self.server = None

    def start(self):
        self.server = grpc.server(
            futures.ThreadPoolExecutor(
                max_workers=self.config.worker_threads
            ),
            options=[
                ("grpc.max_send_message_length", self.config.grpc_max_message),
                ("grpc.max_receive_message_length", self.config.grpc_max_message)
            ]
        )
        datanode_pb2_grpc.add_DataNodeServiceServicer_to_server(
            DataNodeService(self.config,self.storage, self.logger), self.server
        )

        address = f"[::]:{self.config.port}"
        self.server.add_insecure_port(address)
        self.server.start()
        self.logger.log("SERVER_START", f"Listening on {address}")
        print(f"DataNode gRPC Server active on {address}", flush=True)

    def stop(self):
        if self.server:
            self.server.stop(0)