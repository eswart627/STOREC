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
<<<<<<< Updated upstream
        start_time = time.time()
        
=======
        
        # Latency timer (Total RPC time)
        rpc_start_time = time.time()
        
        #start_time = time.time()
        
        # Throughput timer (Pure Disk I/O time)
        total_disk_io_time = 0
        total_bytes_written = 0
        
        f = None
>>>>>>> Stashed changes
        try:
            f = None
            for request in request_iterator:
                block = request.block
                
                # Setup paths on the very first chunk
                if block_id is None:
                    block_id = block.block_id
<<<<<<< Updated upstream
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
=======
                    incoming_size = block.block_size
                    used = self.storage.get_used_bytes()
                    if(used + incoming_size > self.config.capacity_bytes):
                        self.logger.log(
                                "WRITE_REJECTED",
                                f"Capacity exceeded "
                                f"for block {block_id}",
                                is_throughput=True
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
                self.logger.log("RPC_RECEIVE", f"Streaming started for: {block_id}", is_throughput=True)
                
                # Measure DISK I/O ONLY
                io_start = time.time()
                f.write(block.data_bytes)
                f.flush()
                os.fsync(f.fileno()) # Force physical write
                # --- STOP MEASURING DISK I/O ---
                total_disk_io_time += (time.time() - io_start) 
                total_bytes_written += len(block.data_bytes)
                
            if f:
                f.close()
                os.replace(tmp_path, final_path)
                total_latency = time.time() - rpc_start_time
                #duration = time.time() - start_time
                size_mb = total_bytes_written / (1024 * 1024)
                write_throughput = size_mb / total_disk_io_time if total_disk_io_time > 0 else 0
                
                self.logger.log("WRITE_METRICS",
                                f"Block {block_id} stored in volume | Latency: {total_latency:.4f}s | "
                                f"Write Throughput(Disk Speed): {write_throughput:.2f} MB/s",
                                is_throughput=True)

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
                block_id = block_id,
                
                metrics = common_pb2.Metrics(
                latency_seconds = total_latency,
                throughput_mbs = write_throughput
                )  
            )
        except Exception as e:
            if f:
                f.close()
            if (tmp_path and os.path.exists(tmp_path)):
                os.remove(tmp_path)
            self.logger.log("WRITE ERROR", str(e),is_throughput=True)
>>>>>>> Stashed changes
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
                        size_bytes=len(chunk), 
                        data_bytes=chunk
                    )
                )
        except Exception as e:
            self.logger.log("READ_ERROR", str(e))
        
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