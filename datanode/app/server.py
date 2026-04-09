# import time
# import threading

# class DataNodeServer:
#     def __init__(self, config, logger, storage):
#         self.config = config
#         self.logger = logger
#         self.storage = storage
#         self.running = False
    
#     def start(self):
#         self.running = True
#         thread = threading.Thread(target=self._run_server, daemon=True)
#         thread.start()
#         self.logger.log("SERVER_START", f"port={self.config.port}")

#     def _run_server(self):
#         while self.running:
#             time.sleep(1)
#     def stop(self):
#         self.running = False
#         self.logger.log("SERVER_STOP", "graceful")

import grpc
from concurrent import futures
from proto import datanode_pb2
from proto import datanode_pb2_grpc
from proto import common_pb2

class DataNodeService(datanode_pb2_grpc.DataNodeServiceServicer):
    def __init__(self, storage, logger):
        self.storage = storage
        self.logger = logger

    def WriteShard(self, request, context):
        # The infrastructure is now ready to receive data
        self.logger.log("RPC_RECEIVE", f"WriteShard request for Stripe: {request.shard.stripe_id}")
        return datanode_pb2.WriteShardResponse(status=common_pb2.Status(success=True, message="Infrastructure ACK"))

    def ReadShard(self, request, context):
        self.logger.log("RPC_RECEIVE", f"ReadShard request for Stripe: {request.shard.stripe_id}")
        return datanode_pb2.ReadShardResponse(status=common_pb2.Status(success=True), data=b"")

    def DeleteShard(self, request, context):
        return datanode_pb2.DeleteShardResponse(status=common_pb2.Status(success=True))

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
            DataNodeService(self.storage, self.logger), self.server
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