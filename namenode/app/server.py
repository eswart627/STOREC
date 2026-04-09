import grpc
import os
import time
import datetime

from concurrent.futures import ThreadPoolExecutor

from proto import namenode_pb2
from proto import namenode_pb2_grpc
from proto import common_pb2

from namenode.app.heartbeat_manager import HeartbeatManager

import random
import pytz  # Add this import for timezone handling

class NameNodeService(
    namenode_pb2_grpc.NameNodeServiceServicer
):

    def __init__(self, registry, logger):
        self.registry = registry
        self.logger = logger
        self.heartbeat_manager = HeartbeatManager(self.logger)

    def RegisterDataNode(self, request, context):
        """
        Register a data node with the name node.
        Args:
            request: RegisterRequest(Nodeid(node_id:str,hostname:str,port:int),capacity:int)
            context: grpc.ServicerContext
        
        Returns:
            RegisterResponse(Status(success:bool,message:str))
        """
        node = request.node

        self.registry.register(
            node_id=node.node_id,
            hostname=node.hostname,
            port=node.port,
            capacity=request.capacity_bytes,
        )

        current_time = datetime.datetime.now().strftime("%H:%M:%S")  # Format time as HH:MM:SS
        self.logger.log(
            "REGISTER",
            f"{node.node_id} at {current_time}",
        )

        return namenode_pb2.RegisterResponse(
            status=common_pb2.Status(
                success=True,
                message="registered",
            )
        )

    def SendHeartbeat(self, request, context):
        """
        Send a heartbeat to the name node.
        Args:
            request: HeartbeatRequest(Heartbeat(node_id:str,timestamp:int,used_bytes:int,free_bytes:int))
            context: grpc.ServicerContext
        
        Returns:
            HeartbeatResponse(Status(success:bool,message:str))
        """
        
        return self.heartbeat_manager.handle_heartbeat(request, context)

    def AllocateStripe(self, request, context):
        """
        Allocate a stripe to a file.
        Args:
            request: AllocateStripeRequest(file_name:str,policy(data_shards:int,parity_shards:int,stripe_size:int))
            context: grpc.ServicerContext
        
        Returns:
            AllocateStripeResponse()
        """
        required_size:int=request.policy.stripe_size
        node:dict = random.sample([node for node in self.registry.nodes if self.registry.nodes[node]["capacity"] >= required_size], 1)[0]


        return namenode_pb2.AllocateStripeResponse(
            status=common_pb2.Status(
                success=True
            )
            )

    def ReportShard(self, request, context):
        """
        Args:
            request: ReportShardRequest()
            context: grpc.ServicerContext
        
        Returns:
            ReportShardResponse()
        
        """
        return namenode_pb2.ReportShardResponse(
            status=common_pb2.Status(
                success=True
            )
        )

class NameNodeServer:
    """
    Name node server.
    
    Attributes:
        config: Configuration object.
        registry: Node registry object.
        logger: Logger object.
        server: gRPC server.
    """
    def __init__(self, config, registry, logger):

        self.config = config

        self.registry = registry

        self.logger = logger

        self.server = grpc.server(
            ThreadPoolExecutor(
                max_workers=config.worker_threads
            )
        )

        namenode_pb2_grpc.add_NameNodeServiceServicer_to_server(
            NameNodeService(registry, logger), self.server
        )

        #address = f"{config.hostname}:{config.port}"
        address = "0.0.0.0:50051"
        print(f"DEBUG: NameNode binding to {address}", flush=True)
        self.server.add_insecure_port(address)

    def start(self):
        """
        Start the name node server.
        """

        print("Starting gRPC server", flush=True)  # Debug print to indicate gRPC server startup
        self.server.start()

        current_time = datetime.datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%H:%M")  # Format time as HH:MM in IST
        self.logger.log(
            "SERVER_START",
            f"namenode_{current_time}",
        )

        print("gRPC server started and waiting for termination", flush=True)  # Debug print to indicate server is running
        self.server.wait_for_termination()