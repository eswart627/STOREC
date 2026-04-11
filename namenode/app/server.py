import grpc
import os
import time
import datetime
import random
import pytz  # Add this import for timezone handling
import uuid

from concurrent.futures import ThreadPoolExecutor

from proto import namenode_pb2
from proto import namenode_pb2_grpc
from proto import common_pb2

from .heartbeat_manager import HeartbeatManager
from .health_checker import HealthChecker

class NameNodeService(
    namenode_pb2_grpc.NameNodeServiceServicer
):
    """
    Name node service.
    
    Attributes:
        registry: Data node registry.
        logger: Logger object.
        heartbeat_manager: Heartbeat manager.
    """
    def __init__(self, registry, logger):
        self.registry = registry
        self.logger = logger
        self.heartbeat_manager = HeartbeatManager(self.logger,self.registry)
    def RegisterDataNode(self, request:namenode_pb2.RegisterRequest, context)->namenode_pb2.RegisterResponse:
        """
        Register a data node with the name node.
        Args:
            request: RegisterRequest(Nodeid(node_id:str,hostname:str,port:int),capacity:int)
            context: grpc.ServicerContext
        
        Returns:
            RegisterResponse(Status(success:bool,message:str))
        """
        node = request.node
        node_id=str(uuid.uuid4())
        self.registry.register(
            node_id=node_id,
            hostname=node.hostname,
            port=node.port,
            capacity=request.capacity_bytes,
        )

        current_time = datetime.datetime.now().strftime("%H:%M:%S")  # Format time as HH:MM:SS
        self.logger.log(
            "REGISTER",
            f"{node_id} at {current_time}",
        )

        return namenode_pb2.RegisterResponse(
            node_id=node_id,
            status=common_pb2.Status(
                success=True,
                message="registered",
            )
        )

    def SendHeartbeat(self, request:namenode_pb2.HeartbeatRequest, context)->namenode_pb2.HeartbeatResponse:
        """
        Send a heartbeat to the name node.
        Args:
            request: HeartbeatRequest(Heartbeat(node_id:str,timestamp:int,used_bytes:int,free_bytes:int))
            context: grpc.ServicerContext
        
        Returns:
            HeartbeatResponse(Status(success:bool,message:str))
        """
        
        return self.heartbeat_manager.handle_heartbeat(request, context)

    def AllocateStripe(self, request:namenode_pb2.AllocateStripeRequest, context)->namenode_pb2.AllocateStripeResponse:
        """
        Under Maintainence
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

    def ReportShard(self, request:namenode_pb2.ReportShardRequest, context)->namenode_pb2.ReportShardResponse:
        """
        Under maintenance
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

        address = f"{config.hostname}:{config.port}"
        
        print(f"DEBUG: NameNode binding to {address}", flush=True)
        self.server.add_insecure_port(address)
        self.health_checker = HealthChecker(
            self.registry, 
            config.health_check_interval,
            self.logger
        )

    def start(self)->None:
        """
        Start the name node server.
        """

        print("Starting gRPC server", flush=True)  # Debug print to indicate gRPC server startup
        self.server.start()

        # Start health checker
        self.health_checker.start()

        current_time = datetime.datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%H:%M")  # Format time as HH:MM in IST
        self.logger.log(
            "SERVER_START",
            f"namenode_{current_time}",
        )

        print("gRPC server started and waiting for termination", flush=True)  # Debug print to indicate server is running
        self.server.wait_for_termination()