
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
        #self.heartbeat_manager = HeartbeatManager(self.logger,self.registry)
    def RegisterDataNode(self, request:namenode_pb2.RegisterRequest, context)->namenode_pb2.RegisterResponse:
        """
        Register a data node with the name node.
        Args:
            request: RegisterRequest(Nodeid(node_id:str,hostname:str,port:int),capacity:int)
            context: grpc.ServicerContext
        
        Returns:
            RegisterResponse(Status(success:bool,message:str))
        """
        node_id=request.node_id
        node = request.node
        address=f"{node.hostname}:{node.port}"
        if not node_id:
            if self.registry.lookup.get(address):
                existing_node_id = self.registry.lookup[address]
                self.registry.register(
                    node_id=existing_node_id,
                    hostname=node.hostname,
                    port=node.port,
                    capacity=request.capacity_bytes,
                    mode=1,
                )
                return namenode_pb2.RegisterResponse(
                    node_id=existing_node_id,
                    status=common_pb2.Status(
                        success=True,
                        message="Already registered",
                    )
                )
            else:
                
                node_id=self.registry.register(
                    node_id=None,
                    hostname=node.hostname,
                    port=node.port,
                    capacity=request.capacity_bytes,
                    mode=0,
                )
                return namenode_pb2.RegisterResponse(
                    node_id=node_id,
                    status=common_pb2.Status(
                        success=True,
                        message="Registered",
                    )
                )
        else:
            self.registry.register(
                node_id=node_id,
                hostname=node.hostname,
                port=node.port,
                capacity=request.capacity_bytes,
                mode=1,
            )
            self.logger.log("Datanode with ID ", f"{existing_node_id} at {address} (re-registered)")
            return namenode_pb2.RegisterResponse(
                node_id=node_id,
                status=common_pb2.Status(
                    success=True,
                    message="Heartbeat Recorded",
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
        heartbeat = request.heartbeat
        
        # Log the received heartbeat on NameNode side
        self.logger.log(
            "HEARTBEAT_RECEIVED",
            f"node_id={heartbeat.node_id}, timestamp={heartbeat.timestamp}, "
            f"used_bytes={heartbeat.used_bytes}, free_bytes={heartbeat.free_bytes}"
        )
        
        self.registry.heartbeat(heartbeat.node_id)
        return namenode_pb2.HeartbeatResponse(
            status=common_pb2.Status(
                success=True, 
                message=f"ACK for node {heartbeat.node_id} at {heartbeat.timestamp}"
            )
        )
        
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

    def stop(self, grace_period: int = 10) -> None:
        """
        Stop the name node server with a custom grace period.
        """
        print(f"Stopping NameNode server (grace period: {grace_period}s)...", flush=True)
        
        # 1. Stop background processes first
        self.health_checker.stop()
        
        # 2. Persist the registry state
        self.registry.save_state()
        
        # 3. Stop the gRPC server with the increased grace period
        # The server will stop accepting new requests and wait up to 'grace' seconds
        # for existing RPCs to complete.
        self.server.stop(grace=grace_period)
        
        self.logger.log("SERVER_STOP", f"Graceful shutdown completed ({grace_period}s)")
