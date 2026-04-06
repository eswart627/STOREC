import grpc
import os
import time
import datetime

from concurrent.futures import ThreadPoolExecutor

from proto import namenode_pb2
from proto import namenode_pb2_grpc
from proto import common_pb2

from namenode.app.heartbeat_manager import HeartbeatManager


class NameNodeService(
    namenode_pb2_grpc.NameNodeServiceServicer
):

    def __init__(
        self,
        registry,
        logger,
    ):
        self.registry = registry
        self.logger = logger
        self.heartbeat_manager = HeartbeatManager(self.logger)

    def RegisterDataNode(
        self,
        request,
        context,
    ):

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

    def SendHeartbeat(
        self,
        request,
        context,
    ):
        return self.heartbeat_manager.handle_heartbeat(request, context)


class NameNodeServer:

    def __init__(
        self,
        config,
        registry,
        logger,
    ):

        self.config = config

        self.registry = registry

        self.logger = logger

        self.server = grpc.server(
            ThreadPoolExecutor(
                max_workers=config.worker_threads
            )
        )

        namenode_pb2_grpc.add_NameNodeServiceServicer_to_server(
            NameNodeService(
                registry,
                logger,
            ),
            self.server,
        )

        address = (
            f"{config.hostname}:"
            f"{config.port}"
        )

        self.server.add_insecure_port(
            address
        )

    def start(self):

        print("Starting gRPC server", flush=True)  # Debug print to indicate gRPC server startup
        self.server.start()

        current_time = datetime.datetime.now().strftime("%H:%M")  # Format time as HH:MM
        self.logger.log(
            "SERVER_START",
            f"namenode_{current_time}",
        )

        print("gRPC server started and waiting for termination", flush=True)  # Debug print to indicate server is running
        self.server.wait_for_termination()