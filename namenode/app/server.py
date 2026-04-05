import grpc

from concurrent.futures import ThreadPoolExecutor

from proto import namenode_pb2
from proto import namenode_pb2_grpc
from proto import common_pb2


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

        self.logger.log(
            "REGISTER",
            node.node_id,
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

        hb = request.heartbeat

        self.registry.heartbeat(
            hb.node_id
        )

        return namenode_pb2.HeartbeatResponse(
            status=common_pb2.Status(
                success=True
            )
        )


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

        self.server.start()

        self.logger.log(
            "SERVER_START",
            "namenode",
        )

        self.server.wait_for_termination()