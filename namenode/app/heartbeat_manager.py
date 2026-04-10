from logging import Logger
from proto import namenode_pb2
from proto import common_pb2 
import time
from typing import List
from .registry import DataNodeRegistry

class HeartbeatManager:
    """
    Heartbeat manager for data nodes.
    Args:
        logger: Logger object
        registry: Registry object
    """
    def __init__(self, logger:Logger, registry:DataNodeRegistry):
        self.logger = logger
        self.registry = registry

    def handle_heartbeat(self, request:namenode_pb2.HeartbeatRequest, context)-> namenode_pb2.HeartbeatResponse:
        """
        Handle heartbeat from data node.
        Args:
            request: Heartbeat request
            context: gRPC context
        Returns:
            Heartbeat response
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
    