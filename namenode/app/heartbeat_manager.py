from proto import namenode_pb2
from proto import common_pb2 

class HeartbeatManager:
    def __init__(self, logger):
        self.logger = logger

    def handle_heartbeat(self, request, context):
        heartbeat = request.heartbeat
        
        # Log the received heartbeat on NameNode side
        self.logger.log(
            "HEARTBEAT_RECEIVED",
            f"node_id={heartbeat.node_id}, timestamp={heartbeat.timestamp}, "
            f"used_bytes={heartbeat.used_bytes}, free_bytes={heartbeat.free_bytes}"
        )
        
        # Send back a response with a status message
        return namenode_pb2.HeartbeatResponse(
            status=common_pb2.Status(
                success=True, 
                message=f"ACK for node {heartbeat.node_id} at {heartbeat.timestamp}"
            )
        )