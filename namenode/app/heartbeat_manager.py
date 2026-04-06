from proto import namenode_pb2
from proto import namenode_pb2_grpc

class HeartbeatManager:
    def __init__(self, logger):
        self.logger = logger

    def handle_heartbeat(self, request, context):
        heartbeat = request.heartbeat
        self.logger.log(
            "HEARTBEAT_RECEIVED",
            f"node_id={heartbeat.node_id}, timestamp={heartbeat.timestamp}, used_bytes={heartbeat.used_bytes}, free_bytes={heartbeat.free_bytes}"
        )
        return namenode_pb2.HeartbeatResponse(
            status=namenode_pb2.Status(success=True)
        )