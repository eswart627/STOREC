import threading
import time


class DataNodeRegistry:
    """
    Data node registry.
    
    Attributes:
        nodes: Dictionary of registered nodes.
        lock: Lock for thread-safe operations.
    """
    def __init__(self):
        self.nodes = {}
        self.lock = threading.Lock()

    def register(self, node_id, hostname, port, capacity):
        """
        Register a data node.
        
        Args:
            node_id: Unique identifier for the node.
            hostname: Hostname of the node.
            port: Port number of the node.
            capacity: Total storage capacity of the node.
        """
        with self.lock:

            self.nodes[node_id] = {

                "hostname": hostname,
                "port": port,
                "capacity": capacity,
                "last_heartbeat": time.time(),
            }

    def heartbeat(self, node_id):
        """
        Update the last heartbeat time for a data node.
        
        Args:
            node_id: Unique identifier for the node.
        """
        with self.lock:

            if node_id in self.nodes:
                self.nodes[node_id]["last_heartbeat"] = time.time()

    def list_nodes(self):
        """
        List all registered data nodes.
        
        Returns:
            Dictionary of registered nodes.
        """
        with self.lock:

            return dict(self.nodes)