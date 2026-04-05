import threading
import time


class DataNodeRegistry:

    def __init__(self):
        self.nodes = {}
        self.lock = threading.Lock()

    def register(self, node_id, hostname, port, capacity):

        with self.lock:

            self.nodes[node_id] = {

                "hostname": hostname,
                "port": port,
                "capacity": capacity,
                "last_heartbeat": time.time(),
            }

    def heartbeat(self, node_id):

        with self.lock:

            if node_id in self.nodes:

                self.nodes[node_id][
                    "last_heartbeat"
                ] = time.time()

    def list_nodes(self):

        with self.lock:

            return dict(self.nodes)