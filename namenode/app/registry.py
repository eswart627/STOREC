from math import e
import threading
import time
from typing import List

from ..db_manager import get_connection


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

    def register(self, node_id:str, hostname:str, port:int, capacity:int)->None:
        """
        Register a data node.
        
        Args:
            node_id: Unique identifier for the node.
            hostname: Hostname of the node.
            port: Port number of the node.
            capacity: Total storage capacity of the node.
        """
        conn = get_connection()
        cur = conn.cursor()
        
        if node_id in self.nodes:
            self.nodes[node_id]["status"] ="ACTIVE"
            self.nodes[node_id]["last_heartbeat"] = time.time()
            cur.execute("""
                UPDATE dn_table 
                SET dn_status = %s, 
                    dn_last_heartbeat = %s 
                WHERE dn_id = %s
            """, ("ACTIVE", time.time(), node_id))
        else:
            with self.lock:
                self.nodes[node_id] = {
                    "hostname": hostname,
                    "port": port,
                    "capacity": capacity,
                    "last_heartbeat": time.time(),
                }

                cur.execute("""
                    INSERT INTO dn_table (dn_id, dn_address, dn_port, dn_status, dn_capacity, dn_used, dn_available)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (node_id, hostname, port, "ACTIVE", capacity, 0, capacity))
        conn.commit()
        conn.close()
    
    def heartbeat(self, node_id:str)->None:
        """
        Update the last heartbeat time for a data node.
        
        Args:
            node_id: Unique identifier for the node.
        """
        with self.lock:

            if node_id in self.nodes:
                self.nodes[node_id]["last_heartbeat"] = time.time()
            else:
                return f"Node {node_id} not found in registry"

    def list_nodes(self)->dict:
        """
        List all registered data nodes.
        
        Returns:
            Dictionary of registered nodes.
        """
        with self.lock:
            return dict(self.nodes)
    
    def check_node_health(self) -> List[int]:
        """
        Background task to detect dead nodes.
        Returns:
            List of dead node IDs.
        """
        current_time = time.time()
        dead_nodes:List[int] = []
        
        for node_id, node_data in self.nodes.items():
            if current_time - node_data["last_heartbeat"] > 20:  # 3 missed heartbeats
                dead_nodes.append(node_id)
                self.nodes[node_id]["status"] = "INACTIVE"
        return dead_nodes

    def save_state(self) -> None:
        """
        Save the current state of the registry to the database.
        """

        with self.lock:
            conn = get_connection()
            cur = conn.cursor()
            for node_id, node_data in self.nodes.items():
                cur.execute("""
                    UPDATE dn_table 
                    SET dn_status = %s, 
                        dn_last_heartbeat = %s 
                    WHERE dn_id = %s
                """, ("INACTIVE", node_data["last_heartbeat"], node_id))
            conn.commit()
            conn.close()

    def load_state(self):
        """
        Load the state of the registry from the database.
        """
        with self.lock:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT dn_id, dn_address, dn_port, dn_status, dn_capacity, dn_used, dn_available, dn_last_heartbeat
                FROM dn_table
            """)
            rows = cur.fetchall()
            for row in rows:
                self.nodes[row[0]] = {
                    "hostname": row[1],
                    "port": row[2],
                    "capacity": row[3],
                    "last_heartbeat": row[7],
                    "status": row[4],
                }
            conn.close()