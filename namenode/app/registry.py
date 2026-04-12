import threading
import time
from datetime import datetime
from typing import Any , List

from ..db_manager import get_connection


NodeRecord = dict[str, Any]


def _heartbeat_to_epoch(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, datetime):
        return int(value.timestamp())
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return int(datetime.fromisoformat(value).timestamp())
    return int(value)


class DataNodeRegistry:
    """
    In-memory view of DataNodes.

    The DB keeps the persistent source of truth across NameNode restarts,
    while this registry holds the live runtime state used by RPC handlers.
    """
    def __init__(self):
        self.nodes = {}
        self.lookup={}
        self.lock = threading.Lock()

    def register(self, node_id: str, hostname: str, port: int, capacity: int) -> None:
        """
        Register a data node.
        
        Args:
            node_id: Unique identifier for the node.
            hostname: Hostname of the node.
            port: Port number of the node.
            capacity: Total storage capacity of the node.
        """
        now = int(time.time())
        with self.lock:
            existing_node = self.nodes.get(node_id, {})
            old_address = None
            if existing_node.get("hostname") and existing_node.get("port"):
                old_address = f'{existing_node["hostname"]}:{existing_node["port"]}'

            used_bytes = existing_node.get("used", 0)
            available_bytes = max(capacity - used_bytes, 0)
            self.nodes[node_id] = {
                "hostname": hostname,
                "port": port,
                "capacity": capacity,
                "used": used_bytes,
                "available": available_bytes,
                "last_heartbeat": now,
                "status": "ACTIVE",
            }

            if old_address and old_address != f"{hostname}:{port}":
                self.lookup.pop(old_address, None)
            self.lookup[f"{hostname}:{port}"] = node_id

            conn = get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO dn_table (
                    dn_id, dn_address, dn_port, dn_status,
                    dn_capacity, dn_used, dn_available, dn_last_heartbeat
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s))
                ON DUPLICATE KEY UPDATE
                    dn_address = VALUES(dn_address),
                    dn_port = VALUES(dn_port),
                    dn_status = VALUES(dn_status),
                    dn_capacity = VALUES(dn_capacity),
                    dn_used = VALUES(dn_used),
                    dn_available = VALUES(dn_available),
                    dn_last_heartbeat = VALUES(dn_last_heartbeat)
                """,
                (node_id, hostname, port, "ACTIVE", capacity, used_bytes, available_bytes, now),
            )
            conn.commit()
            conn.close()
    
    def heartbeat(self, node_id: str) -> str | None:
        """
        Update the heartbeat timestamp for an already registered DataNode.
        """
        now = int(time.time())
        with self.lock:
            if node_id not in self.nodes:
                return f"Node {node_id} not found in registry"

            self.nodes[node_id]["last_heartbeat"] = now
            self.nodes[node_id]["status"] = "ACTIVE"

            conn = get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE dn_table
                SET dn_status = %s, dn_last_heartbeat = FROM_UNIXTIME(%s)
                WHERE dn_id = %s
                """,
                ("ACTIVE", now, node_id),
            )
            conn.commit()
            conn.close()
        return None

    def list_nodes(self) -> dict[str, NodeRecord]:
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
        
        with self.lock:
            for node_id, node_data in self.nodes.items():
                if current_time - node_data["last_heartbeat"] > 20 and node_data["status"] == "ACTIVE":
                    dead_nodes.append(node_id)
                    self.nodes[node_id]["status"] = "INACTIVE"
        return dead_nodes

    def save_state(self) -> None:
        """
        Persist the current in-memory registry back to the DB on shutdown.
        """

        with self.lock:
            conn = get_connection()
            cur = conn.cursor()
            for node_id, node_data in self.nodes.items():
                node_data["status"] = "INACTIVE"
                cur.execute(
                    """
                    INSERT INTO dn_table (
                        dn_id, dn_address, dn_port, dn_status,
                        dn_capacity, dn_used, dn_available, dn_last_heartbeat
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s))
                    ON DUPLICATE KEY UPDATE
                        dn_address = VALUES(dn_address),
                        dn_port = VALUES(dn_port),
                        dn_status = VALUES(dn_status),
                        dn_capacity = VALUES(dn_capacity),
                        dn_used = VALUES(dn_used),
                        dn_available = VALUES(dn_available),
                        dn_last_heartbeat = VALUES(dn_last_heartbeat)
                """, (
                    node_id,
                    node_data["hostname"],
                    node_data["port"],
                    "INACTIVE",
                    node_data["capacity"],
                    node_data.get("used", 0),
                    node_data.get("available", node_data["capacity"]),
                    int(node_data["last_heartbeat"]),
                ))
            conn.commit()
            conn.close()

    def load_state(self) -> int:
        """
        Load the state of the registry from the database.
        """
        with self.lock:
            self.nodes.clear()
            self.lookup.clear()
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
                    "capacity": row[4],
                    "used": row[5],
                    "available": row[6],
                    "last_heartbeat": _heartbeat_to_epoch(row[7]),
                    "status": "INACTIVE",
                }
                self.lookup[f"{row[1]}:{row[2]}"] = row[0]
            if rows:
                cur.execute("UPDATE dn_table SET dn_status = 'INACTIVE'")
                conn.commit()
            conn.close()
            return len(rows)
