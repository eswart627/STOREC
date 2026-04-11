import threading
import time
from typing import Any

from ..db_manager import get_connection


NodeRecord = dict[str, Any]


class DataNodeRegistry:
    """
    In-memory view of DataNodes.

    The DB keeps the persistent source of truth across NameNode restarts,
    while this registry holds the live runtime state used by RPC handlers.
    """

    def __init__(self) -> None:
        self.nodes: dict[str, NodeRecord] = {}
        self.lock = threading.Lock()

    def register(self, node_id: str, hostname: str, port: int, capacity: int) -> None:
        """
        Register or refresh a DataNode in memory and in the DB.
        """
        now = int(time.time())
        with self.lock:
            self.nodes[node_id] = {
                "hostname": hostname,
                "port": port,
                "capacity": capacity,
                "last_heartbeat": now,
                "status": "ACTIVE",
            }

            conn = get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO dn_table (
                    dn_id, dn_address, dn_port, dn_status, dn_capacity,
                    dn_used, dn_available, dn_last_heartbeat
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    dn_address = VALUES(dn_address),
                    dn_port = VALUES(dn_port),
                    dn_status = VALUES(dn_status),
                    dn_capacity = VALUES(dn_capacity),
                    dn_used = VALUES(dn_used),
                    dn_available = VALUES(dn_available),
                    dn_last_heartbeat = VALUES(dn_last_heartbeat)
                """,
                (node_id, hostname, port, "ACTIVE", capacity, 0, capacity, now),
            )
            conn.commit()
            conn.close()

    def load_from_db(self) -> int:
        """
        Rebuild the in-memory registry from the persistent DB before the server
        starts accepting DataNode RPCs.
        """
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT dn_id, dn_address, dn_port, dn_status, dn_capacity,
                   dn_last_heartbeat
            FROM dn_table
            """
        )
        rows = cur.fetchall()
        conn.close()

        restored_nodes: dict[str, NodeRecord] = {}
        for row in rows:
            node_id, hostname, port, status, capacity, last_heartbeat = row
            restored_nodes[node_id] = {
                "hostname": hostname,
                "port": int(port),
                "capacity": int(capacity),
                "last_heartbeat": int(last_heartbeat),
                "status": status,
            }

        with self.lock:
            self.nodes = restored_nodes

        return len(restored_nodes)

    def has_node(self, node_id: str) -> bool:
        with self.lock:
            return node_id in self.nodes

    def reactivate(self, node_id: str) -> None:
        """
        Mark a known DataNode active again after it reconnects.
        """
        now = int(time.time())
        with self.lock:
            if node_id not in self.nodes:
                return

            self.nodes[node_id]["status"] = "ACTIVE"
            self.nodes[node_id]["last_heartbeat"] = now

            conn = get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE dn_table
                SET dn_status = %s, dn_last_heartbeat = %s
                WHERE dn_id = %s
                """,
                ("ACTIVE", now, node_id),
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
                SET dn_status = %s, dn_last_heartbeat = %s
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

    def check_node_health(self) -> list[str]:
        current_time = int(time.time())
        dead_nodes: list[str] = []

        with self.lock:
            for node_id, node_data in self.nodes.items():
                if (
                    node_data["status"] == "ACTIVE"
                    and current_time - node_data["last_heartbeat"] > 20
                ):
                    self.nodes[node_id]["status"] = "INACTIVE"
                    dead_nodes.append(node_id)
        return dead_nodes

    def save_state(self) -> None:
        """
        Persist the current in-memory registry back to the DB on shutdown.
        """
        with self.lock:
            conn = get_connection()
            cur = conn.cursor()
            for node_id, node_data in self.nodes.items():
                cur.execute(
                    """
                    UPDATE dn_table
                    SET dn_status = %s, dn_last_heartbeat = %s
                    WHERE dn_id = %s
                    """,
                    (
                        node_data["status"],
                        int(node_data["last_heartbeat"]),
                        node_id,
                    ),
                )
            conn.commit()
            conn.close()
