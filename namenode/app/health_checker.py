import threading
import time
from ..db_manager import get_connection
from .registry import DataNodeRegistry
from .logger import Logger

class HealthChecker:
    """
    Health checker for data nodes.
    Args:
        registry: Registry object
        check_interval: Interval in seconds to check node health
        logger: Logger object
    """
    def __init__(self, registry:DataNodeRegistry, check_interval:int,logger:Logger):
        """
        Initialize the health checker.
        
        Args:
            registry: Registry object
            check_interval: Interval in seconds to check node health
            logger: Logger object
        """
        self.registry = registry
        self.logger= logger
        self.running = False
        self.check_interval = check_interval
        
    def start(self)->None:
        """
        Start the health checker.
        """
        self.running = True
        threading.Thread(target=self._check_loop, daemon=True).start()
    
    def _check_loop(self)->None:
        """
        Periodically checks the health of the data nodes and cleans up the inactive nodes:
        """
        while self.running:
            dead_nodes = self.registry.check_node_health()
            if dead_nodes:
                for i in dead_nodes:
                    conn= get_connection()
                    cur=conn.cursor()
                    cur.execute("UPDATE dn_table SET dn_status = 'INACTIVE' WHERE dn_id = %s", (i,))
                    conn.commit()
                    conn.close()
                    self.logger.log("NODE_INACTIVE", f"Node {i} marked as inactive")
            time.sleep(self.check_interval)