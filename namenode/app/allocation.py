import uuid
import threading
from typing import List, Tuple
from proto import common_pb2
from proto import namenode_pb2
from .registry import DataNodeRegistry
from .logger import Logger

class AllocationManager:
    """
    AllocationManager is responsible for allocating blocks to data nodes.
    
    args:
        registry: DataNodeRegistry - The registry of data nodes
        logger: Logger - The logger for logging allocation events
    """
    def __init__(self,registry:DataNodeRegistry, logger:Logger):
        """
        Initialize the allocation manager.
        
        attributes:
            registry: DataNodeRegistry - The registry of data nodes
            logger: Logger - The logger for logging allocation events
            lock: threading.Lock - The lock for thread-safe operations
            cursor: int - The cursor for round-robin allocation
            allocations: dict - The dictionary of allocations
        """
        self.allocations={}
        self.registry=registry
        self.logger=logger
        self.lock = threading.Lock()
        self.cursor = 0

    def get_nodes(self,block_size)->List[str]:
        """
        Get the list of available nodes for allocation.
        
        args:
            block_size: int - The size of the block to allocate
            
        returns:
            list[str] - The list of available node IDs
        """
        available_nodes: List[str] = []
        self.logger.log("DEBUG", f"Looking for nodes with block_size={block_size}")
        self.logger.log("DEBUG", f"Total nodes in registry: {len(self.registry.nodes)}")
        
        for node_id, node_data in self.registry.nodes.items():
            status_ok = node_data['status'] == "ACTIVE"
            not_assigned = not node_data.get('assigned', False)
            enough_space = node_data['available'] >= block_size
            
            self.logger.log("DEBUG", f"Node {node_id}: status={node_data['status']}, assigned={node_data.get('assigned', False)}, available={node_data['available']}")
            self.logger.log("DEBUG", f"Node {node_id}: status_ok={status_ok}, not_assigned={not_assigned}, enough_space={enough_space}")
            
            if status_ok and not_assigned and enough_space:
                available_nodes.append(node_id)
                self.logger.log("DEBUG", f"Node {node_id} added to available list")
        
        self.logger.log("DEBUG", f"Available nodes count: {len(available_nodes)}")
        return available_nodes

    def allocate(self, file_details,no_of_stripes,no_of_shards,block_size)->List[common_pb2.BlockGroups]:
        """
        Allocate blocks to data nodes.
        
        args:
            file_details: common_pb2.FileDetails - The file details
            no_of_stripes: int - The number of stripes
            no_of_shards: int - The number of shards
            block_size: int - The size of the block to allocate
            
        returns:
            list[namenode_pb2.BlockGroup] - The list of block groups
        """
        with self.lock:
            available_nodes = self.get_nodes(block_size)
            if not available_nodes:
                raise Exception("No available nodes to allocate blocks")

            total_nodes = len(available_nodes)
            start_cursor = self.cursor
            reservations: List[List[Tuple[str, str]]] = []

            for i in range(no_of_stripes):
                stripe_reservations: List[Tuple[str, str]] = []
                for j in range(no_of_shards):
                    node_index = (start_cursor + j) % total_nodes
                    node_id = available_nodes[node_index]

                    block_id = str(uuid.uuid4())

                    # Reserve resources
                    self.allocations[block_id] = node_id
                    self.registry.nodes[node_id]['available'] -= block_size
                    self.registry.nodes[node_id]['assigned'] = True

                    stripe_reservations.append((block_id, node_id))

                reservations.append(stripe_reservations)
                start_cursor = (start_cursor + no_of_shards) % total_nodes

            self.cursor = start_cursor
        
        block_groups: List[common_pb2.BlockGroups] = []

        for i, stripe in enumerate(reservations):
            stripe_id = f"{file_details.file_name}_{i}"
            stripe_placements: List[common_pb2.Placement] = []

            for block_id, node_id in stripe:
                node_proto = self.registry.to_node(node_id)

                stripe_placements.append(
                    common_pb2.Placement(
                        block_id=block_id,
                        node=node_proto
                    )
                )

            block_groups.append(
                common_pb2.BlockGroups(
                    stripe_id=stripe_id,
                    placement=stripe_placements
                )
            )

        return block_groups
    def commit_block(self,file_name,block_id,curr)->None:
        """
        Commit a block to the database.
        
        args:
            file_name: str - The name of the file
            block_id: str - The ID of the block
            curr: cursor - The database cursor
            
        returns:
            None
        """
        if block_id in self.allocations:
            node_id = self.allocations[block_id]
            curr.execute("INSERT INTO metadata (file_id, block_id, node_id) VALUES (%s, %s, %s)", (file_name, block_id, node_id))
            self.registry.nodes[node_id]['assigned'] = False
            del self.allocations[block_id]
    def release_nodes(self,blocks:List[str])->None:
        """
        Rollback a block allocation.
        
        args:
            block_id: str - The ID of the block
            
        returns:
            None
        """
        for block_id in blocks:
            if block_id in self.allocations:
                node_id = self.allocations[block_id]
                self.registry.nodes[node_id]['assigned'] = False
                del self.allocations[block_id]
        
    