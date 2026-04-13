import uuid
import threading
from typing import List, Tuple
import common_pb2
import namenode_pb2
from .registry import DataNodeRegistry
from .logger import Logger
from db_manager import get_connection

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
        for node_id, node_data in self.registry.nodes.items():
            if node_data['status'] == "ACTIVE" and not node_data.get('assigned', False) and node_data['available'] >= block_size:
                available_nodes.append(node_id)
        return available_nodes

    def allocate(self, file_details,no_of_stripes,no_of_shards,block_size)->List[namenode_pb2.BlockGroup]:
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
        
        block_groups: List[namenode_pb2.BlockGroup] = []

        for i, stripe in enumerate(reservations):
            stripe_id = f"{file_details.file_name}_{i}"
            stripe_nodes: List[common_pb2.Placement] = []

            for block_id, node_id in stripe:
                node_proto = common_pb2.NodeId(
                    node_id=node_id,
                    node=self.registry.to_node(node_id)
                )

                stripe_nodes.append(
                    common_pb2.Placement(
                        block_id=block_id,
                        node=node_proto
                    )
                )

            block_groups.append(
                namenode_pb2.BlockGroup(
                    stripe_id=stripe_id,
                    nodes=stripe_nodes
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
            del self.allocations[block_id]
        
    