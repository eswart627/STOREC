import uuid
import threading
from typing import List, Tuple, Dict
from proto import common_pb2
from .registry import DataNodeRegistry
from .logger import Logger

class AllocationManager:
    """
    AllocationManager is responsible for allocating blocks to data nodes.
    
    args:
        registry: DataNodeRegistry - The registry of data nodes
        logger: Logger - The logger for logging allocation events
    """
    def __init__(self,registry:DataNodeRegistry, logger:Logger, data_blocks:int=6, parity_blocks:int=3):
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
        self.data_blocks=data_blocks
        self.parity_blocks=parity_blocks
        self.block_size=0
        
    def set_policy(self,data_blocks:int,parity_blocks:int):
        self.data_blocks=data_blocks
        self.parity_blocks=parity_blocks
    def get_nodes(self)->List[str]:
        """
        Get the list of available nodes for allocation.
        
        args:
            block_size: int - The size of the block to allocate
            
        returns:
            list[str] - The list of available node IDs
        """
        available_nodes: List[str] = []
        self.logger.log("DEBUG", f"Looking for nodes with block_size={self.block_size}")
        self.logger.log("DEBUG", f"Total nodes in registry: {len(self.registry.nodes)}")
        
        for node_id, node_data in self.registry.nodes.items():
            status_ok = node_data['status'] == "ACTIVE"
            not_assigned = not node_data.get('assigned', False)
            enough_space = node_data['available'] >= self.block_size
            
            self.logger.log("DEBUG", f"Node {node_id}: status={node_data['status']}, assigned={node_data.get('assigned', False)}, available={node_data['available']}")
            self.logger.log("DEBUG", f"Node {node_id}: status_ok={status_ok}, not_assigned={not_assigned}, enough_space={enough_space}")
            
            if status_ok and not_assigned and enough_space:
                available_nodes.append(node_id)
                self.logger.log("DEBUG", f"Node {node_id} added to available list")
        
        self.logger.log("DEBUG", f"Available nodes count: {len(available_nodes)}")
        return available_nodes

    def allocate(self, file_details,no_of_stripes,no_of_shards)->List[common_pb2.BlockGroups]:
        """
        Allocate blocks to data nodes.
        
        args:
            file_details: common_pb2.FileDetails - The file details
            no_of_stripes: int - The number of stripes
            no_of_shards: int - The number of shards
            block_size: int - The size of the block to allocate
            
        returns:
            list[common_pb2.BlockGroups] - The list of block groups
        """
        with self.lock:
            available_nodes = self.get_nodes()
            if not available_nodes:
                raise Exception("No available nodes to allocate blocks")
            total_nodes = len(available_nodes)
            start_cursor = self.cursor
            
            # Initialize allocation for this file
            file_name = file_details.file_name
            self.allocations[file_name] = {}
            
            for i in range(no_of_stripes):
                stripe_id = f"{file_name}_{i}"
                self.allocations[file_name][stripe_id] = {}
                
                for j in range(no_of_shards):
                    node_index = (start_cursor + j) % total_nodes
                    node_id = available_nodes[node_index]

                    block_id = str(uuid.uuid4())

                    # Reserve resources
                    self.allocations[file_name][stripe_id][block_id] = node_id
                    self.registry.nodes[node_id]['available'] -= self.block_size
                    self.registry.nodes[node_id]['assigned'] = True

                start_cursor = (start_cursor + no_of_shards) % total_nodes

            self.cursor = start_cursor
        
        block_groups: List[common_pb2.BlockGroups] = []

        # Use the actual allocation structure to build block groups
        current_allocation = self.allocations[file_name]
        for i in range(no_of_stripes):
            stripe_id = f"{file_name}_{i}"
            stripe_placements: List[common_pb2.Placement] = []

            # Get the stripe allocation from the current allocation
            stripe_allocation = current_allocation.get(stripe_id, {})
            for block_id, node_id in stripe_allocation.items():
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
    def commit_block(self,file_name,block_ids,curr)->None:
        """
        Commit blocks to the database.
        
        args:
            file_name: str - The name of the file
            block_ids: List[str] - List of block IDs to commit
            curr: cursor - The database cursor
            
        returns:
            None
        """
        with self.lock:
            if file_name not in self.allocations:
                raise Exception(f"No allocations found for file {file_name}")
            
            file_allocation = self.allocations[file_name]
            block_ids_set = set(block_ids)  # Convert to set for efficient lookup
            committed_blocks = set()  # Track what we actually commit
            
            # Iterate through all stripes in the file allocation
            for stripe_id, stripe_allocation in file_allocation.items():
                # Iterate through all blocks in this stripe
                blocks_to_remove = []
                for block_id, node_id in stripe_allocation.items():
                    if block_id in block_ids_set:
                        # Commit this block to database
                        curr.execute("INSERT INTO metadata_table (file_id, stripe_id, block_id,size, node_id) VALUES (%s, %s, %s, %s, %s)", (file_name, stripe_id, block_id, self.block_size, node_id))
                        self.registry.nodes[node_id]['assigned'] = False
                        committed_blocks.add(block_id)  # Track what we committed
                        blocks_to_remove.append(block_id)
                
                # Remove committed blocks from stripe allocation
                #for block_id in blocks_to_remove:
                    #del stripe_allocation[block_id]
                
                # Clean up empty stripe allocations
                #if not stripe_allocation:
                    #del file_allocation[stripe_id]
            
            # Check if all requested block_ids were found and committed
            missing_blocks = block_ids_set - committed_blocks
            if missing_blocks:
                raise Exception(f"Blocks not found in allocation: {list(missing_blocks)}")
            
            # Clean up empty file allocations
            if not file_allocation:
                del self.allocations[file_name]
    def release_nodes(self,file_name:str, blocks:List[str])->None:
        """
        Under Maintainenance
        
        args:
            file_name: str - The name of the file
            blocks: List[str] - List of block IDs to release
            
        returns:
            None
        """
        with self.lock:
            if file_name in self.allocations:
                file_allocation = self.allocations[file_name]
                for block_id in blocks:
                    # Search through stripes to find the block
                    for stripe_id, stripe_allocation in file_allocation.items():
                        if block_id in stripe_allocation:
                            node_id = stripe_allocation[block_id]
                            self.registry.nodes[node_id]['assigned'] = False
                            del stripe_allocation[block_id]
                            
                            # Clean up empty stripe allocations
                            if not stripe_allocation:
                                del file_allocation[stripe_id]
                            break
                
                # Clean up empty file allocations
                if not file_allocation:
                    del self.allocations[file_name]
    def delete_blocks(self,blocks,cur):
        with self.lock:
            for block in blocks:
                block_id, node_id,size = block
            
                self.registry.nodes[node_id]['used'] -= size
                self.registry.nodes[node_id]['available'] += size
            
    def send_metadata(self, file_name:str,file,blocks_per_stripe:int):
        block_groups:List[common_pb2.BlockGroups]=[]
        stripe_count=0
        block_count=0
        while block_count < len(file):
            stripe_id = f"{file_name}_{stripe_count}"
            placements:List[common_pb2.Placement]=[]
            blocks_in_current_stripe = 0
            
            while blocks_in_current_stripe < blocks_per_stripe and block_count < len(file):
                placements.append(common_pb2.Placement(
                    block_id=file[block_count][0],
                    node=self.registry.to_node(file[block_count][1])
                ))
                block_count += 1  
                blocks_in_current_stripe += 1

            block_groups.append(common_pb2.BlockGroups(
                stripe_id=stripe_id,
                placement=placements
            ))
            stripe_count += 1
        return block_groups
        