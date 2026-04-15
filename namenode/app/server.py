
import grpc
import os
import time
import math
import datetime
import random
import pytz  # Add this import for timezone handling
import uuid

from concurrent.futures import ThreadPoolExecutor

from proto import namenode_pb2
from proto import namenode_pb2_grpc
from proto import common_pb2

#from .heartbeat_manager import HeartbeatManager
from .health_checker import HealthChecker
from ..db_manager import get_connection
from .allocation import AllocationManager

class NameNodeService(
    namenode_pb2_grpc.NameNodeServiceServicer
):
    """
    Name node service.
    
    Attributes:
        registry: Data node registry.
        logger: Logger object.
        heartbeat_manager: Heartbeat manager.
    """
    def __init__(self, registry, logger):
        self.registry = registry
        self.logger = logger
        #self.heartbeat_manager = HeartbeatManager(self.logger,self.registry)
        self.allocation_manager = AllocationManager(self.registry, self.logger)
    def RegisterDataNode(self, request:namenode_pb2.RegisterRequest, context)->namenode_pb2.RegisterResponse:
        """
        Register a data node with the name node.
        Args:
            request: RegisterRequest(Nodeid(node_id:str,hostname:str,port:int),capacity:int)
            context: grpc.ServicerContext
        
        Returns:
            RegisterResponse(Status(success:bool,message:str))
        """
        node_id=request.node_id
        node = request.node
        address=f"{node.hostname}:{node.port}"
        if not node_id:
            if self.registry.lookup.get(address):
                existing_node_id = self.registry.lookup[address]
                self.registry.register(
                    node_id=existing_node_id,
                    hostname=node.hostname,
                    port=node.port,
                    capacity=request.capacity_bytes,
                    mode=1,
                )
                return namenode_pb2.RegisterResponse(
                    node_id=existing_node_id,
                    status=common_pb2.Status(
                        success=True,
                        message="Already registered",
                    )
                )
            else:
                
                node_id=self.registry.register(
                    node_id=None,
                    hostname=node.hostname,
                    port=node.port,
                    capacity=request.capacity_bytes,
                    mode=0,
                )
                return namenode_pb2.RegisterResponse(
                    node_id=node_id,
                    status=common_pb2.Status(
                        success=True,
                        message="Registered",
                    )
                )
        else:
            self.registry.register(
                node_id=node_id,
                hostname=node.hostname,
                port=node.port,
                capacity=request.capacity_bytes,
                mode=1,
            )
            self.logger.log("Datanode with ID ", f"{node_id} at {address} (re-registered)")
            return namenode_pb2.RegisterResponse(
                node_id=node_id,
                status=common_pb2.Status(
                    success=True,
                    message="Heartbeat Recorded",
                )
            )
        
    def SendHeartbeat(self, request:namenode_pb2.HeartbeatRequest, context)->namenode_pb2.HeartbeatResponse:
        """
        Send a heartbeat to the name node.
        Args:
            request: HeartbeatRequest(Heartbeat(node_id:str,timestamp:int,used_bytes:int,free_bytes:int))
            context: grpc.ServicerContext
        
        Returns:
            HeartbeatResponse(Status(success:bool,message:str))
        """
        heartbeat = request.heartbeat
        
        # Log the received heartbeat on NameNode side
        self.logger.log(
            "HEARTBEAT_RECEIVED",
            f"node_id={heartbeat.node_id}, timestamp={heartbeat.timestamp}, "
            f"used_bytes={heartbeat.used_bytes}, free_bytes={heartbeat.free_bytes}"
        )
        
        self.registry.heartbeat(heartbeat.node_id)
        return namenode_pb2.HeartbeatResponse(
            status=common_pb2.Status(
                success=True, 
                message=f"ACK for node {heartbeat.node_id} at {heartbeat.timestamp}"
            )
        )
        
    def AllocateBlocks(self, request:namenode_pb2.AllocateBlocksRequest, context)->namenode_pb2.AllocateBlocksResponse:
        """
        Allocate blocks for a file.
        Args:
            request: AllocateBlocksRequest(file_details,stripe_size,data_blocks_k,parity_blocks_m)
            context: grpc.ServicerContext
        
        Returns:
            AllocateBlocksResponse(block_groups:list[BlockGroups])
        """
        file_details=request.file_details
        no_of_stripes=math.ceil(file_details.file_size / request.stripe_size)
        self.allocation_manager.block_size=math.ceil(request.stripe_size / request.data_blocks_k)
        no_of_shards=request.data_blocks_k+request.parity_blocks_m
        self.allocation_manager.set_policy(request.data_blocks_k,request.parity_blocks_m)
        block_groups=self.allocation_manager.allocate(
            file_details=file_details,
            no_of_stripes=no_of_stripes,
            no_of_shards=no_of_shards
        )
        return namenode_pb2.AllocateBlocksResponse(block_groups=block_groups)

    def CommitFile(self, request, context):
        """
        Commit file to registry.
        
        Args:
            request: CommitFileRequest object.
            context: gRPC context.
            
        Returns:
            CommitFileResponse object.
        """

        file_name=request.file_details.file_name
        self.logger.log("COMMIT_FILE", f"Committing file {file_name}")
        file_size=request.file_details.file_size/1024 #kb
        
        conn=get_connection()
        cur=conn.cursor()
        cur.execute("SELECT id FROM metadata_table ORDER BY id DESC LIMIT 1")
        metadata_id = cur.fetchone()
        if metadata_id:
            metadata_id = metadata_id[0]
        else:
            metadata_id = 0

        try:
            self.logger.log("COMMIT_FILE", f"Committing {len(request.block_ids)} blocks for file {file_name}")
            self.allocation_manager.commit_block(file_name, request.block_ids, cur)
        except Exception as e:
            self.logger.log("COMMIT_FILE", f"Failed to commit blocks for file {file_name}: {e}")
            conn.rollback()
            del self.allocation_manager.allocations[file_name]
            return namenode_pb2.CommitFileResponse(
                status=common_pb2.Status(
                    success=False,
                    message=f"Failed to commit blocks for file {file_name}: {e}"
                    )
                )
        try:
            cur.execute("INSERT into file_table(file_name, size, block_count, start_index,data_blocks,parity_blocks) values(%s, %s, %s, %s, %s, %s)", (file_name, file_size, request.total_blocks, metadata_id+1, self.allocation_manager.data_blocks, self.allocation_manager.parity_blocks))
        except Exception as e:
            self.logger.log("COMMIT_FILE", f"Failed to commit file {file_name}: {e}")
            conn.rollback()
            del self.allocation_manager.allocations[file_name]
            return namenode_pb2.CommitFileResponse(
                status=common_pb2.Status(
                    success=False,
                    message=f"Failed to commit file {file_name}: {e}"
                )
            )
        conn.commit()
        return namenode_pb2.CommitFileResponse(
            status=common_pb2.Status(
                success=True,
                message="File committed successfully"
            )
        )
    
    def DeleteFile(self, request, context):
        """
        Delete file from registry.
        
        Args:
            request: DeleteFileRequest object.
            context: gRPC context.
            
        Returns:
            DeleteFileResponse object.

        """
        
        conn= get_connection()
        cur=conn.cursor()
        try:
            cur.execute("SELECT block_id, node_id, size FROM metadata_table WHERE file_id = %s", 
                   (request.file_details.file_name,))
            blocks = cur.fetchall()

            cur.execute("DELETE FROM metadata_table WHERE file_id = %s", (request.file_details.file_name,))
            cur.execute("DELETE FROM file_table WHERE file_name = %s", (request.file_details.file_name,))
            cur.execute("SELECT MAX(id) FROM metadata_table")
            max_id = cur.fetchone()[0]
            if max_id is not None:
                next_id = int(max_id) + 1
                query = f"ALTER TABLE metadata_table AUTO_INCREMENT = {next_id}"
                cur.execute(query)
            self.allocation_manager.delete_blocks(blocks,cur)
            conn.commit()
            self.logger.log("DELETE_FILE", f"Deleting file {request.file_details.file_name}")
            return namenode_pb2.DeleteFileResponse(
                status=common_pb2.Status(
                    success=True,
                    message="File deleted successfully"
                )
            )

        except Exception as e:
            conn.rollback()
            self.logger.log("DELETE_FILE", f"Failed to delete file {request.file_details.file_name}: {e}")
            return namenode_pb2.DeleteFileResponse(
                status=common_pb2.Status(
                    success=False,
                    message=f"Failed to delete file {request.file_details.file_name}: {e}"
                )
            )

    def list_files(self, request, context):
        """
        List files from registry.
        
        Args:
            request: ListFilesRequest object.
            context: gRPC context.
            
        Returns:
            ListFilesResponse object.
        """
        self.logger.log("LIST_FILES", "Listing files...\n")
        conn=get_connection()
        cur=conn.cursor()
        cur.execute("SELECT * FROM file_table")
        files=cur.fetchall()
        for i,row in enumerate(files):
            self.logger.log(f"File_{i}", f"Files: {row}")
        return namenode_pb2.ListFilesResponse(
            status=common_pb2.Status(
                success=True,
                message="Files listed successfully"
            )
        )

    def GetFileMetadata(self, request, context):
        """
        Get file metadata from registry.
        
        Args:
            request: GetFileMetadataRequest object.
            context: gRPC context.
            
        Returns:
            GetFileMetadataResponse object.
        """

        self.logger.log("GET_FILE_METADATA", f"Getting metadata for file {request.file_details.file_name}")
        conn=get_connection()
        cur=conn.cursor()
        cur.execute("SELECT data_blocks,parity_blocks FROM file_table WHERE file_name=%s",(request.file_details.file_name))
        result = cur.fetchone()
        if result:
            data_blocks, parity_blocks = result
        else:
            data_blocks, parity_blocks = 3, 2
        cur.execute("SELECT block_id, node_id FROM metadata_table WHERE file_id=%s", (request.file_details.file_name,))
        file=cur.fetchall()
        total_blocks = len(file)
        blocks_per_stripe = data_blocks + parity_blocks
        no_of_stripes = total_blocks // blocks_per_stripe if blocks_per_stripe > 0 else 0
        stripe_size = request.file_details.file_size // no_of_stripes if no_of_stripes > 0 else 0
        block_groups = self.allocation_manager.send_metadata(request.file_details.file_name, file, blocks_per_stripe)
        return namenode_pb2.GetFileMetadataResponse(
            file_details=request.file_details,
            stripe_size=stripe_size,
            data_blocks_k=data_blocks,
            parity_blocks_m=parity_blocks,
            block_groups=block_groups
            
        )

class NameNodeServer:
    """
    Name node server.
    
    Attributes:
        config: Configuration object.
        registry: Node registry object.
        logger: Logger object.
        server: gRPC server.
    """
    def __init__(self, config, registry, logger,flag=0):

        self.config = config

        self.registry = registry

        self.logger = logger

        self.test = flag
        self.server = grpc.server(
            ThreadPoolExecutor(
                max_workers=config.worker_threads
            )
        )
        

        namenode_pb2_grpc.add_NameNodeServiceServicer_to_server(
            NameNodeService(registry, logger), self.server
        )

        address = f"{config.hostname}:{config.port}"
        
        print(f"DEBUG: NameNode binding to {address}", flush=True)
        self.server.add_insecure_port(address)
        if not self.test:
            self.health_checker = HealthChecker(
                self.registry, 
                config.health_check_interval,
                self.logger
                 )
        else:
            print("DEBUG: Health checker disabled", flush=True)

    def start(self)->None:
        """
        Start the name node server.
        """
        print("Starting gRPC server", flush=True)  # Debug print to indicate gRPC server startup
        self.server.start()

        # Start health checker
        if not self.test:
            self.health_checker.start()

        current_time = datetime.datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%H:%M")  # Format time as HH:MM in IST
        self.logger.log(
            "SERVER_START",
            f"namenode_{current_time}",
        )

        print("gRPC server started and waiting for termination", flush=True)  # Debug print to indicate server is running
        self.server.wait_for_termination()

    def stop(self, grace_period: int = 10) -> None:
        """
        Stop the name node server with a custom grace period.
        """
        print(f"Stopping NameNode server (grace period: {grace_period}s)...", flush=True)
        
        # 1. Stop background processes first
        if not self.test:
            self.health_checker.stop()
        
        # 2. Persist the registry state
        self.registry.save_state()
        
        # 3. Stop the gRPC server with the increased grace period
        # The server will stop accepting new requests and wait up to 'grace' seconds
        # for existing RPCs to complete.
        self.server.stop(grace=grace_period)
        
        self.logger.log("SERVER_STOP", f"Graceful shutdown completed ({grace_period}s)")
