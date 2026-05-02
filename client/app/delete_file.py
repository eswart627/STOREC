from .namenode_client import NameNodeClient
from .transfer import DataNodeClient


def delete_file(file_name):
    namenode = NameNodeClient()
    print("Requesting delete:", file_name)
    block_ids = namenode.delete_file(file_name)

    print("Deleting blocks from DataNodes")
    for block_id in block_ids:
        print("Deleted:", block_id)
        
    print("File deletion completed")