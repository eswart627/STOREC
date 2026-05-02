import os

from .namenode_client import NameNodeClient
from .transfer import DataNodeClient


def read_file(file_name, output_path):
    namenode = NameNodeClient()
    print("Requesting metadata for:", file_name)
    response = namenode.get_file_metadata(file_name)

    block_groups = response.block_groups
    with open(output_path, "wb") as f:
        for stripe in block_groups:
            for placement in stripe.placement:
                node = placement.node.node
                client = DataNodeClient(
                    node.hostname,
                    node.port
                )
                print(
                    f"Reading block "
                    f"{placement.block_id} "
                    f"from "
                    f"{node.hostname}:{node.port}"
                )
                data = client.read_block(placement.block_id)
                f.write(data)
    print("File read completed")