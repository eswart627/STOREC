import os

from .namenode_client import NameNodeClient
from .pipeline_manager import PipelineManager


def upload_file(file_path):
    if not os.path.exists(file_path):
        raise Exception("File does not exist")
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    print("Uploading:", file_name)

    namenode = NameNodeClient()
    response = namenode.allocate_blocks(
        file_name,
        file_size
    )

    pipeline = PipelineManager(response.block_groups)
    block_ids = pipeline.run(file_path)

    namenode.commit_file(
        file_name,
        file_size,
        block_ids
    )
    print("Upload completed")


if __name__ == "__main__":

    import sys

    if len(sys.argv) != 2:

        print(
            "Usage:"
        )

        print(
            "python client.py <file>"
        )

        exit(1)

    upload_file(
        sys.argv[1]
    )