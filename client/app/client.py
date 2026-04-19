import os
import argparse

from .namenode_client import NameNodeClient
from .pipeline_manager import PipelineManager
from .config_loader import (MAX_WORKERS)


def upload_file(file_path, mode,max_workers):
    if not os.path.exists(file_path):
        raise Exception("File does not exist")
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    print("Uploading:", file_name)
    print("Pipeline mode: ", mode)

    namenode = NameNodeClient()

    response = namenode.allocate_blocks(file_name, file_size)
    pipeline = PipelineManager(
        response.block_groups,
        mode=mode,
        max_workers=max_workers
    )
    block_ids = pipeline.run(file_path)

    namenode.commit_file(
        file_name,
        file_size,
        block_ids
    )
    print("Upload completed")


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "file",
        help = "File to upload"
    )

    parser.add_argument(
        "--mode",
        choices=["single", "parallel"],
        default="single",
        help="Pipeline execution mode"
    )

    args = parser.parse_args()

    upload_file(args.file, args.mode, MAX_WORKERS)

if __name__ == "__main__":
    main()