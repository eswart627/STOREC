import os
import argparse
import sys

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

    try:
        # 1. Try to allocate blocks (NameNode might catch duplicate here)
        print(f"Requesting allocation for: {file_name}")
        response = namenode.allocate_blocks(file_name, file_size)
        
        # 2. Run the pipeline
        pipeline = PipelineManager(
            response.block_groups,
            mode=mode,
            max_workers=max_workers
        )
        block_ids = pipeline.run(file_path)

        # 3. Try to commit the file
        namenode.commit_file(
            file_name,
            file_size,
            block_ids
        )
        print("Upload completed successfully")

    except Exception as e:
        # Check if it's the duplicate entry error
        if "Duplicate entry" in str(e) or "already exists" in str(e).lower():
            print(f"\n[ERROR] The file '{file_name}' already exists in the cluster.")
            print("Please delete the existing file first or rename this file.")
        else:
            # Handle other unexpected errors
            print(f"\n[FATAL ERROR] {str(e)}")
        
        # Exit so we don't proceed with a broken state
        sys.exit(1)

def download_file(file_name, output_path, mode, max_workers):
    """Download and reconstruct file"""
    namenode = NameNodeClient()
    
    # Get file metadata
    response = namenode.get_file_metadata(file_name)
    
    # Download blocks
    downloader = DownloadManager(
        response.block_groups,
        mode=mode,
        max_workers=max_workers
    )
    downloader.download_file(output_path)

def delete_file(file_name):
    """Delete file from cluster"""
    namenode = NameNodeClient()
    response = namenode.delete_file(file_name)
    
    if response.status.success:
        print(f"File '{file_name}' deleted successfully")
        print(f"Deleted {len(response.block_ids)} blocks")
    else:
        raise Exception(f"Delete failed: {response.status.message}")

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "file",
        help = "File to upload"
    )

    parser.add_argument(
        "--mode",
        choices=["single", "parallel", "block_parallel"],
        default="single",
        help="Pipeline execution mode"
    )

    args = parser.parse_args()
    
    if args.operation == "upload":
        upload_file(args.file, args.mode, MAX_WORKERS)
    elif args.operation == "download":
        output_path = args.output or args.file
        download_file(args.file, output_path, args.mode, MAX_WORKERS)
    elif args.operation == "delete":
        delete_file(args.file)

if __name__ == "__main__":
    main()