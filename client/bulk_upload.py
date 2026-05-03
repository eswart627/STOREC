import os
import sys
import argparse

# Add the current directory to sys.path so we can import 'app'
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.client import upload_file
from app.config_loader import MAX_WORKERS
from app.delete_file import delete_file


import csv
from datetime import datetime
import time
import uuid
import tempfile

def bulk_upload(directory_path):
    if not os.path.exists(directory_path):
        print(f"Directory path '{directory_path}' does not exist.")
        return

    metrics_dir = 'metrics'
    os.makedirs(metrics_dir, exist_ok=True)
    csv_file = os.path.join(metrics_dir, 'upload_metrics.csv')
    file_exists = os.path.isfile(csv_file)

    print(f"Starting bulk upload from directory: {directory_path} (Testing all modes)")
    
    with open(csv_file, mode='a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['index', 'timestamp', 'file_name', 'file_size', 'mode', 'total_execution_time', 'total_stripes', 'avg_stripe_time', 'avg_encoding_time', 'total_blocks', 'avg_latency', 'avg_throughput', 'avg_network_throughput'])
        
        index = 1
        modes = ["single", "parallel", "block_parallel"]
        for root, _, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                for mode in modes:
                    synthesized_name = f"{mode}_{uuid.uuid4().hex[:8]}_{file}"
                    print(f"\n=== Triggering upload for: {file_path} as {synthesized_name} [Mode: {mode}] ===")
                    
                    with tempfile.TemporaryDirectory() as temp_dir:
                        temp_file_path = os.path.join(temp_dir, synthesized_name)
                        os.symlink(os.path.abspath(file_path), temp_file_path)
                        
                        try:
                            # upload_file calls sys.exit(1) on errors, we catch SystemExit to keep the loop going
                            metrics_tuple = upload_file(temp_file_path, mode, MAX_WORKERS)
                            if metrics_tuple:
                                overall, stripe, block, network, encoding = metrics_tuple
                                writer.writerow([
                                    index,
                                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    synthesized_name,
                                    os.path.getsize(file_path),
                                    mode,
                                    overall.get('total_execution_time', 0),
                                    stripe.get('total_stripes', 0),
                                    stripe.get('avg_stripe_time', 0),
                                    encoding.get('avg_encoding_time', 0),
                                    block.get('total_blocks', 0),
                                    block.get('avg_latency', 0),
                                    block.get('avg_throughput', 0),
                                    network.get('avg_network_throughput', 0)
                                ])
                                f.flush()
                                index += 1
                                
                                print(f"Deleting file {synthesized_name} from cluster to clear space...")
                                try:
                                    delete_file(synthesized_name)
                                except Exception as delete_error:
                                    print(f"[ERROR] Failed to delete {synthesized_name}: {delete_error}")
                        except SystemExit:
                            print(f"[WARNING] Upload for {synthesized_name} aborted due to an error (likely already exists). Attempting cleanup...")
                            try:
                                delete_file(synthesized_name)
                            except Exception:
                                pass
                        except Exception as e:
                            print(f"[ERROR] Exception during upload of {synthesized_name}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bulk upload files to the cluster.")
    parser.add_argument(
        "directory",
        help="Directory path containing files to upload"
    )
    args = parser.parse_args()

    bulk_upload(args.directory)
