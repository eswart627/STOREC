from concurrent.futures import ThreadPoolExecutor
import os
import time
import logging
from .stripe_builder import StripeBuilder
from .encoder import Encoder
from .transfer import DataNodeClient
from .config_loader import (K, M, BLOCK_SIZE)

class PipelineManager:
    def __init__(self,block_groups, mode="single", max_workers=4):
        self.total_execution_time = 0
        
        self.block_groups = block_groups
        self.mode = mode
        self.max_workers = max_workers
        self.encoder = Encoder()
        self.written_block_ids = []
        # for stripe encoding verification
        self.total_stripes_processed = 0
        self.total_bytes_read =0
        self.metrics_report = []
        
    def _print_table(self,file_name,file_path):
        if not self.metrics_report:
            return

        # Prepare the table components
        file_size_bytes = os.path.getsize(file_path)
        file_size_mb = file_size_bytes / (1024 * 1024)
        
        lines = []
        lines.append(f"\n{'='*95}")
        lines.append(f" UPLOAD REPORT | Mode: {self.mode.upper()} | File: {file_name} => Size: ({file_size_mb:.2f} MB)")
        lines.append(f"{'='*95}")
        
        header = f"{'Block ID':<40} | {'Node':<20} | {'Latency':<10} | {'Throughput':<15}"
        lines.append(header)
        lines.append("-" * len(header))

        t_lat, t_tp = 0, 0
        for m in self.metrics_report:
            t_lat += m['lat']
            t_tp += m['tp']
            lines.append(f"{m['id']:<40} | {m['node']:<20} | {m['lat']:>8.4f}s | {m['tp']:>8.2f} MB/s")

        lines.append("-" * len(header))
        
        avg_lat = t_lat / len(self.metrics_report)
        avg_tp = t_tp / len(self.metrics_report)
        
        lines.append(f"{'AVERAGE':<63} | {avg_lat:>8.4f}s | {avg_tp:>8.2f} MB/s")
        lines.append(f"{'='*95}\n")
        
        # --- NEW: Log Total Execution Time here ---
        lines.append(f"{'TOTAL UPLOAD DURATION':<63} | {self.total_execution_time:>8.4f}s")
        lines.append(f"{'='*95}\n")
        
        # Log each line to the file
        for line in lines:
            logging.info(line)
            # Optional: keep the print(line) here if you still want to see it in terminal
        print(f"Upload complete. Metrics logged to client.log")

    def _process_stripe(self, stripe_index, data_blocks, placements):
        #print(f"Processing stripe {stripe_index}")
        #logging.info(f"Processing stripe {stripe_index}")
        blocks = self.encoder.encode(data_blocks)
        local_ids = []

        for block_data, placement in zip(blocks, placements):
            node = placement.node.node
            node_addr = f"{node.hostname}:{node.port}"
            client = DataNodeClient(
                node.hostname,
                node.port
            )
            response = client.write_block(placement.block_id, block_data)
            
            if not response.status.success:
                raise Exception(f"Block write failed: {response.status.message}")
            
            metrics = response.metrics
            
            self.metrics_report.append({
                'id': placement.block_id,
                'node': node_addr,
                'lat': metrics.latency_seconds,
                'tp': metrics.throughput_mbs
            })
            
            #log_msg = (f"[Metric] Block {placement.block_id} write successful ->. "
            #           f"Latency: {metrics.latency_seconds:.4f}s, "
            #           f"Throughput: {metrics.write_throughput_mb_per_sec:.2f} MB/s")
            #logging.info(log_msg)
            
            local_ids.append(placement.block_id)
        return local_ids

    def run(self, file_path):
        file_name = os.path.basename(file_path)
        self.written_block_ids=[]
        
        
        start_time = time.time()
        if self.mode == "single":
            res = self._run_single_threaded(file_path)
        elif self.mode == "parallel":
            res = self._run_parallel(file_path)
        else:
            raise Exception("Invalid pipeline mode")
        
        self.total_execution_time = time.time() - start_time
        self._print_table(file_name,file_path)
        return res

    def _run_single_threaded(self, file_path):
        file_size = os.path.getsize(file_path)

        expected_stripes = len(self.block_groups)
        print(f"\nExpected stripes from NameNode: {expected_stripes}\n")

        builder = StripeBuilder(file_path)
        stripe_index = 0
        try:
            while True:
                data_blocks = (builder.next_stripe())
                if data_blocks is None:
                    break
                if stripe_index >= expected_stripes:
                    raise Exception(
                        "More stripes processed than allocated"
                        f" (processed {stripe_index + 1}, expected {expected_stripes})"
                    )

                # for stripe encoding verification
                self.total_stripes_processed += 1
                stripe_bytes= sum(len(b) for b in data_blocks)
                self.total_bytes_read += stripe_bytes
                
                print(f"Processing stripe {stripe_index}")
                print(
                    f"  Data blocks read: {len(data_blocks)} "
                    f"(expected {K})"
                )

                # encoding
                blocks = self.encoder.encode(data_blocks)
                print(
                    f"  Blocks after encoding: {len(blocks)} "
                    f"(expected {K + M})"
                )

                # placement lookup
                stripe = self.block_groups[stripe_index]
                placements = stripe.placement
                if len(placements) != len(blocks):
                    raise Exception(
                        "Placement count mismatch "
                        f"{len(placements)} vs {len(blocks)}"
                    )
                print(f"  Placement entries: {len(placements)}")

                # write blocks to DN
                for block_data, placement in zip(blocks, placements):
                    block_id = placement.block_id

                    node_id = placement.node
                    node = node_id.node
                    hostname = node.hostname
                    port = node.port

                    block_size = len(block_data)
                    print(
                        f"    Block {block_id}"
                        f" -> {hostname}:{port}"
                        f" size={block_size}"
                    )

                    client = DataNodeClient(hostname, port)
                    response = client.write_block(
                        block_id,
                        block_data
                    )

                    if not response.status.success:
                        raise Exception(f"Block write failed: {response.status.message}")
                    
                    metrics = response.metrics
                    self.metrics_report.append({
                        "id": placement.block_id,
                        "node": f"{node.hostname}:{node.port}",
                        "lat": metrics.latency_seconds,
                        "tp": metrics.throughput_mbs
                    })
                    #print(f"      >> Metric: Latency={metrics.latency_seconds:.4f}s, Disk Speed={metrics.throughput_mbs:.2f} MB/s")
                    # log_msg = (f"[Metric] Block {block_id} write successful ->. "
                    #            f"Latency: {metrics.latency_seconds:.4f}s, "
                    #            f"Throughput: {metrics.throughput_mbs:.2f} MB/s")
                    # logging.info(log_msg)
                    self.written_block_ids.append(block_id)
                stripe_index += 1
                
        finally: 
            builder.close()
            print()
            print("Processing summary")
            print("------------------")
            print(
                f"Stripes processed: "
                f"{self.total_stripes_processed}"
            )
            print(
                f"Total block IDs generated: "
                f"{len(self.written_block_ids)}"
            )
            print(
                f"Total bytes read (padded): "
                f"{self.total_bytes_read}"
            )
            print(
                f"Original file size: "
                f"{file_size}"
            )
        return self.written_block_ids
    
    def _run_parallel(self, file_path):
        print(
            f"Running parallel pipeline "
            f"workers={self.max_workers}"
        )
        builder = StripeBuilder(file_path)
        stripe_index = 0
        futures = []
        executor = ThreadPoolExecutor(max_workers=self.max_workers)
        try:
            while True:
                data_blocks = builder.next_stripe()
                if data_blocks is None:
                    break
                if stripe_index >= len(self.block_groups):
                    raise Exception(
                        "More stripes processed than allocated"
                    )
                placements = (
                    self.block_groups[stripe_index].placement
                )

                future = executor.submit(
                    self._process_stripe,
                    stripe_index,
                    data_blocks,
                    placements
                )

                futures.append(future)
                stripe_index += 1

            for future in futures:
                block_ids = future.result()
                self.written_block_ids.extend(block_ids)
        finally:
            builder.close()
            executor.shutdown()
        return self.written_block_ids
    