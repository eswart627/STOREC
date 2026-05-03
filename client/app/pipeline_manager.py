import os
import time
import logging
from .stripe_builder import StripeBuilder
from .encoder import Encoder
from .transfer import DataNodeClient
from concurrent.futures import (
    ThreadPoolExecutor,
    wait,
    FIRST_COMPLETED
)
from .config_loader import (K, M, BLOCK_SIZE)

from .parallel_writer import ParallelStripeWriter

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
        self.stripe_build_times = []
        self.read_times = []
        self.encoding_times = []
        self.network_throughput_measurements = []
        self.parallel_writer = ParallelStripeWriter(
                self.encoder,
                self.metrics_report
        )
        
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
        
        header = f"{'Block ID':<40} | {'Node':<20} | {'Latency':<10} | {'Throughput':<15} | {'Net TP':<10}"
        lines.append(header)
        lines.append("-" * len(header))

        t_lat, t_tp, t_net_tp = 0, 0, 0
        for m in self.metrics_report:
            t_lat += m['lat']
            t_tp += m['tp']
            t_net_tp += m.get('net_tp', 0)
            net_tp_str = f"{m.get('net_tp', 0):>8.2f}" if 'net_tp' in m else "N/A"
            lines.append(f"{m['id']:<40} | {m['node']:<20} | {m['lat']:>8.4f}s | {m['tp']:>8.2f} MB/s | {net_tp_str} MB/s")

        lines.append("-" * len(header))
        
        avg_lat = t_lat / len(self.metrics_report)
        avg_tp = t_tp / len(self.metrics_report)
        avg_net_tp = t_net_tp / len([m for m in self.metrics_report if 'net_tp' in m]) if any('net_tp' in m for m in self.metrics_report) else 0
        
        avg_net_tp_str = f"{avg_net_tp:>8.2f}" if avg_net_tp > 0 else "N/A"
        lines.append(f"{'AVERAGE':<63} | {avg_lat:>8.4f}s | {avg_tp:>8.2f} MB/s | {avg_net_tp_str} MB/s")
        lines.append(f"{'='*95}\n")
        
        # --- NEW: Log Total Execution Time here ---
        lines.append(f"{'TOTAL UPLOAD DURATION':<63} | {self.total_execution_time:>8.4f}s")
        lines.append(f"{'='*95}\n")
        
        # --- NEW: Stripe Build Time Metrics ---
        if self.stripe_build_times:
            lines.append(f"\n{'STRIPE BUILD TIME METRICS':<95}")
            lines.append(f"{'='*95}")
            
            total_read_time = sum(self.read_times)
            total_encode_time = sum(self.encoding_times)
            total_stripe_time = sum(self.stripe_build_times)
            
            avg_read_time = total_read_time / len(self.read_times) if self.read_times else 0
            avg_encode_time = total_encode_time / len(self.encoding_times) if self.encoding_times else 0
            avg_stripe_time = total_stripe_time / len(self.stripe_build_times)
            
            lines.append(f"Total stripes processed: {len(self.stripe_build_times)}")
            lines.append(f"Total stripe build time: {total_stripe_time:.4f}s")
            lines.append(f"  - Total read time:     {total_read_time:.4f}s")
            lines.append(f"  - Total encode time:   {total_encode_time:.4f}s")
            lines.append(f"Average stripe build time: {avg_stripe_time:.4f}s")
            lines.append(f"  - Avg read time:       {avg_read_time:.4f}s")
            lines.append(f"  - Avg encode time:     {avg_encode_time:.4f}s")
            
            if file_size_bytes > 0:
                stripe_time_per_mb = total_stripe_time / (file_size_bytes / (1024 * 1024))
                lines.append(f"Stripe build time per MB: {stripe_time_per_mb:.4f}s/MB")
            
            lines.append(f"{'='*95}\n")
        
        # --- NEW: Network Throughput Metrics ---
        if self.network_throughput_measurements:
            lines.append(f"\n{'NETWORK THROUGHPUT METRICS':<95}")
            lines.append(f"{'='*95}")
            
            total_net_tp = sum(m['network_throughput_mbps'] for m in self.network_throughput_measurements)
            avg_net_tp = total_net_tp / len(self.network_throughput_measurements)
            
            lines.append(f"Total network tests: {len(self.network_throughput_measurements)}")
            lines.append(f"Average network throughput: {avg_net_tp:.2f} MB/s")
            lines.append(f"Total network throughput: {total_net_tp:.2f} MB/s")
            
            # Network throughput per node
            node_network_tps = {}
            for m in self.network_throughput_measurements:
                node = m['node']
                if node not in node_network_tps:
                    node_network_tps[node] = []
                node_network_tps[node].append(m['network_throughput_mbps'])
            
            lines.append(f"\nNetwork throughput by node:")
            for node, tps in node_network_tps.items():
                avg_node_tp = sum(tps) / len(tps)
                lines.append(f"  {node}: {avg_node_tp:.2f} MB/s ({len(tps)} tests)")
            
            lines.append(f"{'='*95}\n")
        
        # Log each line to the file
        for line in lines:
            logging.info(line)
            # Optional: keep the print(line) here if you still want to see it in terminal
        print(f"Upload complete. Metrics logged to client.log")

    def get_metrics_tuple(self):
        """
        Returns all current metrics as a tuple for programmatic access.
        
        Returns:
            tuple: (total_stripes, total_blocks, total_bytes, stripe_metrics, block_metrics)
        """
        # Stripe build time metrics
        if self.stripe_build_times:
            total_read_time = sum(self.read_times)
            total_encode_time = sum(self.encoding_times)
            total_stripe_time = sum(self.stripe_build_times)
            
            avg_read_time = total_read_time / len(self.read_times) if self.read_times else 0
            avg_encode_time = total_encode_time / len(self.encoding_times) if self.encoding_times else 0
            avg_stripe_time = total_stripe_time / len(self.stripe_build_times)
            
            stripe_metrics = {
                'total_stripes': len(self.stripe_build_times),
                'total_stripe_time': total_stripe_time,
                'avg_stripe_time': avg_stripe_time,
                'total_read_time': total_read_time,
                'avg_read_time': avg_read_time,
                'total_encode_time': total_encode_time,
                'avg_encode_time': avg_encode_time,
                'stripe_times_list': self.stripe_build_times.copy(),
                'read_times_list': self.read_times.copy(),
                'encode_times_list': self.encoding_times.copy()
            }
        else:
            stripe_metrics = {
                'total_stripes': 0,
                'total_stripe_time': 0,
                'avg_stripe_time': 0,
                'total_read_time': 0,
                'avg_read_time': 0,
                'total_encode_time': 0,
                'avg_encode_time': 0,
                'stripe_times_list': [],
                'read_times_list': [],
                'encode_times_list': []
            }
        
        # Block write metrics
        if self.metrics_report:
            total_latency = sum(m['lat'] for m in self.metrics_report)
            avg_latency = total_latency / len(self.metrics_report)
            total_throughput = sum(m['tp'] for m in self.metrics_report)
            avg_throughput = total_throughput / len(self.metrics_report)
            
            block_metrics = {
                'total_blocks': len(self.metrics_report),
                'total_latency': total_latency,
                'avg_latency': avg_latency,
                'total_throughput': total_throughput,
                'avg_throughput': avg_throughput,
                'block_details': self.metrics_report.copy()
            }
        else:
            block_metrics = {
                'total_blocks': 0,
                'total_latency': 0,
                'avg_latency': 0,
                'total_throughput': 0,
                'avg_throughput': 0,
                'block_details': []
            }
        
        # Network throughput metrics
        if self.network_throughput_measurements:
            total_net_tp = sum(m['network_throughput_mbps'] for m in self.network_throughput_measurements)
            avg_net_tp = total_net_tp / len(self.network_throughput_measurements)
            
            # Network throughput per node
            node_network_tps = {}
            for m in self.network_throughput_measurements:
                node = m['node']
                if node not in node_network_tps:
                    node_network_tps[node] = []
                node_network_tps[node].append(m['network_throughput_mbps'])
            
            network_metrics = {
                'total_tests': len(self.network_throughput_measurements),
                'total_network_throughput': total_net_tp,
                'avg_network_throughput': avg_net_tp,
                'node_throughputs': {node: {'avg': sum(tps)/len(tps), 'count': len(tps), 'measurements': tps} 
                                  for node, tps in node_network_tps.items()},
                'measurements': self.network_throughput_measurements.copy()
            }
        else:
            network_metrics = {
                'total_tests': 0,
                'total_network_throughput': 0,
                'avg_network_throughput': 0,
                'node_throughputs': {},
                'measurements': []
            }
        
        # Overall metrics
        overall_metrics = {
            'total_stripes_processed': self.total_stripes_processed,
            'total_bytes_read': self.total_bytes_read,
            'total_execution_time': self.total_execution_time,
            'written_block_ids': self.written_block_ids.copy()
        }
        
        return (
            overall_metrics,
            stripe_metrics,
            block_metrics,
            network_metrics
        )

    def _process_stripe(self, stripe_index, data_blocks, placements):
        #print(f"Processing stripe {stripe_index}")
        #logging.info(f"Processing stripe {stripe_index}")
        encode_start = time.time()
        blocks = self.encoder.encode(data_blocks)
        encode_time = time.time() - encode_start
        local_ids = []

        for block_data, placement in zip(blocks, placements):
            node = placement.node.node
            node_addr = f"{node.hostname}:{node.port}"
            client = DataNodeClient(
                node.hostname,
                node.port
            )
            
            # Measure network throughput for this block transfer
            network_metrics = client.measure_network_throughput(
                test_size_mb=max(1, int(len(block_data) / (1024 * 1024)))  # Use block size or 1MB minimum
            )
            
            response = client.write_block(placement.block_id, block_data)
            
            if not response.status.success:
                raise Exception(f"Block write failed: {response.status.message}")
            
            metrics = response.metrics
            
            self.metrics_report.append({
                'id': placement.block_id,
                'node': node_addr,
                'lat': metrics.latency_seconds,
                'tp': metrics.throughput_mbs,
                'net_tp': network_metrics['network_throughput_mbps']  # Add network throughput
            })
            
            # Store network throughput measurements separately
            self.network_throughput_measurements.append({
                'block_id': placement.block_id,
                'node': node_addr,
                'network_throughput_mbps': network_metrics['network_throughput_mbps'],
                'test_size_mb': network_metrics['test_size_mb'],
                'transfer_time_seconds': network_metrics['transfer_time_seconds']
            })
            
            #log_msg = (f"[Metric] Block {placement.block_id} write successful ->. "
            #           f"Latency: {metrics.latency_seconds:.4f}s, "
            #           f"Throughput: {metrics.write_throughput_mb_per_sec:.2f} MB/s")
            #logging.info(log_msg)
            
            local_ids.append(placement.block_id)
        return local_ids, encode_time

    def run(self, file_path):

        file_name = os.path.basename(file_path)

        self.written_block_ids = []

        start_time = time.time()

        if self.mode == "single":

            res = self._run_single_threaded(file_path)

        elif self.mode == "parallel":

            res = self._run_parallel(file_path)

        elif self.mode == "block_parallel":

            res = self._run_block_parallel(file_path)

        else:

            raise Exception(
                f"Invalid pipeline mode: {self.mode}"
            )

        self.total_execution_time = (
            time.time() - start_time
        )

        self._print_table(
            file_name,
            file_path
        )

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
                encode_start = time.time()
                blocks = self.encoder.encode(data_blocks)
                self.encoding_times.append(time.time() - encode_start)
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
                    
                    # Measure network throughput for this block transfer
                    network_metrics = client.measure_network_throughput(
                        test_size_mb=max(1, int(len(block_data) / (1024 * 1024)))  # Use block size or 1MB minimum
                    )
                    
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
                        "tp": metrics.throughput_mbs,
                        "net_tp": network_metrics['network_throughput_mbps']  # Add network throughput
                    })
                    
                    # Store network throughput measurements separately
                    self.network_throughput_measurements.append({
                        'block_id': placement.block_id,
                        'node': f"{node.hostname}:{node.port}",
                        'network_throughput_mbps': network_metrics['network_throughput_mbps'],
                        'test_size_mb': network_metrics['test_size_mb'],
                        'transfer_time_seconds': network_metrics['transfer_time_seconds']
                    })
                    #print(f"      >> Metric: Latency={metrics.latency_seconds:.4f}s, Disk Speed={metrics.throughput_mbs:.2f} MB/s")
                    # log_msg = (f"[Metric] Block {block_id} write successful ->. "
                    #            f"Latency: {metrics.latency_seconds:.4f}s, "
                    #            f"Throughput: {metrics.throughput_mbs:.2f} MB/s")
                    # logging.info(log_msg)
                    self.written_block_ids.append(block_id)
                stripe_index += 1
                
        finally: 
            self.read_times = builder.get_stripe_build_times()
            self.stripe_build_times = [r + e for r, e in zip(self.read_times, self.encoding_times)]
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
            f"workers={self.max_workers}",
            flush=True
        )

        builder = StripeBuilder(file_path)

        executor = ThreadPoolExecutor(
            max_workers=self.max_workers
        )

        active_futures = set()

        stripe_index = 0

        total_stripes = len(
            self.block_groups
        )

        try:

            while (
                stripe_index < total_stripes
                and len(active_futures)
                < self.max_workers
            ):

                data_blocks = (
                    builder.next_stripe()
                )

                if data_blocks is None:
                    break

                placements = (
                    self.block_groups[
                        stripe_index
                    ].placement
                )

                print(
                    f"Dispatching stripe "
                    f"{stripe_index}",
                    flush=True
                )

                future = executor.submit(
                    self._process_stripe,
                    stripe_index,
                    data_blocks,
                    placements
                )

                future.stripe_id = (
                    stripe_index
                )

                active_futures.add(
                    future
                )

                stripe_index += 1

            while active_futures:

                done, active_futures = wait(
                    active_futures,
                    return_when=
                    FIRST_COMPLETED
                )

                for finished in done:

                    stripe_id = (
                        finished.stripe_id
                    )

                    block_ids, encode_time = (
                        finished.result()
                    )

                    print(
                        f"Stripe "
                        f"{stripe_id} "
                        f"completed",
                        flush=True
                    )

                    self.written_block_ids.extend(
                        block_ids
                    )
                    self.encoding_times.append(encode_time)

                    if stripe_index < total_stripes:

                        data_blocks = (
                            builder.next_stripe()
                        )

                        if data_blocks is not None:

                            placements = (
                                self.block_groups[
                                    stripe_index
                                ].placement
                            )

                            print(
                                f"Dispatching stripe "
                                f"{stripe_index}",
                                flush=True
                            )

                            new_future = (
                                executor.submit(
                                    self._process_stripe,
                                    stripe_index,
                                    data_blocks,
                                    placements
                                )
                            )

                            new_future.stripe_id = (
                                stripe_index
                            )

                            active_futures.add(
                                new_future
                            )

                            stripe_index += 1

        finally:

            self.stripe_build_times = builder.get_stripe_build_times()
            builder.close()

            executor.shutdown()

            print(
                "Parallel pipeline finished",
                flush=True
            )

        return self.written_block_ids

    def _run_block_parallel(self, file_path):

        print(
            "Running block-parallel pipeline",
            flush=True
        )

        builder = StripeBuilder(
            file_path
        )

        stripe_index = 0

        total_stripes = len(
            self.block_groups
        )

        try:

            while True:

                data_blocks = (
                    builder.next_stripe()
                )

                if data_blocks is None:
                    break

                if stripe_index >= total_stripes:

                    raise Exception(
                        "More stripes processed "
                        "than allocated"
                    )

                placements = (
                    self.block_groups[
                        stripe_index
                    ].placement
                )

                print(
                    f"Processing stripe "
                    f"{stripe_index}",
                    flush=True
                )

                block_ids, encode_time = (
                    self.parallel_writer
                    .process_stripe(
                        stripe_index,
                        data_blocks,
                        placements
                    )
                )

                self.written_block_ids.extend(
                    block_ids
                )
                self.encoding_times.append(encode_time)

                stripe_index += 1

        finally:

            self.read_times = builder.get_stripe_build_times()
            self.stripe_build_times = [r + e for r, e in zip(self.read_times, self.encoding_times)]
            builder.close()

            print(
                "Block-parallel pipeline finished",
                flush=True
            )

        return self.written_block_ids


        