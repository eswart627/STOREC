from concurrent.futures import ThreadPoolExecutor
import os

from .stripe_builder import StripeBuilder
from .encoder import Encoder
from .transfer import DataNodeClient
from .config_loader import (K, M, BLOCK_SIZE)

class PipelineManager:
    def __init__(self,block_groups, mode="single", max_workers=4):
        self.block_groups = block_groups
        self.mode = mode
        self.max_workers = max_workers

        self.encoder = Encoder()
        self.written_block_ids = []
        self.max_workers = self.max_workers

        # for stripe encoding verification
        self.total_stripes_processed = 0
        self.total_bytes_read =0

    def _process_stripe(self, stripe_index, data_blocks, placements):
        print(f"Processing stripe {stripe_index}")
        blocks = self.encoder.encode(data_blocks)
        local_ids = []

        for block_data, placement in zip(blocks, placements):
            node = placement.node.node
            client = DataNodeClient(
                node.hostname,
                node.port
            )
            success = client.write_block(
                placement.block_id,
                block_data
            )
            if not success:
                raise Exception("Block write failed")
            local_ids.append(placement.block_id)
        return local_ids
    
    def run(self, file_path):
        self.written_bocl_ids=[]
        if self.mode == "single":
            return self._run_single_threaded(file_path)
        if self.mode == "parallel":
            return self._run_parallel(file_path)
        raise Exception("Invalid pipeline mode")

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
                    success = client.write_block(
                        block_id,
                        block_data
                    )

                    if not success:
                        raise Exception("Block write failed")
                    
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
    