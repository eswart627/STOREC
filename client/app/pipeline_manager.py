from .stripe_builder import StripeBuilder
from .encoder import Encoder
from .transfer import DataNodeClient
import os

from .config_loader import (
    K,
    M,
    BLOCK_SIZE
)

class PipelineManager:

    def __init__(self,block_groups):
        self.block_groups = block_groups
        self.encoder = Encoder()
        self.written_block_ids = []
        # for stripe encoding verification
        self.total_stripes_processed = 0
        self.total_bytes_read =0
    def run(self, file_path):
        file_size = os.path.getsize(file_path)
        expected_stripes = len(self.block_groups)
        print(f"\nExpected stripes from NameNode: {expected_stripes}\n")
        builder = StripeBuilder(file_path)
        stripe_index = 0
        while True:
            data_blocks = (builder.next_stripe())
            if data_blocks is None:
                break
            # for stripe encoding verification
            self.total_stripes_processed += 1
            stripe_bytes= sum(len(b) for b in data_blocks)
            self.total_bytes_read += stripe_bytes
            print(f"Processing stripe {stripe_index}")
            print(
                f"  Data blocks read: {len(data_blocks)} "
                f"(expected {K})"
            )
            blocks = self.encoder.encode(data_blocks)
            print(
                f"  Blocks after encoding: {len(blocks)} "
                f"(expected {K + M})"
            )
            stripe = self.block_groups[stripe_index]
            placements = stripe.placement
            if len(placements) != len(blocks):

                raise Exception(
                    "Placement count mismatch "
                    f"{len(placements)} vs {len(blocks)}"
                )
            print(
                f"  Placement entries: {len(placements)}"
            )
            # for placement in placements:
            #     block_id = placement.block_id
            #     node = placement.node
            #     print(f"Simulating write:"
            #           f"{block_id} ->"
            #           f"{node.node.hostname}:{node.node.port}"
            #           )
            #     self.written_block_ids.append(block_id)
            # stripe_index += 1
            # for block_data, placement in zip(blocks, placements):
            #     block_id = placement.block_id
            #     node = placement.node
            #     client = DataNodeClient(
            #         node.address,
            #         node.port
            #     )
            #     success = client.write_block(
            #         block_id,
            #         block_data
            #     )

            #     if not success:
            #         raise Exception("Block write failed")
                
            #     self.written_block_ids.append(block_id)
            # stripe_index += 1
            for block_data, placement in zip(
                blocks,
                placements
            ):

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

                if block_size != BLOCK_SIZE:

                    raise Exception(
                        "Invalid block size detected"
                    )

                self.written_block_ids.append(
                    block_id
                )

            stripe_index += 1
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

        if self.total_stripes_processed != expected_stripes:

            print()
            print(
                "WARNING: stripe count mismatch "
                "(possible last-stripe padding case)"
            )

        print()
        return self.written_block_ids