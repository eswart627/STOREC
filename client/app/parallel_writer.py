from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed
)

from .transfer import DataNodeClient


class ParallelStripeWriter:
    """
    Handles block-level parallel writes for a single stripe.

    Responsibilities:
        - Encode stripe
        - Dispatch block writes in parallel
        - Collect metrics
        - Return written block IDs

    This module is intentionally isolated so that:
        - single pipeline remains unchanged
        - pipeline manager remains simple
        - block parallelism can be tuned independently
    """

    # Safe default limit
    # Typically equals K + M
    MAX_BLOCK_WORKERS = 8

    def __init__(
        self,
        encoder,
        metrics_report
    ):
        """
        Parameters
        ----------
        encoder : Encoder
            Erasure coding encoder instance

        metrics_report : list
            Shared list for collecting block metrics
        """

        self.encoder = encoder
        self.metrics_report = metrics_report

    # ---------------------------------------------------------

    def process_stripe(
        self,
        stripe_index,
        data_blocks,
        placements
    ):
        """
        Process a single stripe with parallel block writes.
        """

        print(
            f"Processing stripe {stripe_index}",
            flush=True
        )

        # --------------------------------------------
        # Encode stripe
        # --------------------------------------------

        blocks = self.encoder.encode(
            data_blocks
        )

        if len(blocks) != len(placements):
            raise Exception(
                "Block / placement mismatch "
                f"{len(blocks)} vs {len(placements)}"
            )

        local_ids = []

        # --------------------------------------------
        # Determine worker count
        # --------------------------------------------

        worker_count = min(
            len(blocks),
            self.MAX_BLOCK_WORKERS
        )

        # --------------------------------------------
        # Parallel block writes
        # --------------------------------------------

        with ThreadPoolExecutor(
            max_workers=worker_count
        ) as executor:

            futures = []

            for block_data, placement in zip(
                blocks,
                placements
            ):

                future = executor.submit(
                    self._write_block,
                    placement,
                    block_data
                )

                futures.append(future)

            # Collect results as they complete

            for future in as_completed(
                futures
            ):

                block_id = future.result()

                local_ids.append(
                    block_id
                )

        print(
            f"Stripe {stripe_index} completed",
            flush=True
        )

        return local_ids

    def _write_block(
        self,
        placement,
        block_data
    ):
        """
        Write a single block to a DataNode.
        """

        node = placement.node.node

        hostname = node.hostname
        port = node.port

        block_id = placement.block_id

        client = DataNodeClient(
            hostname,
            port
        )

        response = client.write_block(
            block_id,
            block_data
        )

        if not response.status.success:

            raise Exception(
                "Block write failed: "
                f"{response.status.message}"
            )

        # --------------------------------------------
        # Metrics collection
        # --------------------------------------------

        metrics = response.metrics

        self.metrics_report.append({
            "id": block_id,
            "node": f"{hostname}:{port}",
            "lat": metrics.latency_seconds,
            "tp": metrics.throughput_mbs
        })

        return block_id