from .config_loader import (
    BLOCK_SIZE,
    K
)


class StripeBuilder:

    def __init__(
        self,
        file_path
    ):

        self.file = open(
            file_path,
            "rb"
        )
    def next_stripe(self):
        blocks = []

        bytes_read_total = 0

        for _ in range(K):

            data = self.file.read(BLOCK_SIZE)

            if not data:

                break

            bytes_read_total += len(data)

            if len(data) < BLOCK_SIZE:

                data = data.ljust(
                    BLOCK_SIZE,
                    b"\x00"
                )

            blocks.append(data)

        # No data at all → end of file
        if bytes_read_total == 0:
            return None

        # Partial stripe → pad remaining blocks
        while len(blocks) < K:

            blocks.append(
                b"\x00" * BLOCK_SIZE
            )

        return blocks

    def close(self):

        self.file.close()