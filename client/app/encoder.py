from reedsolo import RSCodec

from .config_loader import (BLOCK_SIZE, CELL_SIZE, K, M)


class Encoder:
    def __init__(self):
        self.rs = RSCodec(M)

    def encode(self, data_blocks):
        if len(data_blocks) != K:
            raise ValueError("Invalid number of data blocks")

        parity_blocks = [bytearray(BLOCK_SIZE) for _ in range(M)]

        offset = 0
        while offset < BLOCK_SIZE:
            end = min(offset + CELL_SIZE, BLOCK_SIZE)
            chunk_length = end - offset

            # Extract cell slice from each block
            cell_chunks = [block[offset:end] for block in data_blocks]

            # Encode each byte position in the cell
            for i in range(chunk_length):
                data_vector = bytes(chunk[i] for chunk in cell_chunks)
                encoded = self.rs.encode(data_vector)
                parity_bytes = encoded[-M:]
                for p in range(M):
                    parity_blocks[p][offset + i] = parity_bytes[p]
            offset = end
        return data_blocks + [bytes(p) for p in parity_blocks]