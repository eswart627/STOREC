import time
from .config_loader import (BLOCK_SIZE, K)

class StripeBuilder:
    def __init__(self,file_path):
        self.file = open(file_path,"rb")
        self.stripe_build_times=[]

    def next_stripe(self):
        start_time = time.time()
        blocks = []
        bytes_read_total = 0

        for _ in range(K):
            data = self.file.read(BLOCK_SIZE)
            if not data:
                break
            bytes_read_total += len(data)
            if len(data) < BLOCK_SIZE:
                data = data.ljust(BLOCK_SIZE,b"\x00")
            blocks.append(data)
            
        # No data at all → end of file
        if bytes_read_total == 0:
            return None

        # Partial stripe → pad remaining blocks
        while len(blocks) < K:
            blocks.append(b"\x00" * BLOCK_SIZE)

        end_time = time.time()
        self.stripe_build_times.append(end_time - start_time)
        return blocks

    def get_stripe_build_times(self):
        return self.stripe_build_times

    def get_average_stripe_build_time(self):
        if not self.stripe_build_times:
            return 0
        return sum(self.stripe_build_times) / len(self.stripe_build_times)

    def close(self):
        self.file.close()