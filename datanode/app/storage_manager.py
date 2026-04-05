import os

class StorageManager:
    def __init__(self, base_dir:str):
        self.chunks_dir = os.path.join(
            base_dir, "chunks"
        )
        self.tmp_dir = os.path.join(
            base_dir, "tmp"
        )
        
    def initialize(self):
        os.makedirs(
            self.chunks_dir, exist_ok=True
        )
        os.makedirs(
           self.tmp_dir, exist_ok=True
        )
        self._verify_write_access()

    def _verify_write_access(self):
        test_file = os.path.join(
            self.tmp_dir, "test.tmp"
        )
        with open(test_file, "w") as f:
            f.write("ok")
        # os.remove(test_file)