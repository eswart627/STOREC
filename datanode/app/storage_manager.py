import os

class StorageManager:
    def __init__(self, base_dir:str = "/data"):
        self.base_dir = os.path.abspath(base_dir)
        self.chunks_dir = os.path.join(
            base_dir, "chunks"
        )
        self.tmp_dir = os.path.join(
            base_dir, "tmp"
        )
        
        print(self.base_dir, flush=True)
        
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
        try:
            with open(test_file, "w") as f:
                f.write("ok")
            os.remove(test_file) # Un-commented this to keep volume clean
        except Exception as e:
            print(f"Permissions Error: Cannot write to {self.base_dir}. Error: {e}")
            raise e