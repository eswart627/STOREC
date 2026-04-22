import os

class StorageManager:
    def __init__(self, base_dir:str):
        self.base_dir = os.path.abspath(base_dir)
        self.chunks_dir = os.path.join(
            self.base_dir, "chunks"
        )
        self.tmp_dir = os.path.join(
            self.base_dir, "tmp"
        )
        print(self.base_dir, flush=True)
        
    def initialize(self):
        os.makedirs(self.base_dir, exist_ok=True)
        os.makedirs(self.chunks_dir, exist_ok=True)
        os.makedirs(self.tmp_dir, exist_ok=True)
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
        
    def get_used_bytes(self):
                total = 0
                if not os.path.exists(self.chunks_dir):
                    return 0
                for name in os.listdir(self.chunks_dir):
                    path = os.path.join(
                        self.chunks_dir,
                        name
                    )
                    if os.path.isfile(path):
                        total += os.path.getsize(path)
                return total