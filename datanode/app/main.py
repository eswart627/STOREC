import os
import time
from dotenv import load_dotenv
#from STOREC.datanode.app import heartbeat
from datanode.app.config_loader import Config
from datanode.app.logger import Logger
from datanode.app.storage_manager import StorageManager
from datanode.app.server import DataNodeServer
from datanode.app.rpc_client import RPCClient
from datanode.app.registration import RegistrationManager
from datanode.app.heartbeat import HeartbeatManager

def main():
    # This gets the 'storec' directory where you run the command from
    base_dir = os.getcwd()
    load_dotenv(os.path.join(base_dir, "datanode", ".env"))
    print(f"Base directory: {base_dir}", flush=True)  # Debug print to check base_dir value
    
    # 1. Load Config
    config_path = os.path.join(base_dir, "datanode", "config", "datanode.config")
    config = Config(path=config_path)

    # 2. Initialize Logger 
    # REMOVED (base_dir) because the new Logger handles its own path!
    logger = Logger() 
    
    logger.log(
        "STARTUP", 
        f"node={config.node_id}"
    )

    # 3. Setup Storage
    #storage_root = os.path.join(base_dir, "datanode", config.data_dir)
    storage_root  = "/data"
    print(f"Storage root: {storage_root}", flush=True)  # Debug print to check storage_root value
    
    storage = StorageManager(base_dir=storage_root)
    storage.initialize()
    logger.log("STORAGE_INIT", storage_root)

    # 4. Networking & Server
    rpc = RPCClient(config, logger)
    rpc.connect()

    registration = RegistrationManager(
        rpc_client=rpc,
        config=config,
        logger=logger
    )
    registration.register()
    print(f"DEBUG: Registration complete. Assigned ID: {config.node_id}", flush=True)

    print("Starting DataNode server", flush=True)  # Debug print before server starts
    server = DataNodeServer(
        config=config, 
        logger=logger,
        storage=storage
    )
    server.start()

    print("Starting HeartbeatManager", flush=True)  # Debug print before heartbeat starts
    heartbeat = HeartbeatManager(
    rpc_client=rpc,
    config=config,
    logger=logger,
    storage=storage
    )
    heartbeat.start()
    
    print("DataNode is fully operational. Press Ctrl+C to stop.", flush=True)
    try:
        while True:
            time.sleep(1)  # Just sit here and wait
    except KeyboardInterrupt:
        print("\nShutting down DataNode...", flush=True)
        server.stop()

if __name__ == "__main__":
    main()
