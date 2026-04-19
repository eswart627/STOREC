import os
import time
from dotenv import load_dotenv
from datanode.app.config_loader import Config
from datanode.app.logger import Logger
from datanode.app.storage_manager import StorageManager
from datanode.app.server import DataNodeServer
from datanode.app.rpc_client import RPCClient
from datanode.app.registration import RegistrationManager
from datanode.app.heartbeat import HeartbeatManager

def main():
    base_dir = os.getcwd()
    load_dotenv(os.path.join(base_dir, "datanode", ".env"))
    print(f"Base directory: {base_dir}", flush=True)  # Debug print to check base_dir value
    
    # Load Config
    config_path = os.path.join(base_dir, "datanode", "config", "datanode.config")
    config = Config(path=config_path)
    print("Config loaded successfully", flush=True)

    # Networking & Server
    print("Connecting to NameNode", flush=True)
    rpc = RPCClient(config)
    rpc.connect()

    registration = RegistrationManager(
        rpc_client=rpc,
        config=config
    )
    registration.register()
    print(f"DEBUG: Registration complete. Assigned ID: {config.node_id}", flush=True)

    # storage
    storage_root = config.node_storage_dir
    print("Initializing stroage at: {storage_root}", flush=True)
    storage = StorageManager(base_dir=storage_root)
    storage.initialize()

    # logger
    logger = Logger(base_dir=storage_root)
    logger.log("STARTUP", f"node={config.node_id}")

    # start server
    print("Starting DataNode server", flush=True)  
    server = DataNodeServer(
        config=config, 
        logger=logger,
        storage=storage
    )
    server.start()

    # start heartbeat
    print("Starting HeartbeatManager", flush=True) 
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
            time.sleep(1) 
    except KeyboardInterrupt:
        print("\nShutting down DataNode...", flush=True)
        server.stop()

if __name__ == "__main__":
    main()
