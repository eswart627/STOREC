import os

from datanode.app.config_loader import Config
from datanode.app.logger import Logger
from datanode.app.storage_manager import StorageManager
from datanode.app.server import DataNodeServer
from datanode.app.rpc_client import RPCClient
from datanode.app.registration import RegistrationManager

def main():
    base_dir = os.getcwd()

    config_path = os.path.join(
        base_dir, "datanode","config",
        "datanode.config"
    )
    config = Config(path=config_path)

    logger = Logger(base_dir)
    logger.log(
        "STARTUP", 
        f"node={config.node_id}"
    )

    storage_root = os.path.join(
        base_dir, "datanode",config.data_dir
    )
    storage = StorageManager(
        base_dir=storage_root
    )
    storage.initialize()
    logger.log("STORAGE_INIT", storage_root)

    rpc = RPCClient(config, logger)
    rpc.connect()

    registration = RegistrationManager(
        rpc_client=rpc,
        config=config,
        logger=logger
    )
    registration.register()

    server = DataNodeServer(
        config=config, 
        logger=logger,
        storage=storage
    )
    server.start()

if __name__ == "__main__":
    main()