import subprocess
import os
import argparse
import configparser
import signal
import sys


processes = []


def read_config(config_path):
    parser = configparser.ConfigParser()
    parser.read(config_path)
    hostname = parser.get(
        "NODE",
        "hostname"
    )
    data_dir = parser.get(
        "STORAGE",
        "data_dir"
    )
    return hostname, data_dir

def start_datanode(port, hostname, data_dir):
    env = os.environ.copy()
    env["NODE_PORT"] = str(port)
    env["NODE_HOSTNAME"] = hostname
    env["DATA_DIR"] = data_dir
    command =  (f'cmd /k python -m datanode.app.main')
    print(
        f"Starting DataNode "
        f"on port {port}"
    )
    subprocess.Popen(
        ["python", "-m", "datanode.app.main"],
        env=env,
        creationflags=subprocess.CREATE_NEW_CONSOLE
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--count",
        type=int,
        default=3
    )
    parser.add_argument(
        "--start-port",
        type=int,
        default=50052
    )
    parser.add_argument(
        "--config",
        default="datanode/config/datanode.config"
    )

    args = parser.parse_args()
    hostname, data_dir = read_config(args.config)

    print(f"Using hostname: {hostname}")
    print(f"Using data_dir: {data_dir}")

    for i in range(args.count):
        port = (args.start_port + i)
        start_datanode(port,hostname,data_dir)
    print("All DataNodes started")

if __name__ == "__main__":
    main()