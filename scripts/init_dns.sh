#!/bin/bash

# Linux version of init_dns.py for starting multiple DataNodes

# Default values
COUNT=3
START_PORT=50052
CONFIG_FILE="datanode/config/datanode.config"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --count)
            COUNT="$2"
            shift 2
            ;;
        --start-port)
            START_PORT="$2"
            shift 2
            ;;
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [--count COUNT] [--start-port START_PORT] [--config CONFIG_FILE]"
            echo "  --count       Number of DataNodes to start (default: 3)"
            echo "  --start-port  Starting port number (default: 50052)"
            echo "  --config      Path to DataNode config file (default: datanode/config/datanode.config)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Function to read config file
read_config() {
    local config_file=$1
    
    if [[ ! -f "$config_file" ]]; then
        echo "Error: Config file not found: $config_file"
        exit 1
    fi
    
    # Read hostname and data_dir from config
    hostname=$(grep "^hostname" "$config_file" | cut -d'=' -f2 | tr -d ' ')
    data_dir=$(grep "^data_dir" "$config_file" | cut -d'=' -f2 | tr -d ' ')
    
    if [[ -z "$hostname" ]]; then
        echo "Error: hostname not found in config file"
        exit 1
    fi
    
    if [[ -z "$data_dir" ]]; then
        echo "Error: data_dir not found in config file"
        exit 1
    fi
}

# Function to start a single DataNode
start_datanode() {
    local port=$1
    local hostname=$2
    local data_dir=$3
    
    echo "Starting DataNode on port $port"
    
    # Set environment variables
    export NODE_PORT="$port"
    export NODE_HOSTNAME="$hostname"
    export DATA_DIR="$data_dir"
    
    # Start DataNode in background
    python3 -m datanode.app.main &
    
    # Store process ID for cleanup
    echo $! >> /tmp/datanode_pids.txt
}

# Main execution
echo "Reading config from: $CONFIG_FILE"
read_config "$CONFIG_FILE"

echo "Using hostname: $hostname"
echo "Using data_dir: $data_dir"

# Clear any existing PID file
> /tmp/datanode_pids.txt

# Start DataNodes
for ((i=0; i<COUNT; i++)); do
    port=$((START_PORT + i))
    start_datanode "$port" "$hostname" "$data_dir"
    sleep 1  # Brief pause between starting nodes
done

echo "All DataNodes started"
echo "Process IDs saved in /tmp/datanode_pids.txt"
echo "To stop all DataNodes: kill \$(cat /tmp/datanode_pids.txt)"

# Optional: Wait for user input to stop
read -p "Press Enter to stop all DataNodes..."

# Stop all DataNodes
if [[ -f /tmp/datanode_pids.txt ]]; then
    echo "Stopping all DataNodes..."
    while read -r pid; do
        kill "$pid" 2>/dev/null
    done < /tmp/datanode_pids.txt
    rm -f /tmp/datanode_pids.txt
    echo "All DataNodes stopped"
fi