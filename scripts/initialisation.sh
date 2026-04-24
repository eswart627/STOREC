#!/usr/bin/env bash
set -euo pipefail

# Default values
COUNT=3
START_PORT=50052
CONFIG_FILE="datanode/config/datanode.config"
SKIP_DB=false

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
        --skip-db)
            SKIP_DB=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [--count COUNT] [--start-port START_PORT] [--config CONFIG_FILE] [--skip-db]"
            echo "  --count       Number of DataNodes to start (default: 3)"
            echo "  --start-port  Starting port number for DataNodes (default: 50052)"
            echo "  --config      Path to DataNode config file (default: datanode/config/datanode.config)"
            echo "  --skip-db     Skip database initialization (default: false)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Function to detect Python command
detect_python() {
    if command -v python3 &> /dev/null; then
        echo "python3"
    else
        echo "python"
    fi
}

echo "Initializing project..."
echo "DataNodes: $COUNT starting from port $START_PORT"
echo "Config file: $CONFIG_FILE"

source myenv/bin/activate

# Detect python command
PYTHON_CMD=$(detect_python)
echo "Using Python command: $PYTHON_CMD"

# Initialize database unless skipped
if [[ "$SKIP_DB" == "false" ]]; then
    echo "Initializing database..."
    $PYTHON_CMD -m namenode.db_manager.init-db
else
    echo "Skipping database initialization..."
fi

# Start NameNode in screen session
screen -dmS namenode $PYTHON_CMD -m namenode.app.main
echo "NameNode started in screen session 'namenode'"

sleep 3

# Start DataNodes with passed arguments
scripts/init_dns.sh --count "$COUNT" --start-port "$START_PORT" --config "$CONFIG_FILE"

echo "Initialization complete!"
echo "To attach to NameNode: screen -r namenode"
echo "To attach to a DataNode: screen -r datanode_<port>"
echo "To list all screen sessions: screen -ls"