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

#source myenv/bin/activate

# Detect python command
PYTHON_CMD=$(detect_python)
echo "Using Python command: $PYTHON_CMD"

# Detect OS for background command
if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "cygwin" ]] || uname -s | grep -q "MINGW\|CYGWIN"; then
    BG_CMD="nohup"
    BG_ARGS="> namenode.log 2>&1 &"
    BG_ECHO="NameNode started in background (PID: $!)"
    FINAL_ECHO="NameNode log: namenode.log\nDataNode logs: datanode_<port>.log"
else
    BG_CMD="screen -dmS namenode"
    BG_ARGS=""
    BG_ECHO="NameNode started in screen session 'namenode'"
    FINAL_ECHO="To attach to NameNode: screen -r namenode\nTo attach to a DataNode: screen -r datanode_<port>\nTo list all screen sessions: screen -ls"
fi

# Initialize database unless skipped
if [[ "$SKIP_DB" == "false" ]]; then
    echo "Initializing database..."
    $PYTHON_CMD -m namenode.db_manager.init-db
else
    echo "Skipping database initialization..."
fi

# Start NameNode
$BG_CMD $PYTHON_CMD -m namenode.app.main $BG_ARGS
echo "$BG_ECHO"

sleep 3

# Start DataNodes with passed arguments
scripts/init_dns.sh --count "$COUNT" --start-port "$START_PORT" --config "$CONFIG_FILE"

echo "Initialization complete!"
echo -e "$FINAL_ECHO"