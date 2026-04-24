#!/bin/bash
set -e

# Function to detect Python command
detect_python() {
    if command -v python3 &> /dev/null; then
        echo "python3"
    else
        echo "python"
    fi
}

# Configuration - Relative to the Project Root
PROTO_SRC_DIR="./proto"  # Where your .proto source files are
OUTPUT_DIR="./proto"      # Where you want the .py files to land
PROJECT_ROOT="."         # Project root directory for absolute imports
source myenv/bin/activate

# Detect python command
PYTHON_CMD=$(detect_python)
echo "Using Python command: $PYTHON_CMD"

# 1. Create output dir and make it a package
mkdir -p $OUTPUT_DIR
touch $OUTPUT_DIR/__init__.py

echo "Compiling protos from $PROTO_SRC_DIR to $OUTPUT_DIR..."

# 2. Run compiler
# We use detected python command with PROJECT_ROOT for absolute imports
$PYTHON_CMD -m grpc_tools.protoc \
    -I $PROJECT_ROOT \
    --python_out=$OUTPUT_DIR \
    --grpc_python_out=$OUTPUT_DIR \
    --pyi_out=$OUTPUT_DIR \
    $PROTO_SRC_DIR/*.proto

# 3. Ubuntu-specific sed
# This fixes the imports inside the dfs/proto folder
echo "Fixing imports for Ubuntu..."
sed -i 's/^import \(.*_pb2\)/from . import \1/g' $OUTPUT_DIR/*.py

echo "Done! Generated files are in $OUTPUT_DIR"