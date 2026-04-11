#!/bin/bash
set -e
# Configuration - Relative to the Project Root
PROTO_SRC_DIR="./proto"  # Where your .proto source files are
OUTPUT_DIR="./proto"      # Where you want the .py files to land

# 1. Create output dir and make it a package
mkdir -p $OUTPUT_DIR
touch $OUTPUT_DIR/__init__.py

echo "Compiling protos from $PROTO_SRC_DIR to $OUTPUT_DIR..."

# 2. Run compiler
# We use python3 -m grpc_tools.protoc
py -m grpc_tools.protoc \
    -I $PROTO_SRC_DIR \
    --python_out=$OUTPUT_DIR \
    --grpc_python_out=$OUTPUT_DIR \
    --pyi_out=$OUTPUT_DIR \
    $PROTO_SRC_DIR/*.proto

# 3. Ubuntu-specific sed
# This fixes the imports inside the dfs/proto folder
echo "Fixing imports for Ubuntu..."
sed -i 's/^import \(.*_pb2\)/from . import \1/g' $OUTPUT_DIR/*.py

echo "Done! Generated files are in $OUTPUT_DIR"