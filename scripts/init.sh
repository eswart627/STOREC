#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMENODE_DIR="${PROJECT_ROOT}/namenode"

if command -v docker >/dev/null 2>&1; then
    DOCKER_CMD=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
    DOCKER_CMD=(docker-compose)
else
    echo "Docker Compose is not installed or not in PATH."
    exit 1
fi

if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD=(python3)
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD=(python)
elif command -v py >/dev/null 2>&1; then
    PYTHON_CMD=(py)
else
    echo "Python is not installed or not in PATH."
    exit 1
fi

cleanup() {
    echo "Stopping database..."
    (
        cd "${NAMENODE_DIR}"
        "${DOCKER_CMD[@]}" down
    )
}

trap cleanup EXIT INT TERM

(
    cd "${NAMENODE_DIR}"
    "${DOCKER_CMD[@]}" up -d
)

sleep 5

echo "Initializing database..."
(
    cd "${PROJECT_ROOT}"
    "${PYTHON_CMD[@]}" -m namenode.db_manager.init-db
)

echo "Starting NameNode..."
(
    cd "${PROJECT_ROOT}"
    "${PYTHON_CMD[@]}" -m namenode.app.main
)
