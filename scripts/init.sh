#!/bin/bash
set -e

cleanup() {
    echo "Stopping database..."
    cd C:\\4Files\\BTP\\eject_button\\STOREC\\namenode
    docker-compose down
}

trap cleanup EXIT
trap cleanup INT
trap cleanup TERM

cd C:\\4Files\\BTP\\eject_button\\STOREC\\namenode
docker-compose up -d
sleep 5
cd C:\\4Files\\BTP\\eject_button\\STOREC
ipconfig
echo \n\n\n
echo "Initializing database..."
py -m namenode.db_manager.init-db
echo "Database initialized successfully!"
echo "Starting NameNode..."
py -m namenode.app.main

