#!/bin/bash
set -euo pipefail

# Default values
TARGET="all"
DN_TARGET="both"
NEW_IP=""
OLD_IP=""

# Config file paths
NAMENODE_CONFIG="namenode/config/namenode.config"
DATANODE_CONFIG="datanode/config/datanode.config"
CLIENT_CONFIG="client/config/client.config"

# Function to display usage
usage() {
    echo "Usage: $0 [OPTIONS] <new_ip>"
    echo ""
    echo "Options:"
    echo "  --target <target>     Which config to change (namenode|datanode|client|all) [default: all]"
    echo "  --dn-target <target>  For datanode: which IP to change (namenode|datanode|both) [default: both]"
    echo "  --old-ip <ip>         Replace specific old IP [default: auto-detect]"
    echo "  -h, --help            Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 192.168.1.100                    # Change all IPs to 192.168.1.100"
    echo "  $0 --target namenode 192.168.1.100  # Change only NameNode config"
    echo "  $0 --target datanode --dn-target namenode 192.168.1.100  # Change only NameNode host in DataNode config"
    echo "  $0 --old-ip 192.168.137.9 192.168.1.100  # Replace specific old IP"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --target)
            TARGET="$2"
            shift 2
            ;;
        --dn-target)
            DN_TARGET="$2"
            shift 2
            ;;
        --old-ip)
            OLD_IP="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        -*)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
        *)
            if [[ -z "$NEW_IP" ]]; then
                NEW_IP="$1"
            else
                echo "Error: Multiple new IPs provided"
                usage
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate new IP
if [[ -z "$NEW_IP" ]]; then
    echo "Error: New IP address is required"
    usage
    exit 1
fi

# Validate IP format (basic check)
if ! [[ "$NEW_IP" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Error: Invalid IP format: $NEW_IP"
    exit 1
fi

# Function to detect current IP in a file
detect_ip() {
    local file="$1"
    local pattern="$2"
    grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' "$file" | head -1
}

# Function to change IP in a file
change_ip_in_file() {
    local file="$1"
    local old_ip="$2"
    local new_ip="$3"
    local pattern="$4"
    
    if [[ ! -f "$file" ]]; then
        echo "Warning: File not found: $file"
        return 1
    fi
    
    echo "Updating $file:"
    echo "  Pattern: $pattern"
    echo "  Old IP: $old_ip"
    echo "  New IP: $new_ip"
    
    # Replace IP
    if sed -i "s/$old_ip/$new_ip/g" "$file"; then
        echo "  ✓ Updated successfully"
        return 0
    else
        echo "  ✗ Failed to update"
        return 1
    fi
}

# Function to change NameNode config
change_namenode_config() {
    echo "=== Updating NameNode Config ==="
    
    # Detect old IP if not specified
    if [[ -z "$OLD_IP" ]]; then
        OLD_IP=$(detect_ip "$NAMENODE_CONFIG" "hostname")
    fi
    
    if [[ -z "$OLD_IP" ]]; then
        echo "Error: Could not detect current IP in $NAMENODE_CONFIG"
        return 1
    fi
    
    change_ip_in_file "$NAMENODE_CONFIG" "$OLD_IP" "$NEW_IP" "hostname"
}

# Function to change DataNode config
change_datanode_config() {
    echo "=== Updating DataNode Config ==="
    
    local sections_to_change=()
    
    case "$DN_TARGET" in
        "namenode")
            sections_to_change=("namenode")
            ;;
        "datanode")
            sections_to_change=("datanode")
            ;;
        "both")
            sections_to_change=("datanode" "namenode")
            ;;
        *)
            echo "Error: Invalid dn-target: $DN_TARGET"
            return 1
            ;;
    esac
    
    for section in "${sections_to_change[@]}"; do
        if [[ "$section" == "datanode" ]]; then
            # Change DataNode hostname
            if [[ -z "$OLD_IP" ]]; then
                OLD_IP=$(detect_ip "$DATANODE_CONFIG" "hostname")
            fi
            change_ip_in_file "$DATANODE_CONFIG" "$OLD_IP" "$NEW_IP" "DataNode hostname"
        elif [[ "$section" == "namenode" ]]; then
            # Change NameNode host in DataNode config
            if [[ -z "$OLD_IP" ]]; then
                OLD_IP=$(detect_ip "$DATANODE_CONFIG" "host")
            fi
            change_ip_in_file "$DATANODE_CONFIG" "$OLD_IP" "$NEW_IP" "NameNode host"
        fi
    done
}

# Function to change Client config
change_client_config() {
    echo "=== Updating Client Config ==="
    
    # Detect old IP if not specified
    if [[ -z "$OLD_IP" ]]; then
        OLD_IP=$(detect_ip "$CLIENT_CONFIG" "namenode_address")
    fi
    
    if [[ -z "$OLD_IP" ]]; then
        echo "Error: Could not detect current IP in $CLIENT_CONFIG"
        return 1
    fi
    
    change_ip_in_file "$CLIENT_CONFIG" "$OLD_IP" "$NEW_IP" "namenode_address"
}

# Main execution
echo "IP Change Script"
echo "================"
echo "Target: $TARGET"
if [[ "$TARGET" == "datanode" ]]; then
    echo "DataNode Target: $DN_TARGET"
fi
echo "New IP: $NEW_IP"
if [[ -n "$OLD_IP" ]]; then
    echo "Old IP: $OLD_IP"
fi
echo ""

# Process based on target
case "$TARGET" in
    "namenode")
        change_namenode_config
        ;;
    "datanode")
        change_datanode_config
        ;;
    "client")
        change_client_config
        ;;
    "all")
        change_namenode_config
        change_datanode_config
        change_client_config
        ;;
    *)
        echo "Error: Invalid target: $TARGET"
        echo "Valid targets: namenode, datanode, client, all"
        exit 1
        ;;
esac

echo ""
echo "=== Summary ==="
echo "IP changes completed successfully!"
