from datetime import datetime

from .registry import DataNodeRegistry


def _format_heartbeat(epoch: int) -> str:
    if not epoch:
        return "never"
    value = str(epoch)
    if len(value) == 14 and value.isdigit():
        try:
            return datetime.strptime(value, "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    return datetime.fromtimestamp(epoch).strftime("%Y-%m-%d %H:%M:%S")


def show_lookups() -> None:
    registry = DataNodeRegistry()
    restored_count = registry.load_state()

    print(f"Loaded {restored_count} node(s) from DB into debug registry.")
    print()

    print("Lookup 1: node_id -> node details")
    print("-" * 72)
    if not registry.nodes:
        print("No nodes found.")
    else:
        for node_id, node_data in registry.list_nodes().items():
            print(
                f"{node_id} -> "
                f"host={node_data['hostname']}, "
                f"port={node_data['port']}, "
                f"status={node_data['status']}, "
                f"capacity={node_data['capacity']}, "
                f"used={node_data.get('used', 0)}, "
                f"available={node_data.get('available', node_data['capacity'])}, "
                f"last_heartbeat={_format_heartbeat(node_data['last_heartbeat'])}"
            )

    print()
    print("Lookup 2: ip:port -> node_id")
    print("-" * 72)
    if not registry.lookup:
        print("No address mappings found.")
    else:
        for address, node_id in registry.lookup.items():
            print(f"{address} -> {node_id}")

    print()
    print("Note: this script reconstructs the lookups from DB state.")
    print("It does not attach to a running NameNode process's live Python memory.")


if __name__ == "__main__":
    show_lookups()
