# NameNode — Comprehensive Technical Report

> This document covers every implemented feature in the NameNode component of the Sappho distributed file system, derived from a full reading of the `namenode/` and `proto/` directories.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Directory Structure](#2-directory-structure)
3. [Protobuf Service Definition](#3-protobuf-service-definition)
4. [Initialisation & Startup](#4-initialisation--startup)
5. [Configuration](#5-configuration)
6. [Database Layer](#6-database-layer)
7. [DataNode Registry (In-Memory State)](#7-datanode-registry-in-memory-state)
8. [DataNode Registration](#8-datanode-registration)
9. [Heartbeat Mechanism](#9-heartbeat-mechanism)
10. [Health Checker (Dead-Node Detection)](#10-health-checker-dead-node-detection)
11. [RPC — Block Allocation (Write Path)](#11-rpc--block-allocation-write-path)
12. [RPC — Commit File](#12-rpc--commit-file)
13. [RPC — Get File Metadata (Read Path)](#13-rpc--get-file-metadata-read-path)
14. [RPC — Delete File](#14-rpc--delete-file)
15. [RPC — List Files](#15-rpc--list-files)
16. [RPC — Get Cluster Status](#16-rpc--get-cluster-status)
17. [Logging System](#17-logging-system)
18. [Graceful Shutdown](#18-graceful-shutdown)
19. [Docker & Deployment](#19-docker--deployment)
20. [DB Utility Scripts](#20-db-utility-scripts)

---

## 1. Architecture Overview

The NameNode is the **central metadata server** of a distributed file system built with **erasure-coded storage** (Reed-Solomon style `k` data + `m` parity blocks). It is responsible for:

- Tracking which DataNodes are alive and how much storage they have.
- Allocating blocks across DataNodes when a client wants to write a file.
- Persisting file → block → node mappings in a MySQL database.
- Serving metadata back to clients for reads and deletions.
- Exposing a cluster health dashboard via gRPC.

**Key technologies:** Python, gRPC, Protocol Buffers, MySQL (via PyMySQL), Docker.

---

## 2. Directory Structure

```
namenode/
├── app/
│   ├── __init__.py
│   ├── main.py              # Entry point — wires everything together
│   ├── server.py             # gRPC service + server wrapper
│   ├── registry.py           # In-memory DataNode registry
│   ├── allocation.py         # Block allocation & commit logic
│   ├── health_checker.py     # Background dead-node detector
│   ├── config_loader.py      # INI config reader
│   ├── logger.py             # File-based structured logger
│   └── constants.py          # Global constants (block_size)
├── config/
│   └── namenode.config       # Runtime configuration
├── db_manager/
│   ├── __init__.py            # Exports get_connection()
│   ├── connection.py          # PyMySQL connection factory
│   ├── init-db.py             # Schema bootstrap script
│   ├── nuke.py                # Drop-all-tables script
│   ├── fakenode.py            # Insert 10 fake DataNodes for testing
│   ├── check_tables.py        # Dump all table contents
│   └── test_connection.py     # Smoke-test DB connectivity
├── models/
│   ├── dn_table.sql           # DataNode table DDL
│   ├── filetable.sql          # File table DDL
│   └── metadata.sql           # Block metadata table DDL
└── docker-compose.yml         # MySQL container definition
```

---

## 3. Protobuf Service Definition

**File:** `proto/namenode.proto` — defines the `NameNodeService` with **8 RPCs**:

| RPC | Request | Response | Purpose |
|-----|---------|----------|---------|
| `RegisterDataNode` | `RegisterRequest` | `RegisterResponse` | DataNode registration |
| `SendHeartbeat` | `HeartbeatRequest` | `HeartbeatResponse` | Periodic liveness signal |
| `AllocateBlocks` | `AllocateBlocksRequest` | `AllocateBlocksResponse` | Assign blocks for a file write |
| `CommitFile` | `CommitFileRequest` | `CommitFileResponse` | Finalise file after successful write |
| `GetFileMetadata` | `GetFileMetadataRequest` | `GetFileMetadataResponse` | Retrieve block map for reading |
| `DeleteFile` | `DeleteFileRequest` | `DeleteFileResponse` | Remove a file's metadata |
| `ListFiles` | `ListFilesRequest` | `ListFilesResponse` | List all stored files |
| `GetClusterStatus` | `ClusterStatusRequest` | `ClusterStatusResponse` | Cluster health dashboard |

### Shared Messages (`proto/common.proto`)

| Message | Fields | Usage |
|---------|--------|-------|
| `Node` | `hostname`, `port` | Network address of a DataNode |
| `NodeId` | `node_id`, `Node` | Identifies a specific DataNode |
| `Status` | `success`, `message` | Generic RPC result status |
| `File` | `file_name`, `file_size` | File identity |
| `Block` | `block_id`, `data_bytes`, `block_size` | A single data block |
| `Placement` | `block_id`, `NodeId` | Where a block lives |
| `BlockGroups` | `stripe_id`, `repeated Placement` | One erasure-coded stripe |
| `HeartbeatInfo` | `node_id`, `timestamp`, `used_bytes`, `free_bytes` | Heartbeat payload |
| `Metrics` | `latency_seconds`, `throughput_mbs` | Performance metrics |

---

## 4. Initialisation & Startup

**File:** `namenode/app/main.py`

The boot sequence is:

```
1.  Register SIGINT / SIGTERM handlers for graceful shutdown
2.  Resolve config path:  namenode/config/namenode.config
3.  Load configuration  →  Config object
4.  Create Logger        →  clears all 4 log files
5.  Create DataNodeRegistry (empty in-memory dict)
6.  registry.load_state() → restores previously known DataNodes from MySQL
       - All restored nodes are marked INACTIVE until they re-register
7.  Create NameNodeServer(config, registry, logger, flag)
       - Spins up a gRPC server with ThreadPoolExecutor
       - Registers NameNodeService servicer
       - Binds to config.hostname:config.port
       - Creates HealthChecker (unless test flag is set)
8.  server.start()
       - Starts the gRPC server
       - Starts the HealthChecker background thread
       - Logs SERVER_START with IST timestamp
       - Blocks on server.wait_for_termination()
```

A `flag` argument (default `0`) can be passed via CLI to **disable the HealthChecker** for testing scenarios.

---

## 5. Configuration

**File:** `namenode/app/config_loader.py`  
**Config:** `namenode/config/namenode.config`

Uses Python's `configparser` to read an INI-format file:

```ini
[NODE]
hostname = 192.168.1.166
port = 50051

[SERVER]
worker_threads = 4
health_check_interval = 60
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `hostname` | string | — | Address the gRPC server binds to |
| `port` | int | — | Port the gRPC server listens on |
| `worker_threads` | int | — | ThreadPoolExecutor pool size |
| `health_check_interval` | int | `10` | Seconds between health checks (fallback default) |

---

## 6. Database Layer

### 6.1 Connection Factory

**File:** `namenode/db_manager/connection.py`

Reads credentials from a `.env` file (`DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`) and returns a `pymysql` connection.

### 6.2 Schema (3 Tables)

#### `dn_table` — DataNode Registry (persistent)

| Column | Type | Description |
|--------|------|-------------|
| `dn_index` | INT AUTO_INCREMENT PK | Row index |
| `dn_id` | VARCHAR(255) UNIQUE | UUID assigned by NameNode |
| `dn_address` | VARCHAR(255) | Hostname |
| `dn_port` | INT | Port |
| `dn_status` | VARCHAR(50) | `ACTIVE` or `INACTIVE` |
| `dn_last_heartbeat` | BIGINT | Unix epoch seconds |
| `dn_capacity` | BIGINT | Total bytes |
| `dn_used` | BIGINT | Used bytes |
| `dn_available` | BIGINT | Free bytes |

#### `file_table` — File Catalogue

| Column | Type | Description |
|--------|------|-------------|
| `file_name` | VARCHAR(255) PK | Unique file name |
| `size` | BIGINT | File size in KB |
| `block_count` | INT | Total blocks (data + parity) |
| `start_index` | INT | First `metadata_table.id` for this file |
| `data_blocks` | INT | `k` (data block count) |
| `parity_blocks` | INT | `m` (parity block count) |
| `created_at` | TIMESTAMP | Auto-set on insert |

#### `metadata_table` — Block-Level Metadata

| Column | Type | Description |
|--------|------|-------------|
| `id` | INT AUTO_INCREMENT PK | Row index |
| `file_id` | TEXT | File name (FK-like) |
| `stripe_id` | TEXT | Stripe identifier (`filename_N`) |
| `block_id` | TEXT | UUID of the block |
| `size` | INT | Block size in bytes |
| `node_id` | TEXT | DataNode that stores this block |

### 6.3 Docker MySQL

**File:** `namenode/docker-compose.yml`

Runs a MySQL container on port `3308→3306`, with `dn_table.sql` auto-executed on first boot via Docker entrypoint.

---

## 7. DataNode Registry (In-Memory State)

**File:** `namenode/app/registry.py` — class `DataNodeRegistry`

Maintains a **thread-safe, in-memory dictionary** of all known DataNodes alongside a reverse-lookup map.

### Data Structures

```python
self.nodes = {
    "<uuid>": {
        "hostname": "192.168.1.100",
        "port": 50052,
        "status": "ACTIVE",          # or "INACTIVE"
        "last_heartbeat": 1714700000, # unix epoch
        "capacity": 1000000000,       # total bytes
        "used": 0,
        "available": 1000000000,
        "assigned": False             # currently allocated but not yet committed
    }
}

self.lookup = {
    "192.168.1.100:50052": "<uuid>"   # address → node_id reverse map
}
```

### Methods

| Method | Description |
|--------|-------------|
| `register(node_id, hostname, port, capacity, mode)` | Add or reactivate a node. `mode=0` generates a new UUID; `mode=1` reuses existing ID. |
| `heartbeat(node_id, used, available)` | Updates timestamp, status→ACTIVE, and storage metrics. |
| `list_nodes()` | Returns a snapshot of all nodes (used by cluster status). |
| `check_node_health()` | Scans for nodes whose last heartbeat was >20 seconds ago and marks them INACTIVE. Returns list of newly dead node IDs. |
| `save_state()` | Persists all in-memory node data to `dn_table` via `INSERT ... ON DUPLICATE KEY UPDATE`. Sets all nodes to INACTIVE. |
| `load_state()` | Reads `dn_table` into memory. Marks all restored nodes INACTIVE. Returns count of loaded nodes. |
| `to_node(node_id)` | Converts an in-memory node entry to a `common_pb2.NodeId` protobuf message. |

All write methods are protected by `threading.Lock`.

---

## 8. DataNode Registration

**RPC:** `RegisterDataNode`  
**File:** `namenode/app/server.py` → `NameNodeService.RegisterDataNode`

### Protocol

```
DataNode → NameNode:  RegisterRequest { Node(hostname, port), capacity_bytes, node_id }
NameNode → DataNode:  RegisterResponse { node_id, Status }
```

### Three Registration Scenarios

```
┌─────────────────────────────────────────────────────────┐
│  node_id empty?                                         │
│  ├── YES → address in lookup?                           │
│  │   ├── YES → Reactivate existing node (mode=1)        │
│  │   │         Return existing node_id, "Already reg."  │
│  │   └── NO  → Brand new node (mode=0)                  │
│  │             Generate UUID, return it, "Registered"    │
│  └── NO  → Known node re-registering (mode=1)           │
│             Reactivate with same ID, "Re-registered"     │
└─────────────────────────────────────────────────────────┘
```

1. **New node (no `node_id`, unknown address):** A fresh UUID is generated via `registry.register(mode=0)` and returned to the DataNode, which should persist it locally.
2. **Returning node (no `node_id`, known address):** The lookup map finds the existing UUID. Node is reactivated with `mode=1`.
3. **Node with stored ID:** The DataNode sends its previously assigned UUID. Registry reactivates it directly.

---

## 9. Heartbeat Mechanism

**RPC:** `SendHeartbeat`  
**File:** `namenode/app/server.py` → `NameNodeService.SendHeartbeat`

### Protocol

```
DataNode → NameNode:  HeartbeatRequest { HeartbeatInfo(node_id, timestamp, used_bytes, free_bytes) }
NameNode → DataNode:  HeartbeatResponse { Status(success, "ACK for node X at T") }
```

### What Happens on Each Heartbeat

1. Log the received heartbeat to the maintenance log (mode 1).
2. Call `registry.heartbeat(node_id, used, available)` which:
   - Updates `last_heartbeat` to current epoch time.
   - Sets `status` → `"ACTIVE"`.
   - Updates `used` and `available` byte counts.
3. Return an ACK response.

If `node_id` is not found in the registry, `registry.heartbeat()` returns an error string (currently not surfaced back to the DataNode in the response).

---

## 10. Health Checker (Dead-Node Detection)

**File:** `namenode/app/health_checker.py` — class `HealthChecker`

A **daemon thread** that periodically scans the registry for dead nodes.

### Behaviour

```
Loop (every config.health_check_interval seconds):
  1. Call registry.check_node_health()
     → For each node where (now - last_heartbeat > 20s) AND status == "ACTIVE":
         Mark status = "INACTIVE", collect node_id
  2. For each dead node_id:
     → UPDATE dn_table SET dn_status = 'INACTIVE' WHERE dn_id = ?
     → Log NODE_INACTIVE and CONNECTION_CLOSED
```

- **Timeout threshold:** 20 seconds (hardcoded in `registry.py`).
- **Check interval:** Configurable via `health_check_interval` (default 60s).
- Can be **disabled** by passing `flag=1` to `main()` (test mode).

---

## 11. RPC — Block Allocation (Write Path)

**RPC:** `AllocateBlocks`  
**Files:** `server.py` → `NameNodeService.AllocateBlocks`, `allocation.py` → `AllocationManager`

This is the core write-path RPC. The client calls this before writing any data.

### Protocol

```
Client → NameNode:  AllocateBlocksRequest {
    file_details (file_name, file_size),
    stripe_size, data_blocks_k, parity_blocks_m
}
NameNode → Client:  AllocateBlocksResponse {
    repeated BlockGroups { stripe_id, repeated Placement { block_id, NodeId } }
}
```

### Allocation Algorithm

1. **Compute stripe count:** `ceil(file_size / stripe_size)`
2. **Compute shards per stripe:** `k + m` (data + parity blocks)
3. **Set erasure coding policy** on the AllocationManager.
4. **Get available nodes:** Filter registry for nodes where:
   - `status == "ACTIVE"`
   - `available >= block_size` (5 MB, from `constants.py`)
5. **Round-robin placement** across available nodes:
   ```
   For each stripe i (0 .. num_stripes-1):
       stripe_id = "{file_name}_{i}"
       For each shard j (0 .. num_shards-1):
           node_index = (cursor + j) % total_nodes
           block_id = new UUID
           Reserve: node.available -= block_size, node.assigned = True
       cursor = (cursor + num_shards) % total_nodes
   ```
6. **Build response** as a list of `BlockGroups`, each containing `Placement` entries mapping `block_id → NodeId`.

### In-Memory Allocation Tracking

Allocations are stored in a nested dict until committed:

```python
self.allocations = {
    "file.txt": {
        "file.txt_0": {  # stripe_id
            "block-uuid-1": "node-uuid-A",
            "block-uuid-2": "node-uuid-B",
            ...
        },
        "file.txt_1": { ... }
    }
}
```

### Constants

- **Block size:** 5 MB (`5 * 1024 * 1024` bytes) — defined in `constants.py`.

---

## 12. RPC — Commit File

**RPC:** `CommitFile`  
**File:** `server.py` → `NameNodeService.CommitFile`

Called by the client **after all blocks have been successfully written** to DataNodes.

### Protocol

```
Client → NameNode:  CommitFileRequest { file_details, total_blocks, block_ids[] }
NameNode → Client:  CommitFileResponse { Status }
```

### Commit Logic

1. **Get the next `metadata_table.id`** by querying `SELECT id FROM metadata_table ORDER BY id DESC LIMIT 1`.
2. **Commit blocks** via `allocation_manager.commit_block()`:
   - Looks up each `block_id` in the in-memory allocation map.
   - Inserts a row into `metadata_table` for each block: `(file_id, stripe_id, block_id, size, node_id)`.
   - Marks `node.assigned = False` (releases the reservation).
   - Validates that **all** requested block_ids were found; raises an exception if any are missing.
3. **Insert file record** into `file_table`: `(file_name, size_in_kb, block_count, start_index, data_blocks, parity_blocks)`.
4. **On any failure:** Rollback the DB transaction and remove the file from the in-memory allocations map.

---

## 13. RPC — Get File Metadata (Read Path)

**RPC:** `GetFileMetadata`  
**File:** `server.py` → `NameNodeService.GetFileMetadata`

Called by the client to retrieve the block map before reading a file.

### Protocol

```
Client → NameNode:  GetFileMetadataRequest { file_details (file_name, file_size) }
NameNode → Client:  GetFileMetadataResponse {
    file_details, stripe_size, data_blocks_k, parity_blocks_m,
    repeated BlockGroups { stripe_id, repeated Placement { block_id, NodeId } }
}
```

### Logic

1. Query `file_table` for the file's `data_blocks` (k) and `parity_blocks` (m). Falls back to `k=3, m=2` if not found.
2. Query `metadata_table` for all `(block_id, node_id)` rows matching the file.
3. Compute `blocks_per_stripe = k + m`.
4. Compute `stripe_size = file_size / num_stripes`.
5. Call `allocation_manager.send_metadata()` which groups the flat block list into `BlockGroups`:
   - Iterates through blocks, grouping every `blocks_per_stripe` blocks into a stripe.
   - Each stripe's `stripe_id` is `"{file_name}_{stripe_index}"`.
   - Each block is converted to a `Placement` with the DataNode's address resolved via `registry.to_node()`.

---

## 14. RPC — Delete File

**RPC:** `DeleteFile`  
**File:** `server.py` → `NameNodeService.DeleteFile`

### Protocol

```
Client → NameNode:  DeleteFileRequest { file_details (file_name) }
NameNode → Client:  DeleteFileResponse { Status }
```

### Logic

1. **Query all blocks** for the file from `metadata_table`: `(block_id, node_id, size)`.
2. **Delete metadata:** `DELETE FROM metadata_table WHERE file_id = ?`
3. **Delete file record:** `DELETE FROM file_table WHERE file_name = ?`
4. **Reset auto-increment** on `metadata_table` to `MAX(id) + 1` to avoid gaps.
5. **Release storage** via `allocation_manager.delete_blocks()`:
   - For each deleted block: `node.used -= size`, `node.available += size`.
6. **Commit** the transaction.
7. On any failure: rollback and return error status.

> **Note:** This RPC deletes the *metadata* only. The client is expected to separately issue `DeleteBlock` RPCs to the individual DataNodes to remove the actual data.

---

## 15. RPC — List Files

**RPC:** `ListFiles`  
**File:** `server.py` → `NameNodeService.ListFiles`

### Protocol

```
Client → NameNode:  ListFilesRequest {}
NameNode → Client:  ListFilesResponse { repeated File(file_name, file_size) }
```

### Logic

Simply queries `SELECT file_name, size FROM file_table` and returns all files as `File` protobuf messages. Each file is also logged to the file log (mode 3).

---

## 16. RPC — Get Cluster Status

**RPC:** `GetClusterStatus`  
**File:** `server.py` → `NameNodeService.GetClusterStatus`

### Protocol

```
Client → NameNode:  ClusterStatusRequest {}
NameNode → Client:  ClusterStatusResponse {
    namenode_active: bool,
    repeated NodeInfo { node_id, hostname, port, status, storage_used_pct, last_heartbeat }
}
```

### Logic

1. Calls `registry.list_nodes()` for a snapshot of all DataNodes.
2. Computes `storage_used_pct = (used / capacity) * 100` per node.
3. Returns `namenode_active = True` with all node info.
4. On exception: returns `namenode_active = False`.

This RPC is designed to power a **web dashboard / monitoring UI**.

---

## 17. Logging System

**File:** `namenode/app/logger.py` — class `Logger`

Writes structured CSV-style logs (`timestamp,EVENT,details`) to four separate files:

| Mode | File | Purpose |
|------|------|---------|
| `0` | `namenode.log` | **Main** — registration, commits, deletes, server start/stop |
| `1` | `maintainence.log` | **Maintenance** — heartbeats, node status changes |
| `2` | `debug.log` | **Debug** — allocation internals, node filtering |
| `3` | `file.log` | **File ops** — list files output |

All log files are **truncated on startup** to start fresh each session. Logs are stored under `namenode/logs/`.

---

## 18. Graceful Shutdown

**Files:** `main.py` (signal handler), `server.py` → `NameNodeServer.stop()`

### Signal Handling

`SIGINT` and `SIGTERM` are caught and trigger the shutdown sequence.

### Shutdown Sequence

```
1. health_checker.stop()     →  Sets running=False, daemon thread exits on next loop
2. registry.save_state()     →  Writes all in-memory node state to dn_table
                                 (INSERT ... ON DUPLICATE KEY UPDATE)
                                 All nodes set to INACTIVE
3. server.stop(grace=10)     →  gRPC server stops accepting new RPCs,
                                 waits up to 10s for in-flight RPCs to finish
4. Log SERVER_STOP
```

This ensures that DataNode state is **not lost** across NameNode restarts.

---

## 19. Docker & Deployment

**File:** `namenode/docker-compose.yml`

- Runs a **MySQL** container (`mysql-db`) with credentials from `.env`.
- Exposes port `3308` (host) → `3306` (container).
- Mounts `models/dn_table.sql` as the Docker entrypoint init script.
- Uses a named volume `mysql_data` for persistence.

---

## 20. DB Utility Scripts

Located in `namenode/db_manager/`:

| Script | Purpose |
|--------|---------|
| `init-db.py` | Executes all 3 SQL DDL files (`dn_table.sql`, `filetable.sql`, `metadata.sql`) to bootstrap the schema |
| `nuke.py` | Drops all 3 tables — **destructive reset** |
| `fakenode.py` | Inserts 10 fake DataNodes (IPs `192.168.1.100–109`, ports `50052–50061`, 1 GB each) for testing |
| `check_tables.py` | Dumps all rows from all 3 tables to stdout |
| `test_connection.py` | Smoke-tests DB connectivity |

---

## Appendix: Data Flow Diagrams

### Write Flow

```
Client                         NameNode                        DataNode
  │                               │                               │
  │── AllocateBlocks ────────────→│                               │
  │   (file, stripe_size, k, m)   │  round-robin placement        │
  │←── BlockGroups ──────────────│                               │
  │   (stripe→block→node map)     │                               │
  │                               │                               │
  │── WriteBlock (stream) ────────────────────────────────────────→│
  │←── WriteBlockResponse ────────────────────────────────────────│
  │   (repeat for each block)     │                               │
  │                               │                               │
  │── CommitFile ────────────────→│                               │
  │   (file, block_ids[])         │  INSERT metadata_table        │
  │←── Status ───────────────────│  INSERT file_table             │
```

### Read Flow

```
Client                         NameNode                        DataNode
  │                               │                               │
  │── GetFileMetadata ───────────→│                               │
  │   (file_name)                 │  SELECT from DB               │
  │←── BlockGroups ──────────────│                               │
  │   (stripe_size, k, m, map)    │                               │
  │                               │                               │
  │── ReadBlock ──────────────────────────────────────────────────→│
  │←── Block (stream) ───────────────────────────────────────────│
  │   (repeat for each block)     │                               │
```

### Heartbeat Flow

```
DataNode                       NameNode
  │                               │
  │── SendHeartbeat ─────────────→│  registry.heartbeat()
  │   (node_id, ts, used, free)   │  update in-memory state
  │←── ACK ──────────────────────│
  │                               │
  │       ┌──────────────────────│  HealthChecker (background)
  │       │ every N seconds:      │  check_node_health()
  │       │ if last_hb > 20s ago  │  → mark INACTIVE
  │       │ UPDATE dn_table       │
  │       └──────────────────────│
```
