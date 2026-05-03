# NameNode — Architectural Overview

> A component-level description of the NameNode, the central metadata authority in the Sappho distributed file system.

---

## 1. Role of the NameNode

The NameNode is the single point of coordination in the cluster. It never stores or transfers file data itself; instead, it maintains the *knowledge* of where every piece of data lives. Every client operation — writing a new file, reading an existing one, or deleting one — begins with a conversation with the NameNode.

Its responsibilities fall into three broad categories:

- **Cluster Membership** — knowing which DataNodes exist, whether they are alive, and how much capacity each one has.
- **Namespace & Metadata Management** — maintaining the mapping from a logical file to the physical blocks spread across DataNodes, including the erasure-coding layout.
- **Block Placement** — deciding *where* new blocks should be written when a file is being stored.

---

## 2. Component Architecture

The NameNode is composed of six collaborating components, each with a clearly scoped responsibility:

```
┌─────────────────────────────────────────────────────────────┐
│                        NameNode                             │
│                                                             │
│  ┌───────────┐   ┌────────────┐   ┌──────────────────────┐  │
│  │  Config    │   │  Logger    │   │  Database Layer      │  │
│  │  Loader    │   │            │   │  (MySQL persistence) │  │
│  └─────┬─────┘   └─────┬──────┘   └──────────┬───────────┘  │
│        │               │                      │              │
│  ┌─────▼───────────────▼──────────────────────▼───────────┐  │
│  │                  gRPC Server                           │  │
│  │  ┌──────────────────────────────────────────────────┐  │  │
│  │  │             NameNode Service (8 RPCs)             │  │  │
│  │  └────────┬──────────────────────┬──────────────────┘  │  │
│  └───────────┼──────────────────────┼─────────────────────┘  │
│              │                      │                        │
│  ┌───────────▼──────────┐   ┌──────▼───────────────────┐    │
│  │   DataNode Registry  │   │   Allocation Manager     │    │
│  │   (in-memory state)  │◄──┤   (block placement)      │    │
│  └───────────┬──────────┘   └──────────────────────────┘    │
│              │                                               │
│  ┌───────────▼──────────┐                                   │
│  │   Health Checker      │                                   │
│  │   (background thread) │                                   │
│  └───────────────────────┘                                   │
└─────────────────────────────────────────────────────────────┘
```

### 2.1 Configuration Loader

Reads an INI-format configuration file at startup. It supplies the server's bind address, port, thread-pool size, and health-check interval. The system is designed to be reconfigured without code changes — everything environment-specific lives in this file.

### 2.2 Logger

A structured, file-based logging system that separates concerns into four independent log streams:

| Stream | Captures |
|--------|----------|
| **Main** | Core lifecycle events — registration, file commits, deletions, server start/stop |
| **Maintenance** | Heartbeat traffic and node health transitions |
| **Debug** | Allocation internals — node filtering, capacity checks |
| **File** | File listing queries |

Each log entry is a timestamped CSV record. All streams are cleared on startup to give each session a clean slate.

### 2.3 Database Layer

A MySQL-backed persistence layer responsible for durable storage of three categories of data:

- **Node Registry** — the identity and last-known state of every DataNode that has ever joined the cluster.
- **File Catalogue** — the list of files stored in the system, along with their erasure-coding parameters.
- **Block Metadata** — the fine-grained mapping from each file, through its stripes, down to individual block-to-node assignments.

The database is the system's source of truth across restarts. The NameNode hydrates its in-memory state from it on boot and flushes back to it on shutdown. A containerised MySQL instance is provided via Docker Compose for quick deployment.

### 2.4 DataNode Registry

The beating heart of cluster awareness. This component maintains a **thread-safe, in-memory view** of every known DataNode in the cluster. For each node it tracks:

- Network identity (hostname, port)
- Liveness status (active or inactive)
- Last heartbeat timestamp
- Storage capacity, usage, and free space
- Whether the node is currently mid-allocation (reserved but not yet committed)

It also provides a **reverse-lookup index** mapping network addresses to node identifiers, enabling the NameNode to recognise returning DataNodes even when they've lost their identity.

The registry bridges the gap between volatile runtime state and durable persistence:

- On **startup**, it loads all previously known nodes from the database and marks them as inactive, waiting for them to re-announce themselves.
- On **shutdown**, it flushes the current in-memory state back to the database, ensuring nothing is lost.

### 2.5 Allocation Manager

The block placement engine. Given a file's size and erasure-coding parameters (data blocks *k*, parity blocks *m*, stripe size), it:

1. Determines how many stripes the file requires.
2. Filters the registry for DataNodes that are alive and have sufficient free space.
3. Uses a **round-robin cursor** to spread blocks evenly across the cluster, advancing the cursor between stripes so that consecutive stripes don't start on the same node.
4. Generates unique identifiers for every block and temporarily reserves capacity on the selected nodes.

The allocation is kept in an **in-memory staging area** until the client confirms that all writes succeeded. On commit, the allocation is persisted to the database. On failure, the reservations are released and the staging data is discarded.

The manager also handles the inverse operations: reconstructing block-group metadata from the database for read requests, and reclaiming capacity when files are deleted.

### 2.6 Health Checker

A background daemon thread that periodically scans the DataNode Registry for nodes that have gone silent. If a node's last heartbeat exceeds a configurable timeout, the Health Checker marks it as inactive in both the in-memory registry and the persistent database.

This provides **passive failure detection** — the NameNode doesn't ping DataNodes; it simply stops trusting any node that fails to report in. The checker can be disabled for testing environments.

---

## 3. Initialisation

The startup sequence follows a strict dependency order:

1. **Signal handlers** are installed first, ensuring the system can shut down cleanly from the very beginning.
2. **Configuration** is loaded from disk.
3. **Logging** is initialised, clearing previous session logs.
4. **The DataNode Registry** is created and immediately **hydrated from the database**, restoring knowledge of all previously registered nodes (though all are considered inactive until they re-register).
5. **The gRPC server** is assembled, with the service implementation wired to the registry, allocation manager, and logger.
6. **The Health Checker** daemon is started.
7. The server begins accepting connections and blocks until termination.

---

## 4. DataNode Registration

Registration is the process by which a DataNode joins (or re-joins) the cluster. The NameNode handles three distinct scenarios:

- **First contact** — A DataNode with no stored identity connects for the first time. The NameNode generates a globally unique identifier, records the node's address and capacity, marks it active, and returns the identifier for the DataNode to persist locally.

- **Reconnection by address** — A DataNode without a stored identity connects from a previously known address. Rather than creating a duplicate, the NameNode recognises the address via its reverse-lookup index and reactivates the existing record.

- **Reconnection by identity** — A DataNode presents its previously assigned identifier. The NameNode reactivates the corresponding record directly.

In all cases, the node's capacity is refreshed and its status is set to active. This design tolerates DataNode restarts, network partitions, and identity loss gracefully.

---

## 5. Heartbeat Mechanism

Heartbeats are the ongoing liveness signal from DataNodes to the NameNode. Each heartbeat carries:

- The DataNode's identity
- A timestamp
- Current storage utilisation (used and free bytes)

On receiving a heartbeat, the NameNode refreshes the node's last-seen timestamp, marks it active, and updates its storage metrics. This gives the NameNode a continuously updated picture of cluster health and capacity — both of which feed directly into block placement decisions.

The heartbeat is a **pull-based protocol**: the NameNode passively receives reports rather than actively polling DataNodes. This keeps the NameNode's outbound network footprint minimal.

---

## 6. Health Monitoring & Failure Detection

The Health Checker daemon complements the heartbeat system. While heartbeats tell the NameNode which nodes *are* alive, the health checker identifies which nodes have *stopped being* alive.

It runs on a configurable interval and applies a simple rule: any node whose last heartbeat is older than a fixed threshold (20 seconds) is declared inactive. When a node transitions to inactive:

- Its in-memory status is updated immediately, preventing it from being selected for future allocations.
- Its status in the database is updated, ensuring the change survives a NameNode restart.

This is a **protocol-level failure detector** — it doesn't distinguish between a crashed DataNode, a network partition, or a slow node. It simply reflects observed reachability.

---

## 7. File Operations via RPC

The NameNode exposes its functionality through a gRPC service with eight remote procedure calls. They fall into three categories.

### 7.1 Write Path — Block Allocation & Commit

Writing a file is a **two-phase** operation:

**Phase 1 — Allocation.** The client describes the file it wants to store (name, size, stripe size, erasure-coding parameters *k* and *m*). The NameNode computes the required number of stripes and blocks, selects DataNodes for each block using round-robin placement, and returns a complete placement plan — a list of block groups, each containing a stripe identifier and a set of block-to-node assignments.

At this stage, the placement is *tentative*. Capacity is reserved on the selected nodes, but nothing is persisted.

**Phase 2 — Commit.** After the client has successfully written all blocks to the designated DataNodes, it sends a commit request with the list of block identifiers. The NameNode verifies that every block in the request matches a pending allocation, persists the block metadata and file record to the database, and releases the capacity reservations on the nodes.

If the commit fails at any point, the entire transaction is rolled back: the database is unchanged and the in-memory reservations are cleared.

### 7.2 Read Path — Metadata Retrieval

To read a file, the client needs to know where every block lives. It sends the file name to the NameNode, which looks up the file's erasure-coding parameters and its full block-to-node mapping from the database. The response is structured as a list of block groups (stripes), each containing the identities and addresses of the DataNodes holding the constituent blocks.

With this information, the client can directly contact each DataNode to retrieve the blocks and reconstruct the file.

### 7.3 Delete

Deletion removes a file's metadata from the NameNode's records. The NameNode deletes all block mapping entries and the file catalogue record from the database, then updates the in-memory registry to reclaim the storage capacity on each affected DataNode.

Importantly, this only removes the *metadata*. The client is expected to separately instruct each DataNode to delete the physical block data.

### 7.4 List Files

Returns the names and sizes of all files currently stored in the system. This is a straightforward catalogue query with no side effects.

### 7.5 Cluster Status

Provides a real-time snapshot of the entire cluster's health. For each DataNode, it reports identity, address, liveness status, storage utilisation percentage, and the time of the last heartbeat. It also reports whether the NameNode itself is operational.

This RPC is designed to serve monitoring dashboards and administrative tooling.

---

## 8. Erasure-Coded Storage Model

The NameNode's metadata model is built around **erasure coding**, specifically a *k* data blocks + *m* parity blocks scheme (configurable per file). Files are divided into fixed-size **stripes**, and each stripe is split into *k + m* **shards** (blocks). The *k* data blocks hold the original data, while the *m* parity blocks hold redundancy information.

The NameNode's placement algorithm ensures that the blocks within a stripe are distributed across different DataNodes to maximise fault tolerance. The round-robin cursor is advanced between stripes to spread load across the cluster over time.

This model means that the system can tolerate the loss of up to *m* DataNodes per stripe without data loss, as the original data can be reconstructed from any *k* of the *k + m* blocks.

---

## 9. Persistence & Recovery

The NameNode employs a **dual-state** architecture:

- **In-memory state** provides fast, lock-protected access to the DataNode registry and pending allocations. This is the runtime source of truth for all placement and health decisions.
- **Database state** provides durability across restarts. File metadata and block mappings are written to the database at commit time. Node registry state is flushed on shutdown and restored on startup.

On a clean restart, the NameNode's recovery process is:

1. Load the node registry from the database (all nodes start as inactive).
2. Wait for DataNodes to re-register via the registration RPC (transitioning them to active).
3. File and block metadata are immediately available from the database — no recovery action needed.

Pending allocations that were not committed before a crash are implicitly discarded, since they only exist in memory. The reserved capacity is reclaimed when DataNodes report their actual usage via heartbeats.

---

## 10. Graceful Shutdown

When the NameNode receives a termination signal, it executes a controlled shutdown:

1. The Health Checker background thread is stopped.
2. The in-memory DataNode registry is flushed to the database, with all nodes marked inactive.
3. The gRPC server stops accepting new requests and waits for any in-flight RPCs to complete (with a configurable grace period).

This ensures that the cluster state is fully persisted and no client requests are abruptly terminated.

---

## 11. Interaction Summary

```
                    ┌────────────────────┐
                    │      Client        │
                    └──┬──┬──┬──┬──┬──┬──┘
         AllocateBlocks│  │  │  │  │  │ListFiles
            CommitFile │  │  │  │  │  │ClusterStatus
         GetFileMetadata  │  │  │  │
              DeleteFile  │  │  │  │
                    ┌─────▼──▼──▼──▼─────┐
                    │                    │
                    │     NameNode       │
                    │                    │
                    └──┬───────────┬─────┘
          RegisterDataNode        │SendHeartbeat
                    │             │
         ┌──────────▼─────────────▼──────────┐
         │  DataNode   DataNode   DataNode   │
         │    (1)        (2)       (N)       │
         └───────────────────────────────────┘
```

- **Clients** interact with the NameNode for metadata operations (allocate, commit, read, delete, list, status).
- **DataNodes** interact with the NameNode for membership (register, heartbeat).
- **Clients interact directly with DataNodes** for actual data transfer (read/write/delete blocks) — the NameNode is never in the data path.
