# kv-store

A persistent, thread-safe key-value store built in Go, designed around an **LSM-tree** (Log-Structured Merge-tree) architecture. It's like RocksDB but worse. 

This project is built mainly for my educational purposes and for learning about Go and database engines. I do not intend to use this in any production database. I prioritized understanding a breadth of topics over performance, but a future goal is improving the system to make it highly concurrent and available.

---

## What's Implemented

### Skip List (`skiplist/`)
- Probabilistic skip list used as the underlying sorted data structure for the memtable.
- Supports `Put`, `Get`, and in-order `Iterator`.
- Tracks approximate memory size.

### Memtable (`memtable/`)
- In-memory write buffer backed by the skip list.
- Thread-safe reads and writes via `sync.RWMutex`.
- Tombstone-based logical deletes (`Delete` writes a sentinel value).

### Write-Ahead Log (`wal/`)
- Binary-encoded WAL with `Put`/`Delete` opcodes, key/value pairs.
- `fsync` on every write for durability.
- **Log rotation** and **Crash recovery** (replays entries into the active memtable on startup).

### SSTable (`sstable/`)
- **Write**: serializes a memtable to a sorted binary file.
- **Read**: linear scan to locate a key.
- **Merge (compaction)**: k-way merge using a min-heap (`keyheap/`) over sorted SSTable files; deduplicates keys and drops tombstones.

### Engine (`engine/`)
- Orchestrates components with a clean `Engine` interface (`Put`, `Get`, `Delete`).
- **Flush worker**: writes frozen immutable memtable to SSTable, removes old WAL, and signals that the slot is free.
- **Compaction worker**: triggered after flush; merges SSTables when count reaches `maxSSTableCount` (currently 4).
- **Startup recovery**: loads existing SSTables and replays outstanding WALs.

### HTTP Server (`main.go`)
- Prototype in-memory `map[string]string` HTTP API. *Note: Not yet wired to the LSM engine.*

## What's Left

- **SSTable Indexing**: Replace linear SSTable scan with a sparse index and bloom filters for sub-linear lookups.
- **SSTable Blocks & Compression**: Add block-level encoding/compression
- **Graceful Shutdown**: Flush active memtable and drain workers on `SIGINT`/`SIGTERM`.
- **Range Queries**: Add `Scan(start, end)` support.
- **HTTP Server**: Make an HTTP server package and wire it up to actually use the engine

### Stretch Goals
- **Replication**: Implement consensus-based replication (Raft) for high availability and fault tolerance.
- **Sharding**: Support horizontal partitioning/sharding of keys across multiple nodes.

---

## Project Layout

```
.
├── engine/        # Core LSM engine (orchestrator, flush & compaction workers)
├── memtable/      # In-memory write buffer with tombstone deletes
├── skiplist/      # Probabilistic sorted data structure
├── sstable/       # On-disk sorted string tables (write, read, merge)
├── wal/           # Binary write-ahead log with rotation and replay
├── keyheap/       # Min-heap used for k-way SSTable merge
├── docs/          # Design notes and documentation
├── main.go        # HTTP server prototype
└── README.md      # This file
```

## Running Tests

```bash
go test ./...
```
