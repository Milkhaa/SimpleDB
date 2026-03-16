# Engine package

LSM-style key-value store: write-ahead log (WAL), in-memory sorted MemTable, and immutable SSTables. Focuses on durability guarantees and checksummed records.

**Import path:** `github.com/Milkhaa/SimpleDB/engine`

---

## Package layout

| File | Purpose |
|------|--------|
| `store.go` | `KV` type, `Open`/`Close`, `Get`/`Set`/`Del`, compaction |
| `merge.go` | `SortedKV` abstraction and merge iterator over levels |
| `sorted_array.go` | In-memory sorted table (MemTable) |
| `sorted_file.go` | On-disk SSTable format and iteration |
| `wal.go` | Write-ahead log (append-only, fsynced) |
| `record.go` | WAL record binary format |
| `metadata.go` | Persisted metadata (SSTable list, version) |
| `config.go` | Config and defaults |
| `errors.go` | Package errors |

---

## Usage

```go
import "github.com/Milkhaa/SimpleDB/engine"

kv, err := engine.Open(engine.Config{Path: "./data"})
if err != nil {
    log.Fatal(err)
}
defer kv.Close()

kv.Set([]byte("foo"), []byte("bar"))
val, ok, _ := kv.Get([]byte("foo"))
kv.Del([]byte("foo"))
```

`Config.Path` is a **directory**; the WAL and SSTable files live under it.


## Configuration

- **Config.Path** — Database directory. Default: `.simpledb`.
- **Config.LogThreshold** — Max keys in the MemTable before flushing to an SSTable. Default: `1000`.
- **Config.GrowthFactor** — Merge adjacent SSTables when `cur*GrowthFactor >= cur+next`. Default: `2.0`.



## Data serialization

To store data on disk or send it over a network, it must be converted into a byte sequence—**serialization**. For variable-length data like strings, a common approach is length-prefix: store the length (e.g. 4 bytes little-endian), then the raw bytes.

Text formats (JSON, XML) use delimiters and escaping; they are harder to implement correctly and often have limits (e.g. JSON and binary data or 64-bit integers). **Unless necessary, do not use text formats.** Binary serialization is simple and is what the engine assumes: keys and values are opaque byte slices; the WAL and SSTable formats use length-prefixed fields and checksums.


## Durability

### Append-only log (WAL)

The log only appends entries and never modifies or deletes existing ones. Each log entry records an update (Set or Del) to the key-value store.

On startup, the engine reads the WAL and applies updates in order into the MemTable, then serves reads from the merged view of the MemTable and on-disk SSTables.

**Log record layout (with checksum for atomicity):**

| crc32   | key size | val size | deleted | key data | val data |
|---------|----------|----------|---------|----------|----------|
| 4 bytes | 4 bytes  | 4 bytes  | 1 byte  | ...      | ...      |

Durability means: if the caller receives success, the write must not be lost after a crash. To achieve that, data must reach disk, not only the OS cache.

### OS page cache and fsync

Writes go to the OS page cache first; they are synced to disk later. To ensure data reaches disk, we must flush and wait—on Linux, **fsync** (in Go: `Sync()` on `*os.File`).

On Linux, fsync on a file does not guarantee the file’s *name* is durable: the file is recorded in its parent directory. If the directory entry is not persisted, a crash can make the file unreachable. So creating (or renaming, deleting) a file requires fsync on the **directory** as well. The Go standard library has no API for that; this implementation calls the syscall directly (see `syncDir` in `wal.go`). This is Unix-specific; Windows does not need it.


## Atomicity

We want each log record to be either fully written or not written at all—**atomicity**. After a crash, only the last record might be partial; all earlier ones are complete. So on replay we must detect and skip a partial last record.

A **checksum** (e.g. CRC32 over the rest of the record) identifies incomplete or corrupted writes: if the checksum does not match, we stop replay and ignore that record. The implementation uses `crc32.ChecksumIEEE()` and prepends the checksum to each WAL record as in the table above.
