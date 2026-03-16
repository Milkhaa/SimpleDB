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

For design background (serialization, durability, fsync, atomicity, LSM), see **docs/database_internals.md**.
