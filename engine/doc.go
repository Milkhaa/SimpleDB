// Package engine provides a durable key-value store with an LSM-style layout.
//
// Use Open to create or open a store, then call Get, Set, and Del. All data is
// persisted: writes go to a write-ahead log (WAL) and an in-memory sorted table;
// reads merge the in-memory table with immutable on-disk SSTables. Compaction
// flushes the in-memory table to new SSTables and merges existing SSTables when
// configured.
//
// For design details (durability, formats, compaction), see the engine README.
//
// Package layout (for readers):
//   - store.go    — KV type, Open/Close, Get/Set/Del, compaction
//   - merge.go    — SortedKV abstraction and merge iterator over levels
//   - sorted_array.go — In-memory sorted table (MemTable)
//   - sorted_file.go  — On-disk SSTable format and iteration
//   - wal.go      — Write-ahead log (append-only, fsynced)
//   - record.go   — WAL record binary format
//   - metadata.go — Persisted metadata (SSTable list, version)
//   - config.go   — Config and defaults
//   - errors.go   — Package errors
package engine
