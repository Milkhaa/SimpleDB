# Database
Database internals and computer science basics required to build your own database.

- KV storage engine.
- SQL and relational databases.
- Indexes and data structures.


Software called databases comes in many forms. Some are pure in-memory, such as Redis and Memcached. Some are disk-based, such as MySQL and SQLite.


In-memory databases are limited by RAM size, so traditional databases are disk-based. But even disk-based databases often include in-memory data structures. So different types of databases are not unrelated.

## KV storage engine.
In terms of features, relational databases seem to do more. However, more complex relational databases are built on simpler KV systems, often called storage engines.
For example, LevelDB and RocksDB can be used as standalone KV stores, and also have SQL DBs built on top. So database implementation starts from KV.


We can start with :
```
type KV struct {
	log Log
	mem map[string][]byte // or an in-memory arrays of keys and values
}

type Log struct {
	FileName string
	fp       *os.File
}
```

Each data write is written to a in-memory map, and for that to survive a restart , save the data in an append only file as well on disk. On DB restart, this log is replayed to the in-memory map.

## Data serialization
To store data types from a programming language on disk or send them over a network, they must be converted into a byte sequence. This is called **serialization**.

### Serialization methods

All serialization methods are similar in spirit. For variable-length data like strings, the simplest scheme is: **store the length first, then the bytes**. The length is just an integer; you can choose 2 bytes, 4 bytes, a varint, decimal text, etc.

There are also text formats like JSON and XML. Here “binary” just means “not text.” Text formats usually don’t store lengths; they use delimiters (quotes, tags) to mark where data ends.

Text formats feel intuitive but are tricky to implement correctly. Because data cannot contain delimiters, you need escaping rules, and real-world JSON/XML parsers often have subtle bugs. They also tend to be slower and bulkier than a simple binary format.

Binary formats avoid these issues. Libraries like Protobuf or MsgPack exist, but for low-level systems it’s often easiest—and perfectly fine—to define a tiny custom binary encoding rather than depend on a heavy text format.


## Durability
A database guarantees that written data is not lost. This guarantee is defined by a successful return to the caller. If the database crashes before returning, the state is uncertain to the caller. But if the caller receives success, it can trust the write not disappearing.

### Append-only logs
Like text logs, a database log only appends entries at the end of the file and never modifies or deletes existing entries. Log entries record every update to the database.

When the database starts, it reads the log and applies updates in order, producing the final state.

**Log record layout (initial):**

| key size | val size | op | key data | val data |
|----------|----------|----|----------|----------|
| 4 bytes  | 4 bytes  | 1 byte | ...      | ...      |

### OS page cache and fsync
Each file write does not directly map to a disk write. The OS has a memory cache; writes go to the cache first, then are synced to disk later. This allows merging repeated writes and improves throughput (repeated reads also benefit).

To ensure data reaches disk, an operation must flush all cache layers and wait for completion. On Linux this is the **fsync** syscall; in Go, call it via `Sync()` on `*os.File`.

On Linux, fsync ensures file *data* is written but does not ensure the file itself exists. A file is recorded by its parent directory—if a directory entry is added (file creation) but not written to disk before power loss, the file cannot be reached even if its data is on disk. To fix this, call fsync on the directory. Creating, renaming, and deleting files all require fsync on the containing directory.

This is Unix-specific; Windows does not need this. The Go standard library has no method for fsyncing a directory, so you must invoke syscalls directly:

```go
func syncDir(file string) error {
    flags := os.O_RDONLY | syscall.O_DIRECTORY
    dirfd, err := syscall.Open(path.Dir(file), flags, 0o644)
    if err != nil {
        return err
    }
    defer syscall.Close(dirfd)
    return syscall.Fsync(dirfd)
}
```

## Atomicity
When appending a record to the log, we want it to be either completely written or not written at all—**atomicity**. File writes do not guarantee atomicity in the case of power loss. Only the last record will be affected; previously fsynced records remain intact. This is another reason to use a log in databases.

### Achieving atomicity for log/disk writes
If we can detect an incomplete write, we can simply ignore it. The last record affected will be the one before the last successful fsync. A **checksum** helps: it is a hash, and different data will almost certainly have different checksums. By storing the checksum for each record, we can identify incomplete writes.

We use the standard library’s `crc32.ChecksumIEEE()` to compute the checksum for log records and prepend it to the record:

| crc32   | key size | val size | op | key data | val data |
|---------|----------|----------|----|----------|----------|
| 4 bytes | 4 bytes  | 4 bytes  | 1 byte  | ...      | ...      |

`OpCommit` is the transaction boundary record; it uses the same record layout but with `keyLen=0` and `valLen=0`, so there is no key/val payload.



## LSM Tree:
Right now, our DB is log + in-memory array. It’s an in-memory DB with durability. Data size is limited by memory, so we need disk-based data structures.

There is a way to achieve updates without updating data: the log-structured merge tree (LSM-Tree).
- On update, add a new data structure to the set.
- Merge existing data structures to keep the set size from unlimited growth.
- On query, search all data structures and merge results


Log-structured Merge Tree and B+Tree are the only 2 practical choices to build a general-purpose DB.

In LSM-Tree based implementation, our database have 2 levels: MemTable and SSTable. MemTable uses a log for durability, while SSTable is never modified and is only replaced by new files. For queries, results are merged, and the upper MemTable has higher priority. For updates, data goes to the upper level first, then to lower levels.

A key may exist in multiple levels. Upper levels are newer, so queries go from top to bottom. Deleted keys must be recorded to prevent exposing old versions from lower levels.

Levels are usually stored as files. To avoid creating new files on every update, level 1 is often directly updatable. The simplest form is a log. The log must be mirrored in an in-memory structure (MemTable) for queries. That is why almost all LSM-Tree docs mentions these components.

Deleted keys take space until reaching the last level, which makes the last level special.

Arrays use binary search, but real systems should use n-ary search to reduce IO. Operating systems read disks in pages. To use information efficiently, each n-ary branch should use at least one full page.

### LSM Tree parameters:
- LogShreshold is the max key count in the log. When exceeded, convert to an SSTable.
- GrowthFactor is the size ratio (key count) between the next level and the current level.

We are  using a GrowthFactor of 2. 
A larger ratio can also be used, trading more frequent merges for fewer levels. 

In an LSM-Tree, a key is written on average 𝑂(log𝑁) times. This is called write amplification and can limit write throughput. 

GrowthFactor is the trade-off between query performance (level count) and write throughput


### Atomic updates:
**Copy-on-write:**
Whatever data structure is used, atomicity and durability must be considered once it’s on disk, the A and D in ACID. This depends on how updates are done. A log achieves atomicity with checksums because:
- Appending data does not destroy the old state.
- A checksum can detect an incomplete new state.

Array insert and delete move elements and destroy the old array. To avoid destroying the old state, we can copy the data, apply updates (still 𝑂(𝑁)), then replace the old array with the new one. The replace step can be atomic. For example, Linux rename() can replace a file atomically. This copy + replace method is called copy-on-write (CoW).

On Linux, after a crash, the target of rename() is either the old file or the new file. Provided that the data is fsynced before renaming. Also fsync the directory

The common idea of copy-on-write and logs is: write new data without destroying old data, then switch atomically.


**Double Buffering:**
There is another method that does not rely on filesystem or hardware atomicity:
- Store 2 copies of data, update them in turn.
- Each copy has an monotonic version number, the copy with greater version is used.
- Each copy has a checksum, so the potential bad copy is ignored after recovery.

slot 0 -> [ version 124 | data yyy | crc32 ]
slot 1 -> [ version 123 | data zzz | crc32 ]

This is a cyclic log with only 2 records (ring buffer).

### Querying LSM tree DB:

**Bloom Filter:**
Single-key queries can be further optimized with Bloom filters, which is a compact data structure that can output 2 results in 𝑂(1) time:
- A key definitely does not exist.
- A key probably exists.

It can be seen as a lossy compressed hash table. Details are left to the reader. If each SSTable has a Bloom filter, most levels where the key does not exist can be skipped. A Bloom filter has these properties:
- Space usage is 𝑂(𝑁). Compared to SSTables, it is still small and likely cached in memory.
- Time complexity is 𝑂(1). This does not affect overall complexity, though.
- Keys can only be added, not deleted. This is not an issue because SSTables are immutable.

**sparse-index:**