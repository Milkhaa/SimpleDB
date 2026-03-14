# SimpleDB

A minimal database implementation with append-only logging, durability guarantees, and checksummed records.

---

## Data serialization

To store data types from a programming language on disk or send them over a network, they must be converted into a byte sequence. This is called **serialization**.

### Serialization methods

All serialization methods are similar, differing only in details. To serialize variable-length data like strings, the simplest way is to put the length first, then the data. The length is an integer, and there are many ways to encode it: some formats use 2 bytes, some 4 bytes, some use variable-length varint, and some (e.g. Redis) use decimal digits.

Besides binary format, there are text formats such as JSON and XML. “Binary” has nothing to do with number bases; it is just the opposite of “text”. Most text formats do not encode string length but use delimiters to mark the end of data (JSON uses quotes, XML uses tags).

Text formats look intuitive but are hard to implement. Because encoded data cannot contain delimiters, text formats require complex escaping. Even simple JSON has many bugs across implementations. Compared to simple binary serialization, text formats also waste CPU.

Beyond complexity, text formats often have arbitrary limits. For example, JSON cannot support arbitrary binary data (so base64 is used, which wastes more), and many JSON libraries do not support 64-bit integers. **Unless necessary, do not use text formats.**

For binary serialization, there are implementations like Protobuf and MsgPack, but they are not as widely used as JSON. Many low-level projects invent their own formats—binary serialization is simple and often not worth adding a library dependency. Text formats, due to their complexity, are best handled by a library.

---

## Durability

### Append-only logs

Like text logs, a database log only appends entries at the end of the file and never modifies or deletes existing entries. Log entries record every update to the database.

When the database starts, it reads the log and applies updates in order, producing the final state.

**Log record layout (initial):**

| key size | val size | deleted | key data | val data |
|----------|----------|---------|----------|----------|
| 4 bytes  | 4 bytes  | 1 byte  | ...      | ...      |

Since data is stored on disk, we must ensure it is actually written. If we only write to a file, a power loss can cause the file to disappear or be filled with `0x00`. A database must guarantee that written data is not lost—this is **durability**. The guarantee is defined by a successful return to the caller: if the caller receives success, it can trust the write will not disappear.

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

---

## Atomicity

When appending a record to the log, we want it to be either completely written or not written at all—**atomicity**. File writes do not guarantee atomicity in the case of power loss. Only the last record will be affected; previously fsynced records remain intact. This is another reason to use a log in databases.

### Achieving atomicity for log/disk writes

If we can detect an incomplete write, we can simply ignore it. The last record affected will be the one before the last successful fsync. A **checksum** helps: it is a hash, and different data will almost certainly have different checksums. By storing the checksum for each record, we can identify incomplete writes.

We use the standard library’s `crc32.ChecksumIEEE()` to compute the checksum for log records and prepend it to the record:

| crc32   | key size | val size | deleted | key data | val data |
|---------|----------|----------|---------|----------|----------|
| 4 bytes | 4 bytes  | 4 bytes  | 1 byte  | ...      | ...      |
