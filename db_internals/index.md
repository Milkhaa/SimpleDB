## What Is an Index?
An index is extra information used to help search. Many books have an “Index” chapter. Examples include a book’s table of contents, a library classification system, and search engines. All are indexes in a broad sense. One index matches one query pattern, so in OLTP you must build indexes ahead of time. Databases are not magic. They cannot know future queries patterns.

Relational DBs are made of rows and columns. OLAP databases are column-based. Our OLTP database is row-based. One row is one record. A query first finds the needed record, then reads fields from it. For now, one record is a KV pair. This K is the “primary key”. It is like a page number in a book. V is like the page content. An index is extra KV data that leads to the primary key (page number), then the full record (page content).

Relational DBs use sorting data structures for indexes to support range queries. Some also implement hashtables, but that is niche

## KV for Indexes
Indexed data can be one or more columns (a tuple). Put indexed columns in K and the primary key in V. Example:
```
create table t (
    a string, b string, c string, d string,
    primary key a,
    index (b, c),
    index (c)
);
```

--------------------------------
Type            Key         Value
--------------------------------
Primary key     a           b, c, d
Index           b, c        a
Index           c           a
--------------------------------


Indexes and primary keys are both KV sets. The difference is that the primary key is unique, so a record can be identified by its primary key. Indexed data does not need to be unique. But our KV store does not allow duplicate keys, so we use another plan: Add the primary key to K to make it unique, and leave V empty.

--------------------------------
Type            Key         Value
--------------------------------
Primary key     a           b, c, d
Index           b, c, a     empty
Index           c, a        empty
--------------------------------


## Maintain Indexes
Modify INSERT, UPDATE, and DELETE to update or remove index keys. Requirements:
- When adding a record, insert index keys.
- When deleting a record, delete index keys.
- When updating an existing record, the old index keys may need to be removed.

With indexes, a single update touches multiple keys. Atomicity across multiple keys must be handled(may be using transaction ?)

## Transactions and Atomicity
With indexes, one record in a table may use multiple keys. We need the transaction feature to ensure atomicity across multiple keys.

```
    tx = kv.NewTX()
    tx.Set("k1", "v1")
    tx.Set("k2", "v2")
    tx.Commit()
```

Right now, log + checksum ensures single-key atomicity. 
< If a log record contains multiple keys, we can get transaction atomicity easily>

### Transaction Updates
We can record updates inside a transaction and apply them together on commit.
```
type KVTX struct {
    target *KV
    updates SortedArray
    levels MergedSortedKV
}
```

- KVTX.updates stores updates in the transaction.
- KVTX.levels is used for reads inside the transaction.

Instead of updating KV.mem, we now update KVTX.updates, and postpone log writes.

### Transaction Reads
Reads in a transaction must see its own updates. This is just one more LSM-Tree level


### Log Rollback
For durability, each KV update is written to the WAL. To make multi-key transactions atomic,
the WAL also contains a dedicated commit record that marks the transaction boundary.

```
const (
    OpAdd    byte = 0
    OpDel    byte = 1
    OpCommit byte = 2 // new
)
type record struct {
    key []byte
    val []byte
    // op encodes the record kind: OpAdd, OpDel, or OpCommit.
    op RecordOp
}
```

- On database startup, only process records before OpCommit.
- If writing the log returns an error midway, the next write must restore the file offset to the start of the transaction.

Do not blindly append to the file. Track the file offset and use WriteAt(). If Log.Write() returns an error, the file offset must not change.