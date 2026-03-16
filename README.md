# SimpleDB

A minimal database implementation with an LSM-style key-value store (write-ahead log, in-memory sorted MemTable, immutable SSTables), optional relational tables, and a small SQL parser and executor. Focuses on durability guarantees and checksummed records.

---

## Project structure

| Package / path        | Purpose |
|----------------------|--------|
| **engine**            | Key-value store: `Open(Config)`, `KV` (Get, Set, Del, Seek), WAL, metadata, merge iterator, SSTables. |
| **relations**         | Relational layer: `DB`, `Schema`, `Row`, `Cell`; `ParseStmt` / `ExecStmt` for SQL. |

- **Root** (`github.com/Milkhaa/SimpleDB`): Documentation only; no KV/DB code.
- **Engine** (`github.com/Milkhaa/SimpleDB/engine`): LSM storage. `Config.Path` is a **directory**; the WAL and SSTable files live under it.
- **Relations** (`github.com/Milkhaa/SimpleDB/relations`): Tables, primary-key CRUD, and SQL. Uses the engine package for persistence.

---

## Usage

**Key-value store (engine package):**

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

**Relational + SQL (relations package):**

```go
import "github.com/Milkhaa/SimpleDB/relations"

db := &relations.DB{}
_ = db.Open("./data")  // path is a directory
defer db.Close()

stmt, _ := relations.ParseStmt("create table t (id int64, name string, primary key (id));")
_, _ = db.ExecStmt(stmt)
stmt, _ = relations.ParseStmt("insert into t values (1, 'hello');")
_, _ = db.ExecStmt(stmt)
```

See **relations/README.md** for the full relational and SQL API.

For engine details (configuration, durability, WAL layout, atomicity), see **engine/README.md**.
