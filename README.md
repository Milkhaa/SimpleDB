# SimpleDB

A minimal database you can **use**, **read**, and **learn from**: an LSM-style key-value store with a write-ahead log (WAL), in-memory MemTable, and immutable SSTables; optional relational tables; and a small SQL parser and executor. The implementation focuses on durability, checksummed records, and transaction boundaries so you can see how a small database is built end to end.

---

## Prerequisites

- **Go 1.21+** (tested with 1.23)
- Clone the repo and run from the project root:

```bash
git clone <repo-url> simple-db
cd simple-db
```

---

## Quick start

### 1. Run tests

```bash
go test ./...
```

You should see all packages pass: `engine`, `relations`, and any other packages in the tree.

### 2. Use the key-value store (engine)

```go
package main

import (
	"log"
	"github.com/Milkhaa/SimpleDB/engine"
)

func main() {
	kv, err := engine.Open(engine.Config{Path: "./data"})
	if err != nil {
		log.Fatal(err)
	}
	defer kv.Close()

	kv.Set([]byte("user:1"), []byte("alice"))
	val, ok, _ := kv.Get([]byte("user:1"))
	if ok {
		log.Printf("got: %s", val)
	}
	kv.Del([]byte("user:1"))
}
```

`Path` is a **directory**: the engine creates the WAL and SSTable files under it.

### 3. Use the relational layer and SQL (relations)

```go
package main

import (
	"log"
	"github.com/Milkhaa/SimpleDB/relations"
)

func main() {
	db := &relations.DB{}
	if err := db.Open("./data"); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	stmt, _ := relations.ParseStmt("create table users (id int64, name string, primary key (id));")
	_, _ = db.ExecStmt(stmt)
	stmt, _ = relations.ParseStmt("insert into users values (1, 'alice');")
	_, _ = db.ExecStmt(stmt)

	stmt, _ = relations.ParseStmt("select name from users where id = 1;")
	r, _ := db.ExecStmt(stmt)
	if len(r.Values) > 0 {
		log.Printf("name: %s", r.Values[0][1].Str)
	}
}
```

Statements must end with a semicolon. See [relations/README.md](relations/README.md) for the full SQL grammar and API.

---

## Project structure

| Path | Purpose |
|------|--------|
| **engine** | LSM key-value store: `Open(Config)`, `KV` (Get, Set, Del, Seek), transactions, WAL, MemTable, SSTables, compaction. |
| **relations** | Relational layer: `DB`, `Schema`, `Row`, `Cell`; `ParseStmt` / `ExecStmt` for SQL; primary and secondary indexes. |
| **db_internals** | Learning material: indexes, transactions, WAL layout, durability, LSM concepts (no code). |

- **Root** — This README and the Go module; no database code.
- **engine** — Storage only. Data lives under `Config.Path` (a directory): `wal`, `meta0`/`meta1`, and `sstable_*` files.
- **relations** — Builds on the engine for tables and SQL; same directory is used as the engine path (e.g. `db.Open("./data")`).

---

## Configuration (engine)

| Option | Meaning | Default |
|--------|--------|--------|
| `Config.Path` | Database directory (WAL + SSTables) | `.simpledb` |
| `Config.LogThreshold` | Max keys in MemTable before flush to SSTable | `1000` |
| `Config.GrowthFactor` | Merge adjacent SSTables when `cur*GrowthFactor >= cur+next` | `2.0` |

### Using custom values

Pass them directly to `engine.Open`:

```go
kv, err := engine.Open(engine.Config{
    Path:         "./mydb",
    LogThreshold: 500,
    GrowthFactor: 3.0,
})
if err != nil {
    log.Fatal(err)
}
defer kv.Close()
```

Notes:
- If `Path` is empty, the engine uses the default `.simpledb`.
- If `LogThreshold <= 0`, the engine uses the default `1000`.
- If `GrowthFactor < 2.0`, the engine uses the default `2.0`.

The `relations.DB` helper currently calls `engine.Open` with default config; use the engine directly if you need custom WAL/compaction options.

---

## Learning path

1. **Read the docs**
   - [db_internals/storage_engine.md](db_internals/storage_engine.md) — Durability, serialization, fsync, atomicity, LSM overview.
   - [db_internals/index.md](db_internals/index.md) — Indexes, transactions, WAL and commit boundaries.

2. **Trace the code**
   - **engine**: start with `Open` in `key_value_store.go`, then WAL replay and `Get`/`Set`/`Del`; then `record.go` (WAL format) and `write_ahead_log.go`; then compaction and `sorted_file.go` (SSTable).
   - **relations**: start with `DB.Open` and `ExecStmt` in `db.go` and `exec.go`, then `row.go` (key/value encoding) and `parser.go` (SQL).

3. **Run and change the tests**
   - **engine**: `store_test.go` covers basic CRUD, reopen, compaction, WAL recovery, transactions, and Seek.
   - **relations**: `db_test.go` and others cover tables, indexes, and SQL.

4. **Extend it**
   - Add a new SQL statement, a new cell type, or a different compaction policy; use the existing tests as a safety net.

---

## More detail

- **Engine** — Package layout, config, and design: [engine/README.md](engine/README.md).
- **Relations** — Types, SQL grammar, key/value layout: [relations/README.md](relations/README.md).

---

## Limitations (current scope)

- **Single-process / no concurrency**: the code does not aim to provide safe concurrent reads/writes from multiple goroutines.
- **Simplified SQL**: the `relations` layer supports a small SQL-like subset; `WHERE` predicates are equality-only (no range comparisons like `>=` / `<=`).
- **No cost-based optimizer / full query engine**: index selection is based on matching `WHERE` columns to primary key or a single secondary index, not on query planning heuristics.
- **LSM behavior is intentionally basic**: compaction is controlled by `LogThreshold` and `GrowthFactor` with a straightforward “merge adjacent SSTables” policy.
- **Testing focuses on correctness of the WAL + recovery scenarios**: performance tuning, GC of obsolete data, and more advanced storage features are out of scope for now.

## Planned improvements (ideas)

- **Improve transactions**: add explicit transaction isolation semantics (e.g., snapshot or locking-based), define isolation guarantees, and extend tests for interleavings/negative cases beyond the current WAL commit-boundary atomicity.

- **Broaden SQL capabilities**: add more operators and richer `WHERE` support (e.g. range predicates), while keeping the project small and testable.

- **More storage features**: optional Bloom filters for SSTables, richer metadata, and more compaction strategies.

If you have a specific feature in mind, start by opening an issue or a PR with a focused change plus tests.

## Open source contributions

Contributions are welcome for extending the database features, planned improvements and bug fixes .

- **Start small**: implement one behavior per PR.
- **Tests first**: add/extend tests that reproduce the behavior before changing production code.
- **Verify locally**: run `go test ./...` before submitting.
- **Documentation matters**: if you change storage format or behavior, also update the relevant `db_internals/*.md` docs and comments.

## License

This project is licensed under the MIT License - see [LICENSE](LICENSE) for details.
