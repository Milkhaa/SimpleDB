# Relations package

Relational-style tables and a small SQL executor on top of the **engine** package (LSM key-value store). Rows are stored as key-value pairs: the key is the table name plus primary-key columns; the value is the remaining columns.

**Import path:** `github.com/Milkhaa/SimpleDB/relations`

Storage is provided by `github.com/Milkhaa/SimpleDB/engine`; `DB.Open(path)` opens the engine at the given directory.

---

## Package layout

| File | Purpose |
|------|--------|
| `db.go` | DB, Open/Close, GetSchema/SetSchema, Select/Insert/Update/Delete |
| `schema.go` | Schema, Column, Validate, NewRow, IsPKey |
| `row.go` | Row key/value encoding (EncodeKey/Val, DecodeKey/Val) |
| `cell.go` | Cell, CellType (I64, Str), EncodeKey/DecodeKey, EncodeVal/DecodeVal |
| `exec.go` | ExecStmt, ExecResult, exec per statement type |
| `parser.go` | ParseStmt, SQL grammar, statement structs |
| `errors.go` | Package errors |

---

## Types

### Cell and CellType

A **Cell** holds one value. Supported types are:

| Constant      | Go type | Value encoding (EncodeVal/DecodeVal)     |
| ------------- | ------- | ---------------------------------------- |
| `CellTypeI64` | int64   | 8 bytes, little-endian                    |
| `CellTypeStr` | []byte  | 4-byte length (LE) then raw bytes        |

Key encoding (EncodeKey/DecodeKey) is order-preserving: I64 = XOR sign bit + big-endian; Str = null-terminated with escapes. See “Key and value layout” below.

### Schema and Column

- **Column** — `Name` (string) and `Type` (CellType).
- **Schema** — Defines a table: `Table` (name), `Cols` (columns in order), `Indices` (a list of indexes; `Indices[0]` is the primary key index, additional entries are secondary indexes).

### Row

**Row** is `[]Cell`, one cell per column in schema order.

- **EncodeKey(schema)** — Key bytes: `tableName + 0x00` then **order-preserving** encoded primary-key cells (Cell.EncodeKey) in primary-key order (`schema.Indices[0]`).
- **EncodeVal(schema)** — Value bytes: encoded non-primary-key columns (Cell.EncodeVal) in schema order.
- **DecodeKey(schema, key)** — Fills the primary-key cells of the row from `key` (skips table name up to the null terminator). Other cells unchanged.
- **DecodeVal(schema, val)** — Fills the non-primary-key cells from `val`.

### DB

**DB** is the relational interface. It wraps the engine key-value store and a table catalog (schemas keyed by table name). Each table’s schema is persisted under the key `@schema_` + table name and loaded on demand.

- **Open(path string) error** — Opens or creates the database at `path` (a **directory**; the engine uses it for WAL and SSTables). Schemas are loaded on demand via GetSchema.
- **Close() error** — Closes the store.
- **GetSchema(table string) (*Schema, error)** — Returns the schema for the table, loading from the store if not cached. Returns an error if the table does not exist.
- **Schema(table string) *Schema** — Returns the cached schema for the table, or nil (does not load from the store).
- **SetSchema(schema *Schema) error** — Registers a schema and persists it (used by the SQL layer).
- **Select(schema, row) (ok bool, err error)** — Looks up by primary key (key from `row`). Decodes the value into `row` and returns `true` if found.
- **Insert(schema, row) (updated bool, err error)** — Writes the row. Returns `true` if the key was new or the value changed.
- **Update(schema, row) (updated bool, err error)** — Overwrites the row by primary key (same as Insert).
- **Delete(schema, row) (deleted bool, err error)** — Removes the row by primary key. Returns `true` if it existed.


## SQL (query language)

The package includes a parser and executor for a small SQL-like language. Schemas are persisted in the store under `@schema_` + table name and loaded on demand when a table is first used, so tables survive restarts.

- **ParseStmt(s string) (interface{}, error)** — Parses one statement. The string must end with a semicolon. Returns an opaque value to pass to `ExecStmt`.
- **ExecStmt(stmt interface{}) (ExecResult, error)** — Executes a parsed statement (create table, insert, select, update, delete).
- **ExecResult** — `Updated` (int: number of rows affected for insert/update/delete) and `Values` ([]Row: result rows for select).

**Supported statements:**

| Statement    | Form                                                                 |
| ------------ | -------------------------------------------------------------------- |
| CREATE TABLE | `create table name (col type, ... , primary key (pkey1, ...)[, index (col1, ...)]...);` — `type` is `int64` or `string`. Optional `index (...)` clauses define secondary indexes. |
| INSERT       | `insert into name values (val1, val2, ...);` — Values in column order. |
| SELECT       | `select col1, col2 from name [where col=val and ...];` — WHERE must be equality only. See below. |
| UPDATE       | `update name set col=val, ... [where col=val and ...];`              |
| DELETE       | `delete from name [where col=val and ...];`                         |

**SELECT WHERE and index choice:** WHERE must specify equality on a set of columns that exactly matches either the full primary key or the full column list of one secondary index. Partial keys (e.g. only one column of a two-column PK) are not allowed. If WHERE matches the primary key, at most one row is returned; if it matches a secondary index, all rows with those index values are returned. Example: table `t (a, b, c)` with `primary key (a)`, `index (c)` — `where a = 'x'` uses the PK; `where c = '45'` uses the index on `c` and may return multiple rows.

**UPDATE and DELETE** use the same WHERE rules: equality on full primary key or full index. When WHERE matches a secondary index, every matching row is updated or deleted; `ExecResult.Updated` is the number of rows affected. See `TestUpdateByIndex` and `TestDeleteByIndex` in `db_test.go`.

**Values:** Integers (e.g. `-123`) or quoted strings: single or double quotes, with `\'` and `\"` escapes. Keywords (e.g. `select`, `create`) are case-insensitive; table and column names are case-sensitive. Range predicates (e.g. `col >= 'x'`) are **not** supported; only equality in WHERE.

**Example (SQL):**

```go
import "github.com/Milkhaa/SimpleDB/relations"

db := &relations.DB{}
_ = db.Open("./data")  // directory path
defer db.Close()

stmt, _ := relations.ParseStmt("create table link (ts int64, src string, dst string, primary key (src, dst));")
_, _ = db.ExecStmt(stmt)

stmt, _ = relations.ParseStmt("insert into link values (10, 'eve', 'mallory');")
_, _ = db.ExecStmt(stmt)

stmt, _ = relations.ParseStmt("select ts from link where dst = 'mallory' and src = 'eve';")
r, _ := db.ExecStmt(stmt)
// r.Values has one Row with the selected columns
```

---

## Key and value layout

- **Key:** `tableName` (no length prefix) + single null byte `0x00` + **order-preserving** encoded key columns (EncodeKey) in primary-key order (`schema.Indices[0]`). Table names must not contain the null byte.
- **Value:** Encoded value columns only (EncodeVal: non-PKey columns in schema order).

**Key encoding (order-preserving for bytes.Compare):**
- **I64:** Remap int64 so byte order matches sort order: XOR with `1<<63`, then 8 bytes **big-endian**. So negative &lt; zero &lt; positive.
- **Str:** Null-terminated (append `0x00`). Bytes `0x00` and `0x01` are escaped: `0x00` → `0x01 0x01`, `0x01` → `0x01 0x02`. Lexicographic order is preserved.

**Value encoding (compact, not order-preserving):**
- **I64:** 8 bytes little-endian.
- **Str:** 4-byte length (LE) then raw bytes.

---

## Example (programmatic API)

```go
import "github.com/Milkhaa/SimpleDB/relations"

db := &relations.DB{}
if err := db.Open("./data"); err != nil {
    log.Fatal(err)
}
defer db.Close()

schema := &relations.Schema{
    Table: "session",
    Cols: []relations.Column{
        {Name: "id", Type: relations.CellTypeI64},
        {Name: "action", Type: relations.CellTypeStr},
        {Name: "data", Type: relations.CellTypeStr},
    },
    Indices: [][]int{{0, 1}}, // primary key: (id, action)
}

row := relations.Row{
    relations.Cell{Type: relations.CellTypeI64, I64: 42},
    relations.Cell{Type: relations.CellTypeStr, Str: []byte("open")},
    relations.Cell{Type: relations.CellTypeStr, Str: []byte("item_x")},
}
_, _ = db.Insert(schema, row)

out := schema.NewRow()
out[0].I64 = 42
out[1].Str = []byte("open")
ok, _ := db.Select(schema, out)
// ok == true, out[2].Str == []byte("item_x")
```
