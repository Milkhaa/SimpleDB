# Relations package

Relational-style tables and a small SQL executor on top of the **engine** package (LSM key-value store). Rows are stored as key-value pairs: the key is the table name plus primary-key columns; the value is the remaining columns.

**Import path:** `github.com/Milkhaa/SimpleDB/relations`

Storage is provided by `github.com/Milkhaa/SimpleDB/engine`; `DB.Open(path)` opens the engine at the given directory.

---

## Types

### Cell and CellType

A **Cell** holds one value. Supported types are:

| Constant      | Go type | Encoding                          |
| ------------- | ------- | --------------------------------- |
| `CellTypeI64` | int64   | 8 bytes, little-endian            |
| `CellTypeStr` | []byte  | 4-byte length (LE) then raw bytes  |

### Schema and Column

- **Column** — `Name` (string) and `Type` (CellType).
- **Schema** — Defines a table: `Table` (name), `Cols` (columns in order), `PKey` (primary key column indices into `Cols`).

### Row

**Row** is `[]Cell`, one cell per column in schema order.

- **EncodeKey(schema)** — Key bytes: `tableName + 0x00` then encoded primary-key cells in **PKey** order.
- **EncodeVal(schema)** — Value bytes: encoded non-primary-key columns in schema order.
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
| CREATE TABLE | `create table name (col type, ... , primary key (pkey1, ...));` — `type` is `int64` or `string`. |
| INSERT       | `insert into name values (val1, val2, ...);` — Values in column order. |
| SELECT       | `select col1, col2 from name [where col=val and ...];` — WHERE must be equality on columns (primary key). |
| UPDATE       | `update name set col=val, ... [where col=val and ...];`              |
| DELETE       | `delete from name [where col=val and ...];`                         |

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

- **Key:** `tableName` (no length prefix) + single null byte `0x00` + encoded key columns in the order given by `schema.PKey`. Table names must not contain the null byte.
- **Value:** Encoded value columns only (all columns not in `PKey`), in schema order.

Cell encoding is the same in keys and values: I64 as 8-byte LE, Str as 4-byte length (LE) then bytes.

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
    PKey: []int{0, 1}, // (id, action)
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
