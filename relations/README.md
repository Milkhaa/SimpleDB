# Relations

Relational-style tables on top of the SimpleDB key-value store. Rows are encoded as key-value pairs: the key is the table name plus primary-key columns; the value is the remaining columns.

## Types

### Cell and CellType

A **Cell** holds one value. Supported types are:

| Constant     | Go type | Encoding |
|-------------|---------|----------|
| `CellTypeI64` | `int64`  | 8 bytes, little-endian |
| `CellTypeStr` | `[]byte` | 4-byte length (LE) then raw bytes |

### Schema and Column

- **Column** — `Name` (string) and `Type` (CellType).
- **Schema** — Defines a table: `Table` (name), `Cols` (columns in order), `PKey` (primary key column indices into `Cols`).

### Row

**Row** is `[]Cell`, one cell per column in schema order.

- **EncodeKey(schema)** — Key bytes: `tableName + 0x00` then encoded primary-key cells in **PKey order**.
- **EncodeVal(schema)** — Value bytes: encoded non-primary-key columns in schema order.
- **DecodeKey(schema, key)** — Fills the primary-key cells of the row from `key` (skips table name up to the null terminator). Other cells unchanged.
- **DecodeVal(schema, val)** — Fills the non-primary-key cells from `val`.

### DB

**DB** is the relational interface. It wraps the key-value store.

- **Open(path string) error** — Opens or creates the database at `path` (WAL file path).
- **Close() error** — Closes the store.
- **Select(schema, row) (ok bool, err error)** — Looks up by primary key (key from `row`). Decodes the value into `row` and returns `true` if found.
- **Insert(schema, row) (updated bool, err error)** — Writes the row. Returns `true` if the key was new or the value changed.
- **Update(schema, row) (updated bool, err error)** — Overwrites the row by primary key (same as Insert).
- **Delete(schema, row) (deleted bool, err error)** — Removes the row by primary key. Returns `true` if it existed.

## Key and value layout

- **Key:** `tableName` (no length prefix) + single null byte `0x00` + encoded key columns in the order given by `schema.PKey`. Table names must not contain the null byte.
- **Value:** Encoded value columns only (all columns not in `PKey`), in schema order.

Cell encoding is the same in keys and values: I64 as 8-byte LE, Str as 4-byte length (LE) then bytes.

## Example

```go
db := &DB{}
if err := db.Open(".mydb"); err != nil {
    log.Fatal(err)
}
defer db.Close()

schema := &Schema{
    Table: "event",
    Cols: []Column{
        {Name: "ts", Type: CellTypeI64},
        {Name: "kind", Type: CellTypeStr},
        {Name: "payload", Type: CellTypeStr},
    },
    PKey: []int{0, 1}, // (ts, kind)
}

row := Row{
    Cell{Type: CellTypeI64, I64: 1000},
    Cell{Type: CellTypeStr, Str: []byte("click")},
    Cell{Type: CellTypeStr, Str: []byte("button_a")},
}
_, _ = db.Insert(schema, row)

out := schema.NewRow()
out[0].I64 = 1000
out[1].Str = []byte("click")
ok, _ := db.Select(schema, out)
// ok == true, out[2].Str == []byte("button_a")
```

## Package

This code lives in package `simpledb` under the `relations` directory and uses the root SimpleDB store (`github.com/Milkhaa/SimpleDB`) for persistence.

