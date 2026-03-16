// Package relations provides relational-style tables and a small SQL executor
// on top of the engine key-value store.
//
// Storage model: each row is one key-value pair. The key is table name (null-
// terminated) plus encoded primary-key columns. The value is encoded non-primary-key
// columns. Schemas are stored under keys "@schema_<table>" and loaded on demand.
//
// Use DB.Open to open or create a database, then GetSchema/SetSchema for schemas
// and Select/Insert/Update/Delete for rows. For SQL, use ParseStmt then ExecStmt.
// See the relations README for key/value layout and supported SQL.
//
// Package layout (for readers):
//   - db.go      — DB type, Open/Close, GetSchema/SetSchema, Select/Insert/Update/Delete
//   - schema.go  — Schema, Column, Validate, NewRow, IsPKey
//   - row.go     — Row key/value encoding and decoding (EncodeKey/Val, DecodeKey/Val)
//   - cell.go    — Cell and CellType (I64, Str), Encode/Decode
//   - exec.go    — ExecStmt, ExecResult, and per-statement exec (create table, insert, select, update, delete)
//   - parser.go  — SQL parser (ParseStmt), statement structs, parseCreateTable/parseSelect/...
//   - errors.go  — Package errors
package relations
