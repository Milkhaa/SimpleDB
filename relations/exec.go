package relations

import (
	"errors"
	"fmt"
)

// columnIndex returns the index of the schema column with the given name, or -1 if not found.
func columnIndex(schema *Schema, name string) int {
	for i, c := range schema.Cols {
		if c.Name == name {
			return i
		}
	}
	return -1
}

func oneIf(b bool) int {
	if b {
		return 1
	}
	return 0
}

// ExecResult is the result of executing a statement.
type ExecResult struct {
	Updated int
	Values  []Row
}

// ExecStmt executes a parsed statement.
func (db *DB) ExecStmt(stmt interface{}) (ExecResult, error) {
	switch s := stmt.(type) {
	case *stmtCreateTable:
		return db.execCreateTable(s)
	case *stmtInsert:
		return db.execInsert(s)
	case *stmtSelect:
		return db.execSelect(s)
	case *stmtUpdate:
		return db.execUpdate(s)
	case *stmtDelete:
		return db.execDelete(s)
	default:
		return ExecResult{}, fmt.Errorf("unknown statement type")
	}
}

func (db *DB) execCreateTable(s *stmtCreateTable) (ExecResult, error) {
	if _, err := db.GetSchema(s.Table); err == nil {
		return ExecResult{}, errors.New("duplicate table name")
	}
	pkeyIdx := make([]int, 0, len(s.Pkey))
	for _, name := range s.Pkey {
		i := columnIndex(&Schema{Cols: s.Cols}, name)
		if i < 0 {
			return ExecResult{}, fmt.Errorf("primary key column %q not found in table columns", name)
		}
		pkeyIdx = append(pkeyIdx, i)
	}
	schema := &Schema{
		Table: s.Table,
		Cols:  s.Cols,
		PKey:  pkeyIdx,
	}
	if err := schema.Validate(); err != nil {
		return ExecResult{}, err
	}
	if err := db.SetSchema(schema); err != nil {
		return ExecResult{}, err
	}
	return ExecResult{}, nil
}

func (db *DB) execInsert(s *stmtInsert) (ExecResult, error) {
	schema, err := db.GetSchema(s.Table)
	if err != nil {
		return ExecResult{}, fmt.Errorf("table %q not found", s.Table)
	}
	if len(s.Value) != len(schema.Cols) {
		return ExecResult{}, fmt.Errorf("insert: expected %d values, got %d", len(schema.Cols), len(s.Value))
	}
	row := make(Row, len(schema.Cols))
	for i := range schema.Cols {
		row[i] = s.Value[i]
	}
	updated, err := db.Insert(schema, row)
	if err != nil {
		return ExecResult{}, err
	}
	return ExecResult{Updated: oneIf(updated)}, nil
}

func (db *DB) execSelect(s *stmtSelect) (ExecResult, error) {
	schema, err := db.GetSchema(s.Table)
	if err != nil {
		return ExecResult{}, fmt.Errorf("table %q not found", s.Table)
	}
	row := schema.NewRow()
	if err := fillRowFromKeys(schema, row, s.Keys); err != nil {
		return ExecResult{}, err
	}
	ok, err := db.Select(schema, row)
	if err != nil {
		return ExecResult{}, err
	}
	if !ok {
		return ExecResult{Values: []Row{}}, nil
	}
	out, err := projectRow(schema, row, s.Cols)
	if err != nil {
		return ExecResult{}, err
	}
	return ExecResult{Values: []Row{out}}, nil
}

func (db *DB) execUpdate(s *stmtUpdate) (ExecResult, error) {
	schema, err := db.GetSchema(s.Table)
	if err != nil {
		return ExecResult{}, fmt.Errorf("table %q not found", s.Table)
	}
	row := schema.NewRow()
	if err := fillRowFromKeys(schema, row, s.Keys); err != nil {
		return ExecResult{}, err
	}
	ok, err := db.Select(schema, row)
	if err != nil {
		return ExecResult{}, err
	}
	if !ok {
		return ExecResult{}, nil
	}
	for _, nv := range s.Value {
		i := columnIndex(schema, nv.Column)
		if i < 0 {
			return ExecResult{}, fmt.Errorf("unknown column %q", nv.Column)
		}
		row[i] = nv.Value
	}
	updated, err := db.Update(schema, row)
	if err != nil {
		return ExecResult{}, err
	}
	return ExecResult{Updated: oneIf(updated)}, nil
}

func (db *DB) execDelete(s *stmtDelete) (ExecResult, error) {
	schema, err := db.GetSchema(s.Table)
	if err != nil {
		return ExecResult{}, fmt.Errorf("table %q not found", s.Table)
	}
	row := schema.NewRow()
	if err := fillRowFromKeys(schema, row, s.Keys); err != nil {
		return ExecResult{}, err
	}
	deleted, err := db.Delete(schema, row)
	if err != nil {
		return ExecResult{}, err
	}
	return ExecResult{Updated: oneIf(deleted)}, nil
}

func fillRowFromKeys(schema *Schema, row Row, keys []sqlNamedCell) error {
	for _, k := range keys {
		i := columnIndex(schema, k.Column)
		if i < 0 {
			return fmt.Errorf("unknown column %q", k.Column)
		}
		row[i] = k.Value
	}
	return nil
}

func projectRow(schema *Schema, row Row, cols []string) (Row, error) {
	out := make(Row, len(cols))
	for j, name := range cols {
		i := columnIndex(schema, name)
		if i < 0 {
			return nil, fmt.Errorf("unknown column %q", name)
		}
		out[j] = row[i]
	}
	return out, nil
}
