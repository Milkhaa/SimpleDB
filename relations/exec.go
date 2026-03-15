package simpledb

import (
	"errors"
	"fmt"
)

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
		found := false
		for i, c := range s.Cols {
			if c.Name == name {
				pkeyIdx = append(pkeyIdx, i)
				found = true
				break
			}
		}
		if !found {
			return ExecResult{}, fmt.Errorf("primary key column %q not found in table columns", name)
		}
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
	n := 0
	if updated {
		n = 1
	}
	return ExecResult{Updated: n}, nil
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
		return ExecResult{Updated: 0}, nil
	}
	for _, nv := range s.Value {
		found := false
		for i, c := range schema.Cols {
			if c.Name == nv.Column {
				row[i] = nv.Value
				found = true
				break
			}
		}
		if !found {
			return ExecResult{}, fmt.Errorf("unknown column %q", nv.Column)
		}
	}
	updated, err := db.Update(schema, row)
	if err != nil {
		return ExecResult{}, err
	}
	n := 0
	if updated {
		n = 1
	}
	return ExecResult{Updated: n}, nil
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
	n := 0
	if deleted {
		n = 1
	}
	return ExecResult{Updated: n}, nil
}

func fillRowFromKeys(schema *Schema, row Row, keys []sqlNamedCell) error {
	for _, k := range keys {
		found := false
		for i, c := range schema.Cols {
			if c.Name == k.Column {
				row[i] = k.Value
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("unknown column %q", k.Column)
		}
	}
	return nil
}

func projectRow(schema *Schema, row Row, cols []string) (Row, error) {
	out := make(Row, len(cols))
	for j, name := range cols {
		found := false
		for i, c := range schema.Cols {
			if c.Name == name {
				out[j] = row[i]
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("unknown column %q", name)
		}
	}
	return out, nil
}
