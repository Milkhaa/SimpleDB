package simpledb

import "fmt"

// ExecResult is the result of executing a statement.
type ExecResult struct {
	Updated int
	Values  []Row
}

// ExecStmt executes a parsed statement.
func (db *DB) ExecStmt(stmt interface{}) (ExecResult, error) {
	switch s := stmt.(type) {
	case *stmtCreatTable:
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

func (db *DB) execCreateTable(s *stmtCreatTable) (ExecResult, error) {
	pkeyIdx := make([]int, 0, len(s.Pkey))
	for _, name := range s.Pkey {
		for i, c := range s.Cols {
			if c.Name == name {
				pkeyIdx = append(pkeyIdx, i)
				break
			}
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
	db.SetSchema(schema)
	return ExecResult{}, nil
}

func (db *DB) execInsert(s *stmtInsert) (ExecResult, error) {
	schema := db.Schema(s.Table)
	if schema == nil {
		return ExecResult{}, fmt.Errorf("table %q not found", s.Table)
	}
	row := make(Row, len(schema.Cols))
	for i := range schema.Cols {
		if i >= len(s.Value) {
			return ExecResult{}, fmt.Errorf("insert: not enough values")
		}
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
	schema := db.Schema(s.Table)
	if schema == nil {
		return ExecResult{}, fmt.Errorf("table %q not found", s.Table)
	}
	row := schema.NewRow()
	fillRowFromKeys(schema, row, s.Keys)
	ok, err := db.Select(schema, row)
	if err != nil {
		return ExecResult{}, err
	}
	if !ok {
		return ExecResult{Values: []Row{}}, nil
	}
	out := projectRow(schema, row, s.Cols)
	return ExecResult{Values: []Row{out}}, nil
}

func (db *DB) execUpdate(s *stmtUpdate) (ExecResult, error) {
	schema := db.Schema(s.Table)
	if schema == nil {
		return ExecResult{}, fmt.Errorf("table %q not found", s.Table)
	}
	row := schema.NewRow()
	fillRowFromKeys(schema, row, s.Keys)
	ok, err := db.Select(schema, row)
	if err != nil {
		return ExecResult{}, err
	}
	if !ok {
		return ExecResult{Updated: 0}, nil
	}
	for _, nv := range s.Value {
		for i, c := range schema.Cols {
			if c.Name == nv.Column {
				row[i] = nv.Value
				break
			}
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
	schema := db.Schema(s.Table)
	if schema == nil {
		return ExecResult{}, fmt.Errorf("table %q not found", s.Table)
	}
	row := schema.NewRow()
	fillRowFromKeys(schema, row, s.Keys)
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

func fillRowFromKeys(schema *Schema, row Row, keys []sqlNamedCell) {
	for _, k := range keys {
		for i, c := range schema.Cols {
			if c.Name == k.Column {
				row[i] = k.Value
				break
			}
		}
	}
}

func projectRow(schema *Schema, row Row, cols []string) Row {
	out := make(Row, len(cols))
	for j, name := range cols {
		for i, c := range schema.Cols {
			if c.Name == name {
				out[j] = row[i]
				break
			}
		}
	}
	return out
}
