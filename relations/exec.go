package relations

import (
	"errors"
	"fmt"
)

// exec.go implements the SQL executor: ExecStmt dispatches to execCreateTable,
// execInsert, execSelect, execUpdate, or execDelete. WHERE validation and
// index matching for SELECT are in query_planner.go. Helpers here:
// columnIndex, fillRowFromKeys, projectRow.

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
	Updated int   // rows affected for insert/update/delete
	Values  []Row // result rows for select
}

// ExecStmt executes a parsed statement (value returned by ParseStmt).
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

	indices := make([][]int, 0, len(s.Indices)+1)

	pkeyIdx := make([]int, 0, len(s.Pkey))
	for _, name := range s.Pkey {
		i := columnIndex(&Schema{Cols: s.Cols}, name)
		if i < 0 {
			return ExecResult{}, fmt.Errorf("primary key column %q not found in table columns", name)
		}
		pkeyIdx = append(pkeyIdx, i)
	}

	indices = append(indices, pkeyIdx) // primary key index is always the first index

	for _, index := range s.Indices {
		indexIdx := make([]int, 0, len(index))
		for _, name := range index {
			i := columnIndex(&Schema{Cols: s.Cols}, name)
			if i < 0 {
				return ExecResult{}, fmt.Errorf("index column %q not found in table columns", name)
			}
			indexIdx = append(indexIdx, i)
		}
		indices = append(indices, indexIdx)
	}

	schema := &Schema{
		Table:   s.Table,
		Cols:    s.Cols,
		Indices: indices,
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

	var rows []Row
	if len(s.Keys) == 0 {
		rows, err = db.SelectAll(schema)
		if err != nil {
			return ExecResult{}, err
		}
	} else {
		indexID, err := matchIndexForWhere(schema, s.Keys)
		if err != nil {
			return ExecResult{}, err
		}
		row := schema.NewRow()
		if err := fillRowFromKeys(schema, row, s.Keys); err != nil {
			return ExecResult{}, err
		}
		if indexID == 0 {
			ok, err := db.Select(schema, row)
			if err != nil {
				return ExecResult{}, err
			}
			if ok {
				rows = []Row{row}
			}
		} else {
			rows, err = db.SelectByIndex(schema, indexID, row)
			if err != nil {
				return ExecResult{}, err
			}
		}
	}
	var values []Row
	for _, r := range rows {
		out, err := projectRow(schema, r, s.Cols)
		if err != nil {
			return ExecResult{}, err
		}
		values = append(values, out)
	}
	return ExecResult{Values: values}, nil
}

func (db *DB) execUpdate(s *stmtUpdate) (ExecResult, error) {
	schema, err := db.GetSchema(s.Table)
	if err != nil {
		return ExecResult{}, fmt.Errorf("table %q not found", s.Table)
	}

	// Reject primary-key updates: changing the PK without knowing the old key
	// would behave like a blind insert and leave old row/index entries behind.
	for _, nv := range s.Value {
		i := columnIndex(schema, nv.Column)
		if i < 0 {
			return ExecResult{}, fmt.Errorf("unknown column %q", nv.Column)
		}
		if schema.IsPKey(i) {
			return ExecResult{}, fmt.Errorf("update: primary key column %q cannot be updated", nv.Column)
		}
	}

	var rows []Row
	if len(s.Keys) == 0 {
		rows, err = db.SelectAll(schema)
		if err != nil {
			return ExecResult{}, err
		}
	} else {
		indexID, err := matchIndexForWhere(schema, s.Keys)
		if err != nil {
			return ExecResult{}, err
		}
		row := schema.NewRow()
		if err := fillRowFromKeys(schema, row, s.Keys); err != nil {
			return ExecResult{}, err
		}
		if indexID == 0 {
			ok, err := db.Select(schema, row)
			if err != nil {
				return ExecResult{}, err
			}
			if ok {
				rows = []Row{row}
			}
		} else {
			rows, err = db.SelectByIndex(schema, indexID, row)
			if err != nil {
				return ExecResult{}, err
			}
		}
	}
	n := 0
	for _, r := range rows {
		for _, nv := range s.Value {
			i := columnIndex(schema, nv.Column)
			if i < 0 {
				return ExecResult{}, fmt.Errorf("unknown column %q", nv.Column)
			}
			r[i] = nv.Value
		}
		updated, err := db.Update(schema, r)
		if err != nil {
			return ExecResult{}, err
		}
		if updated {
			n++
		}
	}
	return ExecResult{Updated: n}, nil
}

func (db *DB) execDelete(s *stmtDelete) (ExecResult, error) {
	schema, err := db.GetSchema(s.Table)
	if err != nil {
		return ExecResult{}, fmt.Errorf("table %q not found", s.Table)
	}

	var rows []Row
	if len(s.Keys) == 0 {
		rows, err = db.SelectAll(schema)
		if err != nil {
			return ExecResult{}, err
		}
	} else {
		indexID, err := matchIndexForWhere(schema, s.Keys)
		if err != nil {
			return ExecResult{}, err
		}
		row := schema.NewRow()
		if err := fillRowFromKeys(schema, row, s.Keys); err != nil {
			return ExecResult{}, err
		}
		if indexID == 0 {
			ok, err := db.Select(schema, row)
			if err != nil {
				return ExecResult{}, err
			}
			if ok {
				rows = []Row{row}
			}
		} else {
			rows, err = db.SelectByIndex(schema, indexID, row)
			if err != nil {
				return ExecResult{}, err
			}
		}
	}
	n := 0
	for _, r := range rows {
		deleted, err := db.Delete(schema, r)
		if err != nil {
			return ExecResult{}, err
		}
		if deleted {
			n++
		}
	}
	return ExecResult{Updated: n}, nil
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
