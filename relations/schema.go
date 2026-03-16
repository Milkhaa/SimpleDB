package relations

// Column is a named column with a Cell type (I64 or Str).
type Column struct {
	Name string
	Type CellType
}

// Schema describes a table: name, columns in order, and primary key column
// indices (PKey). PKey order determines key encoding: EncodeKey uses PKey
// order; DecodeKey expects the same.
type Schema struct {
	Table string
	Cols  []Column
	PKey  []int // indices into Cols, in key encoding order
}

// Validate checks that every PKey index is in range [0, len(Cols)). Returns an error if any are invalid.
func (s *Schema) Validate() error {
	for _, idx := range s.PKey {
		if idx < 0 || idx >= len(s.Cols) {
			return ErrInvalidPKey
		}
	}
	return nil
}

// IsPKey reports whether the column at colIndex is part of the primary key.
func (s *Schema) IsPKey(colIndex int) bool {
	for _, idx := range s.PKey {
		if idx == colIndex {
			return true
		}
	}
	return false
}

// NewRow returns a new Row with one Cell per column, each Cell's Type set from the schema.
func (s *Schema) NewRow() Row {
	row := make(Row, len(s.Cols))
	for i := range s.Cols {
		row[i] = Cell{Type: s.Cols[i].Type}
	}
	return row
}
