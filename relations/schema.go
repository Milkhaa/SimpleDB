package relations

// Column is a named column with a Cell type (I64 or Str).
type Column struct {
	Name string
	Type CellType
}

// Schema describes a table: name, columns in order, and a set of indexes.
//
// Indices[0] is the primary key index (column indices into Cols, in key encoding order).
// Additional entries in Indices are secondary indexes.
type Schema struct {
	Table   string
	Cols    []Column
	Indices [][]int
}

// PKey returns the primary key index (Indices[0]) or nil if missing.
func (s *Schema) PKey() []int {
	if len(s.Indices) == 0 {
		return nil
	}
	return s.Indices[0]
}

// Validate checks that the schema has a primary key and that every index column
// is in range [0, len(Cols)). Returns an error if any are invalid.
func (s *Schema) Validate() error {
	if len(s.Indices) == 0 || len(s.Indices[0]) == 0 {
		return ErrInvalidPKey
	}
	for _, index := range s.Indices {
		for _, idx := range index {
			if idx < 0 || idx >= len(s.Cols) {
				return ErrInvalidPKey
			}
		}
	}
	return nil
}

// IsPKey reports whether the column at colIndex is part of the primary key.
func (s *Schema) IsPKey(colIndex int) bool {
	for _, idx := range s.PKey() {
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
