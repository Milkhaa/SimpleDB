package relations

// Column describes a column name and type.
type Column struct {
	Name string
	Type CellType
}

// Schema describes a table: name, columns, and primary key column indices.
type Schema struct {
	Table string
	Cols  []Column
	PKey  []int // indices into Cols
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

// NewRow returns a new Row with one Cell per column, each Cell's Type set from the schema.
func (s *Schema) NewRow() Row {
	row := make(Row, len(s.Cols))
	for i := range s.Cols {
		row[i] = Cell{Type: s.Cols[i].Type}
	}
	return row
}
