package simpledb

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

// NewRow returns a new Row with one Cell per column, each Cell's Type set from the schema.
func (s *Schema) NewRow() Row {
	row := make(Row, len(s.Cols))
	for i := range s.Cols {
		row[i] = Cell{Type: s.Cols[i].Type}
	}
	return row
}
