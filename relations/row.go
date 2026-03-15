package simpledb

import "fmt"

// Row is a slice of cells, one per column in schema order.
type Row []Cell

// EncodeKey encodes the table name (with trailing zero) and the primary-key cells into key bytes.
func (r Row) EncodeKey(schema *Schema) ([]byte, error) {
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	var key []byte
	key = append(key, schema.Table...)
	key = append(key, 0)
	for _, idx := range schema.PKey {
		if idx >= len(r) {
			return nil, fmt.Errorf("simpledb: PKey index %d out of row length %d", idx, len(r))
		}
		key = r[idx].Encode(key)
	}
	return key, nil
}

// EncodeVal encodes only the non-primary-key columns into value bytes.
func (r Row) EncodeVal(schema *Schema) ([]byte, error) {
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	pkeySet := make(map[int]bool)
	for _, idx := range schema.PKey {
		pkeySet[idx] = true
	}
	var val []byte
	for i := range schema.Cols {
		if !pkeySet[i] {
			if i >= len(r) {
				return nil, fmt.Errorf("simpledb: column index %d out of row length %d", i, len(r))
			}
			val = r[i].Encode(val)
		}
	}
	return val, nil
}

// DecodeKey decodes key bytes into the primary-key cells of r. Other cells are unchanged.
// Validates that the key's table name matches schema.Table and that PKey indices are in range.
func (r *Row) DecodeKey(schema *Schema, key []byte) error {
	if err := schema.Validate(); err != nil {
		return err
	}
	i := 0
	for i < len(key) && key[i] != 0 {
		i++
	}
	if i >= len(key) {
		return fmt.Errorf("simpledb: key missing table name terminator")
	}
	tableName := string(key[:i])
	if tableName != schema.Table {
		return fmt.Errorf("simpledb: table name mismatch: key %q vs schema %q", tableName, schema.Table)
	}
	i++ // consume the 0
	rest := key[i:]
	for _, idx := range schema.PKey {
		if idx >= len(*r) {
			return fmt.Errorf("simpledb: PKey index %d out of row length %d", idx, len(*r))
		}
		var err error
		rest, err = (*r)[idx].Decode(rest)
		if err != nil {
			return err
		}
	}
	return nil
}

// DecodeVal decodes value bytes into the non-primary-key cells of r.
func (r *Row) DecodeVal(schema *Schema, val []byte) error {
	if err := schema.Validate(); err != nil {
		return err
	}
	pkeySet := make(map[int]bool)
	for _, idx := range schema.PKey {
		pkeySet[idx] = true
	}
	rest := val
	for i := range schema.Cols {
		if pkeySet[i] {
			continue
		}
		if i >= len(*r) {
			return fmt.Errorf("simpledb: column index %d out of row length %d", i, len(*r))
		}
		var err error
		rest, err = (*r)[i].Decode(rest)
		if err != nil {
			return err
		}
	}
	return nil
}
