package simpledb

// Row is a slice of cells, one per column in schema order.
type Row []Cell

// EncodeKey encodes the table name (with trailing zero) and the primary-key cells into key bytes.
func (r Row) EncodeKey(schema *Schema) []byte {
	var key []byte
	key = append(key, schema.Table...)
	key = append(key, 0)              // null byte 0x00 as terminator
	for _, idx := range schema.PKey { //todo: should we add in order of schema.PKey or order of key row?
		key = r[idx].Encode(key)
	}
	return key
}

// EncodeVal encodes only the non-primary-key columns into value bytes.
func (r Row) EncodeVal(schema *Schema) []byte {
	pkeySet := make(map[int]bool)
	for _, idx := range schema.PKey {
		pkeySet[idx] = true
	}
	var val []byte
	for i := range schema.Cols {
		if !pkeySet[i] {
			val = r[i].Encode(val)
		}
	}
	return val
}

// DecodeKey decodes key bytes into the primary-key cells of r. Other cells are unchanged.
func (r *Row) DecodeKey(schema *Schema, key []byte) error {
	// Skip table name (up to and including the zero byte)
	i := 0
	for i < len(key) && key[i] != 0 {
		i++
	}
	if i < len(key) {
		i++ // consume the 0
	}
	rest := key[i:]
	for _, idx := range schema.PKey {
		if idx >= len(*r) {
			break
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
			break
		}
		var err error
		rest, err = (*r)[i].Decode(rest)
		if err != nil {
			return err
		}
	}
	return nil
}
