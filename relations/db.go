package relations

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Milkhaa/SimpleDB/engine"
)

// DB is the relational interface: it wraps the engine key-value store and a
// per-table schema cache. Row keys are table name + primary key; row values
// are non-primary-key columns. Schemas are persisted under schemaKey and
// loaded on demand in GetSchema.
type DB struct {
	store  *engine.KV
	tables map[string]*Schema // cached schemas by table name
}

// Schema storage key: "@schema_<tableName>". Table names must not contain null bytes.
func schemaKey(tableName string) []byte {
	return []byte("@schema_" + tableName)
}

func (db *DB) ensureTables() {
	if db.tables == nil {
		db.tables = make(map[string]*Schema)
	}
}

func indexPrefix(table string, indexID int) []byte {
	// Keep index keys outside the row-key namespace (which starts with tableName bytes).
	// Format: "@idx_" + table + "\x00" + indexID + "\x00"
	b := make([]byte, 0, len(table)+16)
	b = append(b, "@idx_"...)
	b = append(b, table...)
	b = append(b, 0)
	b = append(b, byte(indexID))
	b = append(b, 0)
	return b
}

func indexEntryKey(schema *Schema, indexID int, row Row) ([]byte, error) {
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	if indexID <= 0 || indexID >= len(schema.Indices) {
		return nil, fmt.Errorf("relations: invalid index id %d", indexID)
	}
	key := indexPrefix(schema.Table, indexID)
	for _, colIdx := range schema.Indices[indexID] {
		if colIdx >= len(row) {
			return nil, fmt.Errorf("relations: index col %d out of row length %d", colIdx, len(row))
		}
		key = row[colIdx].EncodeKey(key)
	}
	// Suffix with the primary key so multiple rows can share the same secondary key.
	for _, colIdx := range schema.PKey() {
		if colIdx >= len(row) {
			return nil, fmt.Errorf("relations: PKey col %d out of row length %d", colIdx, len(row))
		}
		key = row[colIdx].EncodeKey(key)
	}
	return key, nil
}

// indexKeyPrefix returns the index key prefix for a given index and row (index columns only, no PK).
// Used to Seek into index entries; stored keys are prefix + PK encoding.
func indexKeyPrefix(schema *Schema, indexID int, row Row) ([]byte, error) {
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	if indexID <= 0 || indexID >= len(schema.Indices) {
		return nil, fmt.Errorf("relations: invalid index id %d", indexID)
	}
	key := indexPrefix(schema.Table, indexID)
	for _, colIdx := range schema.Indices[indexID] {
		if colIdx >= len(row) {
			return nil, fmt.Errorf("relations: index col %d out of row length %d", colIdx, len(row))
		}
		key = row[colIdx].EncodeKey(key)
	}
	return key, nil
}

// Open opens or creates the database at path (directory for LSM). Schemas are loaded on demand via GetSchema.
func (db *DB) Open(path string) error {
	s, err := engine.Open(engine.Config{Path: path})
	if err != nil {
		return err
	}
	db.store = s
	db.ensureTables()
	return nil
}

// GetSchema returns the schema for the table, loading from the store if not cached.
func (db *DB) GetSchema(table string) (*Schema, error) {
	if db.tables != nil {
		if s := db.tables[table]; s != nil {
			return s, nil
		}
	}
	data, ok, err := db.store.Get(schemaKey(table))
	if err != nil {
		return nil, err
	}
	if !ok || len(data) == 0 {
		return nil, errors.New("table not found")
	}
	var s Schema
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	db.ensureTables()
	db.tables[table] = &s
	return &s, nil
}

// Schema returns the cached schema for the table name, or nil (does not load from store).
func (db *DB) Schema(table string) *Schema {
	if db.tables == nil {
		return nil
	}
	return db.tables[table]
}

// SetSchema registers a schema for a table and persists it (used by SQL execution).
func (db *DB) SetSchema(schema *Schema) error {
	db.ensureTables()
	data, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	if _, err = db.store.Set(schemaKey(schema.Table), data); err != nil {
		return err
	}
	db.tables[schema.Table] = schema
	return nil
}

// Close closes the database.
func (db *DB) Close() error {
	if db.store == nil {
		return nil
	}
	err := db.store.Close()
	db.store = nil
	return err
}

// Select loads the row by primary key into row and returns true if found.
func (db *DB) Select(schema *Schema, row Row) (ok bool, err error) {
	key, err := row.EncodeKey(schema)
	if err != nil {
		return false, err
	}
	val, ok, err := db.store.Get(key)
	if err != nil || !ok {
		return false, err
	}
	err = row.DecodeVal(schema, val)
	if err != nil {
		return false, err
	}
	return true, nil
}

// SelectByIndex returns all rows whose index columns match the values in row.
// row must have the index column cells set; PK cells are filled from the index and then the full row is loaded.
func (db *DB) SelectByIndex(schema *Schema, indexID int, row Row) ([]Row, error) {
	prefix, err := indexKeyPrefix(schema, indexID, row)
	if err != nil {
		return nil, err
	}
	iter, err := db.store.Seek(prefix)
	if err != nil {
		return nil, err
	}
	var out []Row
	for iter.Valid() {
		k := iter.Key()
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		suffix := k[len(prefix):]
		pkRow := schema.NewRow()
		if err := pkRow.DecodePKOnly(schema, suffix); err != nil {
			break
		}
		ok, err := db.Select(schema, pkRow)
		if err != nil {
			return out, err
		}
		if ok {
			out = append(out, pkRow)
		}
		if err := iter.Next(); err != nil {
			return out, err
		}
	}
	return out, nil
}

// Insert writes the row. It returns updated=true if the key was new or the value changed.
func (db *DB) Insert(schema *Schema, row Row) (updated bool, err error) {
	key, err := row.EncodeKey(schema)
	if err != nil {
		return false, err
	}
	// If the row already exists, load the old version so we can maintain secondary index entries.
	var oldRow Row
	oldVal, exists, err := db.store.Get(key)
	if err != nil {
		return false, err
	}
	if exists {
		oldRow = schema.NewRow()
		if err := oldRow.DecodeKey(schema, key); err != nil {
			return false, err
		}
		if err := oldRow.DecodeVal(schema, oldVal); err != nil {
			return false, err
		}
	}
	val, err := row.EncodeVal(schema)
	if err != nil {
		return false, err
	}
	updated, err = db.store.Set(key, val)
	if err != nil || !updated {
		return updated, err
	}
	// Maintain secondary indexes (Indices[1:]).
	if exists {
		for indexID := 1; indexID < len(schema.Indices); indexID++ {
			oldIdxKey, err := indexEntryKey(schema, indexID, oldRow)
			if err != nil {
				return true, err
			}
			_, err = db.store.Del(oldIdxKey)
			if err != nil {
				return true, err
			}
		}
	}
	for indexID := 1; indexID < len(schema.Indices); indexID++ {
		idxKey, err := indexEntryKey(schema, indexID, row)
		if err != nil {
			return true, err
		}
		if _, err := db.store.Set(idxKey, nil); err != nil {
			return true, err
		}
	}
	return true, nil
}

// Update overwrites the row by primary key. Returns updated=true if the value changed.
func (db *DB) Update(schema *Schema, row Row) (updated bool, err error) {
	return db.Insert(schema, row)
}

// Delete removes the row by primary key. Returns deleted=true if the key existed.
func (db *DB) Delete(schema *Schema, row Row) (deleted bool, err error) {
	key, err := row.EncodeKey(schema)
	if err != nil {
		return false, err
	}
	// Load existing row so we can remove secondary index entries.
	oldVal, exists, err := db.store.Get(key)
	if err != nil || !exists {
		return false, err
	}
	oldRow := schema.NewRow()
	if err := oldRow.DecodeKey(schema, key); err != nil {
		return false, err
	}
	if err := oldRow.DecodeVal(schema, oldVal); err != nil {
		return false, err
	}
	for indexID := 1; indexID < len(schema.Indices); indexID++ {
		idxKey, err := indexEntryKey(schema, indexID, oldRow)
		if err != nil {
			return false, err
		}
		if _, err := db.store.Del(idxKey); err != nil {
			return false, err
		}
	}
	return db.store.Del(key)
}
