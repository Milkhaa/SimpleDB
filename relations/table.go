package relations

import (
	"encoding/json"
	"errors"

	"github.com/Milkhaa/SimpleDB/engine"
)

func schemaKey(tableName string) []byte {
	return []byte("@schema_" + tableName)
}

// DB provides relational operations on top of the key-value store.
type DB struct {
	store  *engine.KV
	tables map[string]*Schema
}

// Open opens or creates the database at path (directory for LSM). Schemas are loaded on demand via GetSchema.
func (db *DB) Open(path string) error {
	s, err := engine.Open(engine.Config{Path: path})
	if err != nil {
		return err
	}
	db.store = s
	if db.tables == nil {
		db.tables = make(map[string]*Schema)
	}
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
	if db.tables == nil {
		db.tables = make(map[string]*Schema)
	}
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
	if db.tables == nil {
		db.tables = make(map[string]*Schema)
	}
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

// Insert writes the row. It returns updated=true if the key was new or the value changed.
func (db *DB) Insert(schema *Schema, row Row) (updated bool, err error) {
	key, err := row.EncodeKey(schema)
	if err != nil {
		return false, err
	}
	val, err := row.EncodeVal(schema)
	if err != nil {
		return false, err
	}
	return db.store.Set(key, val)
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
	return db.store.Del(key)
}
