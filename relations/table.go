package simpledb

import (
	kv "github.com/Milkhaa/SimpleDB"
)

// DB provides relational operations on top of the key-value store.
type DB struct {
	store  *kv.Store
	tables map[string]*Schema
}

// Open opens or creates the database at path (WAL file path).
func (db *DB) Open(path string) error {
	s, err := kv.Open(kv.Config{Path: path})
	if err != nil {
		return err
	}
	db.store = s
	if db.tables == nil {
		db.tables = make(map[string]*Schema)
	}
	return nil
}

// Schema returns the registered schema for the table name, or nil.
func (db *DB) Schema(table string) *Schema {
	if db.tables == nil {
		return nil
	}
	return db.tables[table]
}

// SetSchema registers a schema for a table (used by SQL execution).
func (db *DB) SetSchema(schema *Schema) {
	if db.tables == nil {
		db.tables = make(map[string]*Schema)
	}
	db.tables[schema.Table] = schema
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
