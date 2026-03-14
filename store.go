package simpledb

import "bytes"

// Store is a durable key-value store backed by an in-memory table and a write-ahead log.
// All mutations are appended to the WAL and synced before returning; on Open, the WAL
// is replayed to reconstruct state.
type Store struct {
	memtable map[string][]byte
	wal      *wal
}

// Open opens or creates the database at the path given in cfg. If the WAL exists,
// it is replayed to restore the in-memory state (incomplete or corrupted tail
// records are skipped).
func Open(cfg Config) (*Store, error) {
	if cfg.Path == "" {
		cfg.Path = ".simpledb"
	}
	s := &Store{
		memtable: make(map[string][]byte),
		wal:      &wal{},
	}
	if err := s.wal.open(cfg.Path); err != nil {
		return nil, err
	}
	// Replay WAL to restore state.
	var rec record
	for {
		done, err := s.wal.read(&rec)
		if err != nil {
			_ = s.wal.close()
			return nil, err
		}
		if done {
			break
		}
		key := string(rec.key)
		if rec.deleted {
			delete(s.memtable, key)
		} else {
			s.memtable[key] = rec.val
		}
	}
	return s, nil
}

// Close closes the store and the underlying WAL. The store must not be used after Close.
func (s *Store) Close() error {
	s.memtable = nil
	return s.wal.close()
}

// Get returns the value for key. ok is false if the key is missing or was deleted.
func (s *Store) Get(key []byte) (value []byte, ok bool, err error) {
	v, ok := s.memtable[string(key)]
	return v, ok, nil
}

// Set writes key-value. It returns updated=true if the key was new or the value changed.
func (s *Store) Set(key, value []byte) (updated bool, err error) {
	k := string(key)
	if v, exists := s.memtable[k]; exists && bytes.Equal(v, value) {
		return false, nil
	}
	s.memtable[k] = value
	err = s.wal.append(&record{key: key, val: value, deleted: false})
	return true, err
}

// Del removes key. It returns updated=true if the key existed.
func (s *Store) Del(key []byte) (updated bool, err error) {
	k := string(key)
	if _, ok := s.memtable[k]; !ok {
		return false, nil
	}
	delete(s.memtable, k)
	err = s.wal.append(&record{key: key, deleted: true})
	return true, err
}
