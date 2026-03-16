package engine

import (
	"bytes"
	"slices"
)

// SortedArray is the in-memory sorted table (MemTable): keys, values, and a
// deleted flag per entry (tombstones). Used for WAL replay and live writes.
type SortedArray struct {
	keys    [][]byte
	vals    [][]byte
	deleted []bool
}

func (a *SortedArray) Size() int          { return len(a.keys) }
func (a *SortedArray) EstimatedSize() int { return len(a.keys) }
func (a *SortedArray) Key(i int) []byte   { return a.keys[i] }

func (a *SortedArray) Iter() (SortedKVIter, error) {
	return &sortedArrayIter{keys: a.keys, vals: a.vals, deleted: a.deleted, pos: 0}, nil
}

func (a *SortedArray) Seek(key []byte) (SortedKVIter, error) {
	pos, _ := slices.BinarySearchFunc(a.keys, key, bytes.Compare)
	return &sortedArrayIter{keys: a.keys, vals: a.vals, deleted: a.deleted, pos: pos}, nil
}

type sortedArrayIter struct {
	keys    [][]byte
	vals    [][]byte
	deleted []bool
	pos     int
}

func (it *sortedArrayIter) Valid() bool {
	return 0 <= it.pos && it.pos < len(it.keys)
}
func (it *sortedArrayIter) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.keys[it.pos]
}
func (it *sortedArrayIter) Val() []byte {
	if !it.Valid() {
		return nil
	}
	return it.vals[it.pos]
}
func (it *sortedArrayIter) Deleted() bool {
	if !it.Valid() || it.pos >= len(it.deleted) {
		return false
	}
	return it.deleted[it.pos]
}
func (it *sortedArrayIter) Next() error {
	if it.pos < len(it.keys) {
		it.pos++
	}
	return nil
}
func (it *sortedArrayIter) Prev() error {
	if it.pos >= 0 {
		it.pos--
	}
	return nil
}

// Clear removes all entries. Used after flushing the MemTable to an SSTable.
func (a *SortedArray) Clear() {
	a.keys = a.keys[:0]
	a.vals = a.vals[:0]
	a.deleted = a.deleted[:0]
}

// set inserts or overwrites key→val (internal use, e.g. WAL replay). Does not return updated.
func (a *SortedArray) set(key, val []byte) {
	keyCopy := append([]byte(nil), key...)
	valCopy := append([]byte(nil), val...)
	idx, found := slices.BinarySearchFunc(a.keys, key, bytes.Compare)
	if found {
		a.vals[idx] = valCopy
		if idx < len(a.deleted) {
			a.deleted[idx] = false
		}
		return
	}
	a.keys = slices.Insert(a.keys, idx, keyCopy)
	a.vals = slices.Insert(a.vals, idx, valCopy)
	if a.deleted == nil {
		a.deleted = make([]bool, len(a.keys))
	} else {
		a.deleted = slices.Insert(a.deleted, idx, false)
	}
}

// del marks an existing key as deleted; returns false if key missing or already deleted (internal use).
func (a *SortedArray) del(key []byte) bool {
	idx, found := slices.BinarySearchFunc(a.keys, key, bytes.Compare)
	if !found {
		return false
	}
	if a.deleted[idx] {
		return false
	}
	a.vals[idx] = nil
	a.deleted[idx] = true
	return true
}

// Set inserts or updates key to val. Returns updated=true if the key was new, was deleted, or value changed.
func (a *SortedArray) Set(key, val []byte) (updated bool, err error) {
	idx, found := slices.BinarySearchFunc(a.keys, key, bytes.Compare)
	valCopy := append([]byte(nil), val...)
	if found {
		updated = a.deleted[idx] || !bytes.Equal(a.vals[idx], val)
		if updated {
			a.vals[idx] = valCopy
			if idx < len(a.deleted) {
				a.deleted[idx] = false
			}
		}
		return updated, nil
	}
	a.keys = slices.Insert(a.keys, idx, append([]byte(nil), key...))
	a.vals = slices.Insert(a.vals, idx, valCopy)
	if a.deleted == nil {
		a.deleted = make([]bool, len(a.keys))
	} else {
		a.deleted = slices.Insert(a.deleted, idx, false)
	}
	return true, nil
}

// Del marks key as deleted or inserts a tombstone. Returns deleted=true if the key existed and was not already deleted.
func (a *SortedArray) Del(key []byte) (deleted bool, err error) {
	return a.delOrTombstone(key), nil
}

// delOrTombstone ensures the key is marked deleted; if not present, inserts a tombstone.
// Returns true if the key existed and was not already deleted.
func (a *SortedArray) delOrTombstone(key []byte) bool {
	idx, found := slices.BinarySearchFunc(a.keys, key, bytes.Compare)
	if found {
		was := !a.deleted[idx]
		a.vals[idx] = nil
		a.deleted[idx] = true
		return was
	}
	keyCopy := append([]byte(nil), key...)
	a.keys = slices.Insert(a.keys, idx, keyCopy)
	a.vals = slices.Insert(a.vals, idx, nil)
	if a.deleted == nil {
		a.deleted = make([]bool, len(a.keys))
	}
	a.deleted = slices.Insert(a.deleted, idx, true)
	return false
}
