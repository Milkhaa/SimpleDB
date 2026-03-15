package simpledb

import "bytes"

// SortedKV is a key-sorted sequence of key-value pairs (MemTable snapshot or SSTable).
type SortedKV interface {
	EstimatedSize() int
	Iter() (SortedKVIter, error)
	Seek(key []byte) (SortedKVIter, error)
}

// SortedKVIter iterates over a SortedKV in key order.
type SortedKVIter interface {
	Valid() bool
	Key() []byte
	Val() []byte
	Deleted() bool
	Next() error
	Prev() error
}

// MergedSortedKV merges multiple SortedKV levels; for duplicate keys the earliest level wins.
type MergedSortedKV []SortedKV

func (m MergedSortedKV) EstimatedSize() (total int) {
	for _, sub := range m {
		total += sub.EstimatedSize()
	}
	return total
}

func (m MergedSortedKV) Iter() (SortedKVIter, error) {
	levels := make([]SortedKVIter, len(m))
	for i, sub := range m {
		it, err := sub.Iter()
		if err != nil {
			return nil, err
		}
		levels[i] = it
	}
	return &MergedSortedKVIter{levels: levels, which: levelsLowest(levels)}, nil
}

func (m MergedSortedKV) Seek(key []byte) (SortedKVIter, error) {
	levels := make([]SortedKVIter, len(m))
	for i, sub := range m {
		it, err := sub.Seek(key)
		if err != nil {
			return nil, err
		}
		levels[i] = it
	}
	return &MergedSortedKVIter{levels: levels, which: levelsLowest(levels)}, nil
}

// MergedSortedKVIter merges multiple level iterators; which is the index of the current key.
type MergedSortedKVIter struct {
	levels []SortedKVIter
	which  int
}

func (iter *MergedSortedKVIter) Valid() bool {
	return iter.which >= 0
}

func (iter *MergedSortedKVIter) Key() []byte {
	if !iter.Valid() {
		return nil
	}
	return iter.levels[iter.which].Key()
}

func (iter *MergedSortedKVIter) Val() []byte {
	if !iter.Valid() {
		return nil
	}
	return iter.levels[iter.which].Val()
}

func (iter *MergedSortedKVIter) Deleted() bool {
	if !iter.Valid() {
		return false
	}
	return iter.levels[iter.which].Deleted()
}

func (iter *MergedSortedKVIter) Next() error {
	cur := ([]byte)(nil)
	if iter.Valid() {
		cur = iter.Key()
	}
	for _, sub := range iter.levels {
		if !sub.Valid() || bytes.Compare(cur, sub.Key()) >= 0 {
			if err := sub.Next(); err != nil {
				return err
			}
		}
	}
	iter.which = levelsLowest(iter.levels)
	return nil
}

func (iter *MergedSortedKVIter) Prev() error {
	cur := ([]byte)(nil)
	if iter.Valid() {
		cur = iter.Key()
	}
	for _, sub := range iter.levels {
		if !sub.Valid() || bytes.Compare(cur, sub.Key()) <= 0 {
			if err := sub.Prev(); err != nil {
				return err
			}
		}
	}
	iter.which = levelsHighest(iter.levels)
	return nil
}

func levelsLowest(levels []SortedKVIter) int {
	win := -1
	winKey := []byte(nil)
	for i, sub := range levels {
		if sub.Valid() && (win < 0 || bytes.Compare(winKey, sub.Key()) > 0) {
			win, winKey = i, sub.Key()
		}
	}
	return win
}

func levelsHighest(levels []SortedKVIter) int {
	win := -1
	winKey := []byte(nil)
	for i, sub := range levels {
		if sub.Valid() && (win < 0 || bytes.Compare(winKey, sub.Key()) < 0) {
			win, winKey = i, sub.Key()
		}
	}
	return win
}

// filterDeleted returns an iterator that skips deleted entries in Next/Prev.
func filterDeleted(iter SortedKVIter) (SortedKVIter, error) {
	for iter.Valid() && iter.Deleted() {
		if err := iter.Next(); err != nil {
			return nil, err
		}
	}
	return &NoDeletedIter{inner: iter}, nil
}

// NoDeletedIter wraps a SortedKVIter and skips deleted entries in Next/Prev.
type NoDeletedIter struct {
	inner SortedKVIter
}

func (iter *NoDeletedIter) Valid() bool   { return iter.inner.Valid() }
func (iter *NoDeletedIter) Key() []byte   { return iter.inner.Key() }
func (iter *NoDeletedIter) Val() []byte   { return iter.inner.Val() }
func (iter *NoDeletedIter) Deleted() bool { return iter.inner.Deleted() }

func (iter *NoDeletedIter) Next() error {
	err := iter.inner.Next()
	for err == nil && iter.inner.Valid() && iter.inner.Deleted() {
		err = iter.inner.Next()
	}
	return err
}

func (iter *NoDeletedIter) Prev() error {
	err := iter.inner.Prev()
	for err == nil && iter.inner.Valid() && iter.inner.Deleted() {
		err = iter.inner.Prev()
	}
	return err
}

// NoDeletedSortedKV wraps a SortedKV and skips deleted entries when iterating.
// Used when writing the first SSTable or the result of merging the last two levels
// so that the on-disk file contains no tombstones.
type NoDeletedSortedKV struct {
	SortedKV
}

func (n NoDeletedSortedKV) Iter() (SortedKVIter, error) {
	it, err := n.SortedKV.Iter()
	if err != nil {
		return nil, err
	}
	return filterDeleted(it)
}

func (n NoDeletedSortedKV) Seek(key []byte) (SortedKVIter, error) {
	it, err := n.SortedKV.Seek(key)
	if err != nil {
		return nil, err
	}
	return filterDeleted(it)
}
