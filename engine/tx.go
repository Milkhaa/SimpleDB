package engine

import "bytes"

// KVTX is a transaction over KV. Updates are buffered in updates and applied on Commit.
// Reads see tx.updates merged with the target's mem and sstables (tx.levels).
type KVTX struct {
	target  *KV
	updates *SortedArray
	levels  MergedSortedKV
}

// NewTX starts a new transaction. All reads and writes use the transaction until Commit or Abort.
func (kv *KV) NewTX() *KVTX {
	tx := &KVTX{
		target:  kv,
		updates: &SortedArray{},
	}
	tx.levels = make(MergedSortedKV, 0, 2+len(kv.sstables))
	tx.levels = append(tx.levels, tx.updates, kv.mem)
	for _, f := range kv.sstables {
		tx.levels = append(tx.levels, f)
	}
	return tx
}

// Seek returns an iterator at the first entry with key >= key, skipping deleted entries.
// Reads see the transaction's own updates (newest) then mem then sstables.
func (tx *KVTX) Seek(key []byte) (SortedKVIter, error) {
	iter, err := tx.levels.Seek(key)
	if err != nil {
		return nil, err
	}
	return filterDeleted(iter)
}

// Get returns the value for key. ok is false if the key is missing or was deleted.
func (tx *KVTX) Get(key []byte) (value []byte, ok bool, err error) {
	iter, err := tx.Seek(key)
	if err != nil {
		return nil, false, err
	}
	ok = iter.Valid() && bytes.Equal(iter.Key(), key)
	if ok {
		value = iter.Val()
	}
	return value, ok, err
}

// Set writes key-value in the transaction. It returns updated=true if the key was new or the value changed.
// The write is buffered in the transaction until Commit.
func (tx *KVTX) Set(key, value []byte) (updated bool, err error) {
	oldVal, exist, err := tx.Get(key)
	if err != nil {
		return false, err
	}
	updated = !exist || !bytes.Equal(oldVal, value)
	if !updated {
		return false, nil
	}
	_, err = tx.updates.Set(key, value)
	return true, err
}

// Del removes key in the transaction. It returns updated=true if the key existed.
// The delete is buffered until Commit.
func (tx *KVTX) Del(key []byte) (updated bool, err error) {
	_, exist, err := tx.Get(key)
	if err != nil || !exist {
		return false, err
	}
	_, err = tx.updates.Del(key)
	return true, err
}

// Commit applies the transaction to the target KV (write log, update mem) and ends the transaction.
func (tx *KVTX) Commit() error {
	return tx.target.applyTX(tx)
}

// Abort discards the transaction. For now it is a no-op.
func (tx *KVTX) Abort() {}

// applyTX writes all updates in the transaction to the log and then applies them to mem.
func (kv *KV) applyTX(tx *KVTX) error {
	iter, err := tx.updates.Iter()
	if err != nil {
		return err
	}
	var recs []record
	for iter.Valid() {
		k, v := iter.Key(), iter.Val()
		deleted := iter.Deleted()
		recs = append(recs, record{key: append([]byte(nil), k...), val: append([]byte(nil), v...), deleted: deleted})
		if err := iter.Next(); err != nil {
			return err
		}
	}
	for _, rec := range recs {
		if err := kv.log.append(&rec); err != nil {
			return err
		}
	}
	for _, rec := range recs {
		if rec.deleted {
			kv.mem.Del(rec.key)
		} else {
			kv.mem.Set(rec.key, rec.val)
		}
	}
	if err := kv.Compact(); err != nil {
		return err
	}
	return nil
}
