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

// applyTX writes all updates in the transaction to the log (one record per key), then Commit() for atomicity.
// On any error, ResetTX() is called so the log write offset is rolled back.
func (kv *KV) applyTX(tx *KVTX) error {
	if err := kv.updateLog(tx); err != nil {
		return err
	}
	kv.updateMem(tx)
	if err := kv.Compact(); err != nil {
		return err
	}
	return nil
}

// updateLog writes each of tx.updates to the log, then Commit(). defer ResetTX() rolls back offset on any error.
func (kv *KV) updateLog(tx *KVTX) error {
	defer kv.log.ResetTX()
	iter, err := tx.updates.Iter()
	if err != nil {
		return err
	}
	for ; err == nil && iter.Valid(); err = iter.Next() {
		op := OpAdd
		if iter.Deleted() {
			op = OpDel
		}
		rec := record{key: append([]byte(nil), iter.Key()...), val: append([]byte(nil), iter.Val()...), op: op}
		if err := kv.log.Write(&rec); err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	return kv.log.Commit()
}

// updateMem applies all tx.updates to kv.mem.
func (kv *KV) updateMem(tx *KVTX) {
	iter, err := tx.updates.Iter()
	if err != nil {
		return
	}
	for ; err == nil && iter.Valid(); err = iter.Next() {
		if iter.Deleted() {
			kv.mem.Del(iter.Key())
		} else {
			kv.mem.Set(iter.Key(), iter.Val())
		}
	}
}
