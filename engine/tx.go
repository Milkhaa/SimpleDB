package engine

import (
	"bytes"
	"errors"
)

// KVTX is a transaction over KV. Updates are buffered in updates and applied on Commit.
// Reads see tx.updates merged with the target's mem and sstables (tx.levels).
type KVTX struct {
	target  *KV
	updates *SortedArray
	levels  MergedSortedKV
	closed  bool
}

// NewTX starts a new transaction. All reads and writes use the transaction until Commit or Abort.
func (kv *KV) NewTX() *KVTX {
	tx := &KVTX{
		target:  kv,
		updates: &SortedArray{},
	}
	tx.closed = false
	tx.levels = make(MergedSortedKV, 0, 2+len(kv.sstables))
	tx.levels = append(tx.levels, tx.updates, kv.mem)
	for _, f := range kv.sstables {
		tx.levels = append(tx.levels, f)
	}
	return tx
}

func (tx *KVTX) ensureOpen() error {
	if tx.closed {
		return ErrTxClosed
	}
	return nil
}

// Seek returns an iterator at the first entry with key >= key, skipping deleted entries.
// Reads see the transaction's own updates (newest) then mem then sstables.
func (tx *KVTX) Seek(key []byte) (SortedKVIter, error) {
	if err := tx.ensureOpen(); err != nil {
		return nil, err
	}
	iter, err := tx.levels.Seek(key)
	if err != nil {
		return nil, err
	}
	return filterDeleted(iter)
}

// Get returns the value for key. ok is false if the key is missing or was deleted.
func (tx *KVTX) Get(key []byte) (value []byte, ok bool, err error) {
	if err := tx.ensureOpen(); err != nil {
		return nil, false, err
	}
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
	if err := tx.ensureOpen(); err != nil {
		return false, err
	}
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
	if err := tx.ensureOpen(); err != nil {
		return false, err
	}
	_, exist, err := tx.Get(key)
	if err != nil || !exist {
		return false, err
	}
	_, err = tx.updates.Del(key)
	return true, err
}

// Commit applies the transaction to the target KV (write log, update mem) and ends the transaction.
func (tx *KVTX) Commit() error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	// Commit applies the tx to the target KV. Only a successful commit
	// makes the tx terminal.
	err := tx.target.applyTX(tx)
	if err == nil {
		tx.closed = true
	}
	return err
}

// Abort discards the transaction. For now it is a no-op.
func (tx *KVTX) Abort() {
	// Abort is terminal: further operations (including Commit) must fail.
	tx.closed = true
}

// applyTX writes all updates in the transaction to the log (one record per key), then Commit() for atomicity.
// On any error, ResetTX() is called so the log write offset is rolled back.
func (kv *KV) applyTX(tx *KVTX) error {
	if err := kv.updateLog(tx); err != nil {
		return err
	}
	kv.updateMem(tx)
	// Compact is post-commit maintenance. If it fails, the transaction data
	// is already durable (WAL) and visible (MemTable), so the tx is still
	// considered committed.
	if err := kv.Compact(); err != nil {
		return nil
	}
	return nil
}

// updateLog writes each of tx.updates to the WAL, then Commit() for atomicity.
// It uses WAL rollback on any error; rollback also truncates the WAL tail.
func (kv *KV) updateLog(tx *KVTX) (retErr error) {
	defer func() {
		if rbErr := kv.log.ResetTX(); rbErr != nil {
			retErr = errors.Join(retErr, rbErr)
		}
	}()
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
