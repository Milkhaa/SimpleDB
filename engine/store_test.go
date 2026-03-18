package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Basic CRUD, reopen, and compaction  ---

func TestStoreBasic(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(Config{Path: dir, LogThreshold: 5})
	require.NoError(t, err)
	defer kv.Close()

	// Set and read back
	updated, err := kv.Set([]byte("alpha"), []byte("one"))
	require.NoError(t, err)
	assert.True(t, updated)

	val, ok, err := kv.Get([]byte("alpha"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "one", string(val))

	// Missing key
	_, ok, err = kv.Get([]byte("missing"))
	require.NoError(t, err)
	assert.False(t, ok)

	// Del missing is no-op; Del present returns updated
	updated, err = kv.Del([]byte("missing"))
	require.NoError(t, err)
	assert.False(t, updated)

	updated, err = kv.Del([]byte("alpha"))
	require.NoError(t, err)
	assert.True(t, updated)

	_, ok, err = kv.Get([]byte("alpha"))
	require.NoError(t, err)
	assert.False(t, ok)

	// Two more keys
	_, err = kv.Set([]byte("beta"), []byte("two"))
	require.NoError(t, err)
	_, err = kv.Set([]byte("gamma"), []byte("three"))
	require.NoError(t, err)

	// Reopen: deleted key is gone, others persist
	kv.Close()
	kv, err = Open(Config{Path: dir, LogThreshold: 5})
	require.NoError(t, err)
	defer kv.Close()

	_, ok, err = kv.Get([]byte("alpha"))
	require.NoError(t, err)
	assert.False(t, ok)
	val, ok, err = kv.Get([]byte("beta"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "two", string(val))

	// Compact (flush mem to SSTable) and verify reads still work
	err = kv.Compact()
	require.NoError(t, err)
	val, ok, err = kv.Get([]byte("beta"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "two", string(val))

	// Idempotent Set (same value) returns !updated
	updated, err = kv.Set([]byte("beta"), []byte("two"))
	require.NoError(t, err)
	assert.False(t, updated)

	// Del gamma, reopen, only beta remains
	updated, err = kv.Del([]byte("gamma"))
	require.NoError(t, err)
	assert.True(t, updated)
	_, ok, err = kv.Get([]byte("gamma"))
	require.NoError(t, err)
	assert.False(t, ok)

	kv.Close()
	kv, err = Open(Config{Path: dir, LogThreshold: 5})
	require.NoError(t, err)
	defer kv.Close()

	_, ok, err = kv.Get([]byte("alpha"))
	require.NoError(t, err)
	assert.False(t, ok)
	val, ok, err = kv.Get([]byte("beta"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "two", string(val))
	_, ok, err = kv.Get([]byte("gamma"))
	require.NoError(t, err)
	assert.False(t, ok)
}

// --- Reopen / compact under load  ---

func TestStoreReopenUnderLoad(t *testing.T) {
	dir := t.TempDir()
	const N = 20

	for mode := 0; mode < 3; mode++ {
		// Start each mode from a clean database directory (mirrors the 0805 test pattern).
		os.RemoveAll(dir)
		kv, err := Open(Config{Path: dir, LogThreshold: 4})
		require.NoError(t, err)

		for i := 0; i < N; i++ {
			key := []byte(fmt.Sprintf("node%d", i))
			updated, err := kv.Set(key, key)
			require.NoError(t, err)
			assert.True(t, updated)

			if mode == 0 || mode == 1 {
				err = kv.Compact()
				require.NoError(t, err)
			}
			if mode == 1 || mode == 2 {
				kv.Close()
				kv, err = Open(Config{Path: dir, LogThreshold: 4})
				require.NoError(t, err)
			}

			for j := 0; j <= i; j++ {
				key := []byte(fmt.Sprintf("node%d", j))
				val, ok, err := kv.Get(key)
				require.NoError(t, err)
				assert.True(t, ok, "key %q should be present after step %d (mode %d)", string(key), i, mode)
				assert.Equal(t, string(key), string(val))
			}
		}
		kv.Close()
	}
}

// --- WAL recovery: truncated log  ---

func TestStoreRecoveryTruncatedWAL(t *testing.T) {
	dir := t.TempDir()

	kv, err := Open(Config{Path: dir})
	require.NoError(t, err)
	_, err = kv.Set([]byte("first"), []byte("v1"))
	require.NoError(t, err)
	_, err = kv.Set([]byte("second"), []byte("v2"))
	require.NoError(t, err)
	kv.Close()

	// Truncate last byte of WAL so the second transaction is incomplete
	walPath := filepath.Join(dir, "wal")
	fp, err := os.OpenFile(walPath, os.O_RDWR, 0o644)
	require.NoError(t, err)
	st, err := fp.Stat()
	require.NoError(t, err)
	err = fp.Truncate(st.Size() - 1)
	require.NoError(t, err)
	fp.Close()

	kv2, err := Open(Config{Path: dir})
	require.NoError(t, err)
	defer kv2.Close()

	val, ok, err := kv2.Get([]byte("first"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "v1", string(val))
	_, ok, err = kv2.Get([]byte("second"))
	require.NoError(t, err)
	assert.False(t, ok, "second's commit was truncated so it must not be replayed")
}

// --- WAL recovery: multi-key transaction truncated  ---

func TestStoreRecoveryMultiKeyTransaction(t *testing.T) {
	dir := t.TempDir()

	kv, err := Open(Config{Path: dir})
	require.NoError(t, err)
	_, err = kv.Set([]byte("first"), []byte("v1"))
	require.NoError(t, err)
	tx := kv.NewTX()
	_, err = tx.Set([]byte("third"), []byte("v3"))
	require.NoError(t, err)
	_, err = tx.Set([]byte("second"), []byte("v2"))
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	kv.Close()

	walPath := filepath.Join(dir, "wal")
	fp, err := os.OpenFile(walPath, os.O_RDWR, 0o644)
	require.NoError(t, err)
	st, err := fp.Stat()
	require.NoError(t, err)
	err = fp.Truncate(st.Size() - 1)
	require.NoError(t, err)
	fp.Close()

	kv2, err := Open(Config{Path: dir})
	require.NoError(t, err)
	defer kv2.Close()

	val, ok, err := kv2.Get([]byte("first"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "v1", string(val))
	_, ok, err = kv2.Get([]byte("second"))
	require.NoError(t, err)
	assert.False(t, ok, "second and third are in the truncated tx; both must be absent")
	_, ok, err = kv2.Get([]byte("third"))
	require.NoError(t, err)
	assert.False(t, ok)
}

// --- Seek and iteration  ---

func TestStoreSeek(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(Config{Path: dir})
	require.NoError(t, err)
	defer kv.Close()

	keys := []string{"ape", "bee", "cat"}
	vals := []string{"1", "2", "3"}
	for i := range keys {
		_, err := kv.Set([]byte(keys[i]), []byte(vals[i]))
		require.NoError(t, err)
	}

	tx := kv.NewTX()
	defer tx.Abort()

	// Seek before first key, then Next through all
	iter, err := tx.Seek([]byte("a"))
	require.NoError(t, err)
	for i := range keys {
		require.True(t, iter.Valid())
		assert.Equal(t, keys[i], string(iter.Key()))
		assert.Equal(t, vals[i], string(iter.Val()))
		err = iter.Next()
		require.NoError(t, err)
	}
	assert.False(t, iter.Valid())

	// Prev back to start
	err = iter.Prev()
	require.NoError(t, err)
	for i := len(keys) - 1; i >= 0; i-- {
		require.True(t, iter.Valid())
		assert.Equal(t, keys[i], string(iter.Key()))
		assert.Equal(t, vals[i], string(iter.Val()))
		err = iter.Prev()
		require.NoError(t, err)
	}
	assert.False(t, iter.Valid())

	// Seek to mid-range lands on first key >= seek
	iter, err = tx.Seek([]byte("bb"))
	require.NoError(t, err)
	require.True(t, iter.Valid())
	assert.Equal(t, "bee", string(iter.Key()))

	iter, err = tx.Seek([]byte("bee"))
	require.NoError(t, err)
	require.True(t, iter.Valid())
	assert.Equal(t, "bee", string(iter.Key()))

	// Seek past last key is invalid
	iter, err = tx.Seek([]byte("z"))
	require.NoError(t, err)
	assert.False(t, iter.Valid())
}

// --- Transaction commit: multi-key visible after commit ---

func TestStoreTransactionCommit(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(Config{Path: dir})
	require.NoError(t, err)
	defer kv.Close()

	tx := kv.NewTX()
	updated, err := tx.Set([]byte("x"), []byte("vx"))
	require.NoError(t, err)
	assert.True(t, updated)
	updated, err = tx.Set([]byte("y"), []byte("vy"))
	require.NoError(t, err)
	assert.True(t, updated)

	// Reads inside tx see own updates
	val, ok, err := tx.Get([]byte("x"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "vx", string(val))

	err = tx.Commit()
	require.NoError(t, err)

	// After commit, store sees persisted data
	val, ok, err = kv.Get([]byte("x"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "vx", string(val))
	val, ok, err = kv.Get([]byte("y"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "vy", string(val))
}

// --- Transaction abort: uncommitted writes not visible ---

func TestStoreTransactionAbort(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(Config{Path: dir})
	require.NoError(t, err)
	defer kv.Close()

	tx := kv.NewTX()
	_, err = tx.Set([]byte("orphan"), []byte("discarded"))
	require.NoError(t, err)
	tx.Abort()

	_, ok, err := kv.Get([]byte("orphan"))
	require.NoError(t, err)
	assert.False(t, ok)
}

// --- LSM: log threshold and compaction, then reopen ---

func TestStoreLSMCompactionAndReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Config{Path: dir, LogThreshold: 5})
	require.NoError(t, err)
	defer s.Close()

	s.Set([]byte("red"), []byte("10"))
	s.Set([]byte("blue"), []byte("20"))
	s.Set([]byte("green"), []byte("30"))
	val, ok, err := s.Get([]byte("blue"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "20", string(val))

	// Exceed threshold to trigger flush
	s.Set([]byte("cyan"), []byte("40"))
	s.Set([]byte("magenta"), []byte("50"))
	val, ok, err = s.Get([]byte("red"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "10", string(val))

	// Reopen: data loads from SSTable + WAL
	s.Close()
	s, err = Open(Config{Path: dir, LogThreshold: 5})
	require.NoError(t, err)
	defer s.Close()
	val, ok, err = s.Get([]byte("magenta"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "50", string(val))
	_, ok, err = s.Get([]byte("absent"))
	require.NoError(t, err)
	assert.False(t, ok)

	// Del
	updated, err := s.Del([]byte("green"))
	require.NoError(t, err)
	assert.True(t, updated)
	_, ok, err = s.Get([]byte("green"))
	require.NoError(t, err)
	assert.False(t, ok)
}

// --- Reopen persists data after simple writes ---

func TestStoreReopenPersistsData(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(Config{Path: dir})
	require.NoError(t, err)
	kv.Set([]byte("foo"), []byte("bar"))
	kv.Set([]byte("baz"), []byte("qux"))
	kv.Close()

	kv2, err := Open(Config{Path: dir})
	require.NoError(t, err)
	defer kv2.Close()
	val, ok, err := kv2.Get([]byte("foo"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "bar", string(val))
	val, ok, err = kv2.Get([]byte("baz"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "qux", string(val))
}

// --- Reopen after Set/Del/Set: only final state persists ---

func TestStoreReopenAfterSetDelSet(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(Config{Path: dir})
	require.NoError(t, err)
	kv.Set([]byte("foo"), []byte("bar"))
	kv.Get([]byte("foo"))
	kv.Get([]byte("missing"))
	kv.Del([]byte("missing"))
	kv.Del([]byte("foo"))
	kv.Get([]byte("foo"))
	kv.Set([]byte("baz"), []byte("qux"))
	kv.Close()

	kv2, err := Open(Config{Path: dir})
	require.NoError(t, err)
	defer kv2.Close()
	_, ok, err := kv2.Get([]byte("foo"))
	require.NoError(t, err)
	assert.False(t, ok)
	val, ok, err := kv2.Get([]byte("baz"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "qux", string(val))
}
