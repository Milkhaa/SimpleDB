package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testDBPath = ".tmp_kv"

func TestStore(t *testing.T) {
	dir := t.TempDir()
	kv2 := doStoreReopen(t, dir)
	defer kv2.Close()
	_, ok, _ := kv2.Get([]byte("foo"))
	assert.False(t, ok)
	val, ok, err := kv2.Get([]byte("baz"))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "qux", string(val))
}

const testLSMDir = ".tmp_lsm"

func TestStoreLSM(t *testing.T) {
	defer os.RemoveAll(testLSMDir)
	os.RemoveAll(testLSMDir)

	s, err := Open(Config{Path: testLSMDir, LogThreshold: 5})
	assert.NoError(t, err)
	defer s.Close()

	s.Set([]byte("red"), []byte("10"))
	s.Set([]byte("blue"), []byte("20"))
	s.Set([]byte("green"), []byte("30"))
	val, ok, err := s.Get([]byte("blue"))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "20", string(val))

	// Exceed threshold to trigger flush (5 keys)
	s.Set([]byte("cyan"), []byte("40"))
	s.Set([]byte("magenta"), []byte("50"))
	val, ok, err = s.Get([]byte("red"))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "10", string(val))

	// Reopen: data should load from SSTable + WAL
	s.Close()
	s, err = Open(Config{Path: testLSMDir, LogThreshold: 5})
	assert.NoError(t, err)
	val, ok, err = s.Get([]byte("magenta"))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "50", string(val))
	_, ok, err = s.Get([]byte("absent"))
	assert.NoError(t, err)
	assert.False(t, ok)

	// Del
	updated, err := s.Del([]byte("green"))
	assert.NoError(t, err)
	assert.True(t, updated)
	_, ok, err = s.Get([]byte("green"))
	assert.NoError(t, err)
	assert.False(t, ok)
	s.Close()
}

func TestStoreTransaction(t *testing.T) {
	defer os.RemoveAll(testDBPath)
	os.RemoveAll(testDBPath)

	kv, err := Open(Config{Path: testDBPath})
	assert.NoError(t, err)
	defer kv.Close()

	tx := kv.NewTX()
	u1, err := tx.Set([]byte("k1"), []byte("v1"))
	assert.True(t, u1)
	assert.NoError(t, err)
	u2, err := tx.Set([]byte("k2"), []byte("v2"))
	assert.True(t, u2)
	assert.NoError(t, err)
	// Reads inside tx see own updates
	v1, ok, err := tx.Get([]byte("k1"))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "v1", string(v1))
	assert.NoError(t, tx.Commit())

	// After commit, reads see persisted data
	v1, ok, err = kv.Get([]byte("k1"))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "v1", string(v1))
	v2, ok, err := kv.Get([]byte("k2"))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "v2", string(v2))
}

func TestStoreTransactionAbort(t *testing.T) {
	defer os.RemoveAll(testDBPath)
	os.RemoveAll(testDBPath)

	kv, err := Open(Config{Path: testDBPath})
	assert.NoError(t, err)
	defer kv.Close()

	tx := kv.NewTX()
	_, err = tx.Set([]byte("k1"), []byte("v1"))
	assert.NoError(t, err)
	tx.Abort()

	_, ok, err := kv.Get([]byte("k1"))
	assert.NoError(t, err)
	assert.False(t, ok)
}

// TestStoreReplayOneKey verifies WAL replay after close/reopen with a single key.
func TestStoreReplayOneKey(t *testing.T) {
	dir := t.TempDir()

	kv, err := Open(Config{Path: dir})
	assert.NoError(t, err)
	_, err = kv.Set([]byte("baz"), []byte("qux"))
	assert.NoError(t, err)
	err = kv.Close()
	assert.NoError(t, err)

	kv2, err := Open(Config{Path: dir})
	assert.NoError(t, err)
	defer kv2.Close()
	val, ok, err := kv2.Get([]byte("baz"))
	assert.NoError(t, err)
	assert.True(t, ok, "baz should be found after reopen")
	assert.Equal(t, "qux", string(val))
}

// TestStoreReplayTwoKeys verifies WAL replay with two commits (Set then Set, no Del).
func TestStoreReplayTwoKeys(t *testing.T) {
	dir := t.TempDir()

	kv, err := Open(Config{Path: dir})
	assert.NoError(t, err)
	_, err = kv.Set([]byte("foo"), []byte("bar"))
	assert.NoError(t, err)
	_, err = kv.Set([]byte("baz"), []byte("qux"))
	assert.NoError(t, err)
	err = kv.Close()
	assert.NoError(t, err)

	kv2, err := Open(Config{Path: dir})
	assert.NoError(t, err)
	defer kv2.Close()
	val, ok, err := kv2.Get([]byte("baz"))
	assert.NoError(t, err)
	assert.True(t, ok, "baz should be found after reopen")
	assert.Equal(t, "qux", string(val))
	val, ok, err = kv2.Get([]byte("foo"))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "bar", string(val))
}

// TestStoreReplaySetDelSet verifies WAL replay with Set, Del, Set (same as TestStore sequence).
func TestStoreReplaySetDelSet(t *testing.T) {
	dir := t.TempDir()

	kv, err := Open(Config{Path: dir})
	assert.NoError(t, err)
	kv.Set([]byte("foo"), []byte("bar"))
	kv.Del([]byte("foo"))
	kv.Set([]byte("baz"), []byte("qux"))
	err = kv.Close()
	assert.NoError(t, err)

	kv2, err := Open(Config{Path: dir})
	assert.NoError(t, err)
	defer kv2.Close()
	_, ok, err := kv2.Get([]byte("foo"))
	assert.NoError(t, err)
	assert.False(t, ok, "foo should be deleted")
	val, ok, err := kv2.Get([]byte("baz"))
	assert.NoError(t, err)
	assert.True(t, ok, "baz should be found after reopen")
	assert.Equal(t, "qux", string(val))
}

// doStoreReopen runs the standard Set(foo), Del(foo), Set(baz) sequence, closes, reopens, and returns the new KV.
func doStoreReopen(t *testing.T, dir string) *KV {
	kv, err := Open(Config{Path: dir})
	assert.NoError(t, err)
	kv.Set([]byte("foo"), []byte("bar"))
	kv.Get([]byte("foo"))
	kv.Get([]byte("missing"))
	kv.Del([]byte("missing"))
	kv.Del([]byte("foo"))
	kv.Get([]byte("foo"))
	kv.Set([]byte("baz"), []byte("qux"))
	kv.Close()
	kv2, err := Open(Config{Path: dir})
	assert.NoError(t, err)
	return kv2
}

// TestStoreReplayExactSequence mirrors TestStore's exact first-run sequence then reopen.
func TestStoreReplayExactSequence(t *testing.T) {
	dir := t.TempDir()
	kv2 := doStoreReopen(t, dir)
	defer kv2.Close()
	_, ok, _ := kv2.Get([]byte("foo"))
	assert.False(t, ok)
	val, ok, err := kv2.Get([]byte("baz"))
	assert.NoError(t, err)
	assert.True(t, ok, "baz should be found")
	assert.Equal(t, "qux", string(val))
}

// TestStoreRecovery (from 0804 TestKVRecovery) verifies replay after truncated or corrupted WAL.
// Only transactions that ended with OpCommit are applied; partial transaction at end is discarded.
func TestStoreRecovery(t *testing.T) {
	dir := t.TempDir()

	// Write k1 and k2 in two separate transactions, then truncate last byte of WAL.
	kv, err := Open(Config{Path: dir})
	assert.NoError(t, err)
	_, err = kv.Set([]byte("k1"), []byte("v1"))
	assert.NoError(t, err)
	_, err = kv.Set([]byte("k2"), []byte("v2"))
	assert.NoError(t, err)
	kv.Close()

	walPath := filepath.Join(dir, "wal")
	fp, err := os.OpenFile(walPath, os.O_RDWR, 0o644)
	assert.NoError(t, err)
	st, err := fp.Stat()
	assert.NoError(t, err)
	err = fp.Truncate(st.Size() - 1)
	assert.NoError(t, err)
	fp.Close()

	kv2, err := Open(Config{Path: dir})
	assert.NoError(t, err)
	defer kv2.Close()
	val, ok, err := kv2.Get([]byte("k1"))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "v1", string(val))
	_, ok, err = kv2.Get([]byte("k2"))
	assert.NoError(t, err)
	assert.False(t, ok, "k2's transaction was truncated so it should not be replayed")
}

// TestStoreRecoveryMultiKeyTX (from 0805 TestKVRecovery) verifies atomicity: one tx with k2+k3,
// truncate WAL, reopen → only k1 (previous tx) is present; k2 and k3 (same tx) are both absent.
func TestStoreRecoveryMultiKeyTX(t *testing.T) {
	dir := t.TempDir()

	kv, err := Open(Config{Path: dir})
	assert.NoError(t, err)
	_, err = kv.Set([]byte("k1"), []byte("v1"))
	assert.NoError(t, err)
	tx := kv.NewTX()
	_, err = tx.Set([]byte("k3"), []byte("v3"))
	assert.NoError(t, err)
	_, err = tx.Set([]byte("k2"), []byte("v2"))
	assert.NoError(t, err)
	err = tx.Commit()
	assert.NoError(t, err)
	kv.Close()

	walPath := filepath.Join(dir, "wal")
	fp, err := os.OpenFile(walPath, os.O_RDWR, 0o644)
	assert.NoError(t, err)
	st, err := fp.Stat()
	assert.NoError(t, err)
	err = fp.Truncate(st.Size() - 1)
	assert.NoError(t, err)
	fp.Close()

	kv2, err := Open(Config{Path: dir})
	assert.NoError(t, err)
	defer kv2.Close()
	val, ok, err := kv2.Get([]byte("k1"))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "v1", string(val))
	_, ok, err = kv2.Get([]byte("k2"))
	assert.NoError(t, err)
	assert.False(t, ok, "k2 in same tx as k3; truncated tx should not be replayed")
	_, ok, err = kv2.Get([]byte("k3"))
	assert.NoError(t, err)
	assert.False(t, ok, "k3 in same tx as k2; truncated tx should not be replayed")
}
