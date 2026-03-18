package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWALResetTXTruncatesTail verifies the rollback path truncates the WAL.
// Without truncation, a stale suffix after writer.committed can make the next
// Open() fail with WAL decode errors.
func TestWALResetTXTruncatesTail(t *testing.T) {
	dir := t.TempDir()

	kv, err := Open(Config{Path: dir, LogThreshold: 1000})
	require.NoError(t, err)

	tx := kv.NewTX()
	_, err = tx.Set([]byte("k1"), []byte("v1"))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	committed := kv.log.writer.committed
	walPath := filepath.Join(dir, "wal")

	// Append a fake WAL record header at the committed offset with an invalid op.
	// WAL record header layout is: crc32(4) | keyLen(4) | valLen(4) | op(1).
	// record.decodeFrom validates op right after reading the header.
	garbage := make([]byte, 4+4+4+1)
	garbage[len(garbage)-1] = 0x99 // invalid op => decodeFrom returns ErrBadChecksum
	fp, err := os.OpenFile(walPath, os.O_RDWR, 0o644)
	require.NoError(t, err)
	_, err = fp.WriteAt(garbage, committed)
	require.NoError(t, err)
	require.NoError(t, fp.Close())

	// Simulate rollback: ResetTX() must truncate the tail back to committed.
	require.NoError(t, kv.log.ResetTX())

	require.NoError(t, kv.Close())

	kv2, err := Open(Config{Path: dir, LogThreshold: 1000})
	require.NoError(t, err)
	defer kv2.Close()

	val, ok, err := kv2.Get([]byte("k1"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "v1", string(val))
}

// TestOpenAllTruncatesEOFPartialTail verifies Open() truncates the WAL tail
// when the file ends with an incomplete record after the last commit.
func TestOpenAllTruncatesEOFPartialTail(t *testing.T) {
	dir := t.TempDir()

	kv, err := Open(Config{Path: dir, LogThreshold: 1000})
	require.NoError(t, err)

	tx := kv.NewTX()
	_, err = tx.Set([]byte("k1"), []byte("v1"))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	committed := kv.log.writer.committed
	walPath := filepath.Join(dir, "wal")
	require.NoError(t, kv.Close())

	// Append 1 byte at committed offset so decodeFrom hits io.EOF while reading
	// the fixed WAL record prefix.
	fp, err := os.OpenFile(walPath, os.O_RDWR, 0o644)
	require.NoError(t, err)
	_, err = fp.WriteAt([]byte{0xAB}, committed)
	require.NoError(t, err)
	require.NoError(t, fp.Close())

	kv2, err := Open(Config{Path: dir, LogThreshold: 1000})
	require.NoError(t, err)
	defer kv2.Close()

	st, err := os.Stat(walPath)
	require.NoError(t, err)
	assert.Equal(t, committed, st.Size(), "WAL should be truncated to the last commit boundary")
}

func TestKVTXAbortAndCommitTerminal(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(Config{Path: dir, LogThreshold: 1000})
	require.NoError(t, err)
	defer kv.Close()

	// Abort is terminal and prevents commit/apply.
	tx := kv.NewTX()
	_, err = tx.Set([]byte("a"), []byte("1"))
	require.NoError(t, err)
	tx.Abort()

	err = tx.Commit()
	require.Error(t, err, "commit after abort should fail")

	_, ok, err := kv.Get([]byte("a"))
	require.NoError(t, err)
	assert.False(t, ok)

	// Successful commit is terminal and prevents reuse.
	tx2 := kv.NewTX()
	_, err = tx2.Set([]byte("b"), []byte("2"))
	require.NoError(t, err)
	require.NoError(t, tx2.Commit())

	err = tx2.Commit()
	require.Error(t, err, "second commit on the same tx should fail")
}

func TestCommitIgnoresPostCommitCompactionErrors(t *testing.T) {
	dir := t.TempDir()

	kv, err := Open(Config{Path: dir, LogThreshold: 1})
	require.NoError(t, err)

	// Make the directory non-writable so creating a new SSTable fails during Compact(),
	// after WAL + MemTable updates have already succeeded.
	require.NoError(t, os.Chmod(dir, 0o500))

	tx := kv.NewTX()
	_, err = tx.Set([]byte("k"), []byte("v"))
	require.NoError(t, err)

	// After the fix, Commit() should not fail solely due to post-commit compaction errors.
	err = tx.Commit()
	assert.NoError(t, err)

	// Restore permissions so the test can cleanly close and reopen.
	require.NoError(t, os.Chmod(dir, 0o700))
	require.NoError(t, kv.Close())

	kv2, err := Open(Config{Path: dir, LogThreshold: 1})
	require.NoError(t, err)
	defer kv2.Close()

	val, ok, err := kv2.Get([]byte("k"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "v", string(val))
}
