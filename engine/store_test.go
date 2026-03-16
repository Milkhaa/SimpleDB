package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testDBPath = ".tmp_kv"

func TestStore(t *testing.T) {
	defer os.RemoveAll(testDBPath)
	os.RemoveAll(testDBPath)

	kv, err := Open(Config{Path: testDBPath})
	assert.NoError(t, err)
	defer kv.Close()

	updated, err := kv.Set([]byte("foo"), []byte("bar"))
	assert.True(t, updated)
	assert.NoError(t, err)

	val, ok, err := kv.Get([]byte("foo"))
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.Equal(t, "bar", string(val))

	_, ok, err = kv.Get([]byte("missing"))
	assert.False(t, ok)
	assert.NoError(t, err)

	updated, err = kv.Del([]byte("missing"))
	assert.False(t, updated)
	assert.NoError(t, err)

	updated, err = kv.Del([]byte("foo"))
	assert.True(t, updated)
	assert.NoError(t, err)

	_, ok, err = kv.Get([]byte("foo"))
	assert.False(t, ok)
	assert.NoError(t, err)

	updated, err = kv.Set([]byte("baz"), []byte("qux"))
	assert.True(t, updated)
	assert.NoError(t, err)

	// Simulate restart: close and reopen.
	kv.Close()
	kv, err = Open(Config{Path: testDBPath})
	assert.NoError(t, err)

	_, ok, err = kv.Get([]byte("foo"))
	assert.False(t, ok)
	assert.NoError(t, err)

	val, ok, err = kv.Get([]byte("baz"))
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.Equal(t, "qux", string(val))

	updated, err = kv.Set([]byte("zap"), []byte("bang"))
	assert.True(t, updated)
	assert.NoError(t, err)

	kv.Close()

	// Truncate last byte of WAL to simulate incomplete write.
	walPath := filepath.Join(testDBPath, "wal")
	fp, err := os.OpenFile(walPath, os.O_RDWR, 0o644)
	assert.NoError(t, err)
	st, _ := fp.Stat()
	err = fp.Truncate(st.Size() - 1)
	assert.NoError(t, err)
	fp.Close()

	kv, err = Open(Config{Path: testDBPath})
	assert.NoError(t, err)
	val, ok, err = kv.Get([]byte("baz"))
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.Equal(t, "qux", string(val))
	_, ok, err = kv.Get([]byte("zap"))
	assert.False(t, ok)
	assert.NoError(t, err)
	kv.Close()

	// Corrupt last byte of WAL to simulate bad checksum.
	fp, err = os.OpenFile(walPath, os.O_RDWR, 0o644)
	assert.NoError(t, err)
	st, _ = fp.Stat()
	_, err = fp.WriteAt([]byte{0}, st.Size()-1)
	assert.NoError(t, err)
	fp.Close()

	kv, err = Open(Config{Path: testDBPath})
	assert.NoError(t, err)
	val, ok, err = kv.Get([]byte("baz"))
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.Equal(t, "qux", string(val))
	_, ok, err = kv.Get([]byte("zap"))
	assert.False(t, ok)
	assert.NoError(t, err)
	kv.Close()
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
