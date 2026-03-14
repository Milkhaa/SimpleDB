package simpledb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testDBPath = ".test_db"

func TestStore(t *testing.T) {
	defer os.Remove(testDBPath)
	os.Remove(testDBPath)

	kv, err := Open(Config{Path: testDBPath})
	assert.NoError(t, err)
	defer kv.Close()

	updated, err := kv.Set([]byte("k1"), []byte("v1"))
	assert.True(t, updated)
	assert.NoError(t, err)

	val, ok, err := kv.Get([]byte("k1"))
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.Equal(t, "v1", string(val))

	_, ok, err = kv.Get([]byte("non-existing-key"))
	assert.False(t, ok)
	assert.NoError(t, err)

	updated, err = kv.Del([]byte("non-existing-key"))
	assert.False(t, updated)
	assert.NoError(t, err)

	updated, err = kv.Del([]byte("k1"))
	assert.True(t, updated)
	assert.NoError(t, err)

	_, ok, err = kv.Get([]byte("k1"))
	assert.False(t, ok)
	assert.NoError(t, err)

	updated, err = kv.Set([]byte("k2"), []byte("v2"))
	assert.True(t, updated)
	assert.NoError(t, err)

	// Simulate restart: close and reopen.
	kv.Close()
	kv, err = Open(Config{Path: testDBPath})
	assert.NoError(t, err)

	_, ok, err = kv.Get([]byte("k1"))
	assert.False(t, ok)
	assert.NoError(t, err)

	val, ok, err = kv.Get([]byte("k2"))
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.Equal(t, "v2", string(val))

	updated, err = kv.Set([]byte("k3"), []byte("v3"))
	assert.True(t, updated)
	assert.NoError(t, err)

	kv.Close()

	// Truncate last byte of log to simulate incomplete write.
	fp, err := os.OpenFile(testDBPath, os.O_RDWR, 0o644)
	assert.NoError(t, err)
	st, _ := fp.Stat()
	err = fp.Truncate(st.Size() - 1)
	assert.NoError(t, err)
	fp.Close()

	kv, err = Open(Config{Path: testDBPath})
	assert.NoError(t, err)
	val, ok, err = kv.Get([]byte("k2"))
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.Equal(t, "v2", string(val))
	_, ok, err = kv.Get([]byte("k3"))
	assert.False(t, ok)
	assert.NoError(t, err)
	kv.Close()

	// Corrupt last byte to simulate bad checksum.
	fp, err = os.OpenFile(testDBPath, os.O_RDWR, 0o644)
	assert.NoError(t, err)
	st, _ = fp.Stat()
	_, err = fp.WriteAt([]byte{0}, st.Size()-1)
	assert.NoError(t, err)
	fp.Close()

	kv, err = Open(Config{Path: testDBPath})
	assert.NoError(t, err)
	val, ok, err = kv.Get([]byte("k2"))
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.Equal(t, "v2", string(val))
	_, ok, err = kv.Get([]byte("k3"))
	assert.False(t, ok)
	assert.NoError(t, err)
	kv.Close()
}
