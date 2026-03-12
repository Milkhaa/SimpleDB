package simpledb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyValueStore(t *testing.T) {
	kv := KeyValueStore{}
	kv.disk.FileName = ".test_db"
	defer os.Remove(kv.disk.FileName)

	os.Remove(kv.disk.FileName)
	err := kv.Open()
	assert.Nil(t, err)
	defer kv.Close()

	updated, err := kv.Set([]byte("k1"), []byte("v1"))
	assert.True(t, updated && err == nil)

	val, ok, err := kv.Get([]byte("k1"))
	assert.True(t, string(val) == "v1" && ok && err == nil)

	_, ok, err = kv.Get([]byte("non-existing-key"))
	assert.True(t, !ok && err == nil)

	updated, err = kv.Del([]byte("non-existing-key"))
	assert.True(t, !updated && err == nil)

	updated, err = kv.Del([]byte("k1"))
	assert.True(t, updated && err == nil)

	val, ok, err = kv.Get([]byte("k1"))
	assert.True(t, !ok && err == nil)

	_, ok, err = kv.Get([]byte("non-existing-key"))
	assert.True(t, !ok && err == nil)

	updated, err = kv.Set([]byte("k2"), []byte("v2"))
	assert.True(t, updated && err == nil)

	// simulate database restart
	kv.Close()
	err = kv.Open()
	assert.Nil(t, err)

	_, ok, err = kv.Get([]byte("k1"))
	assert.True(t, !ok && err == nil) //because k1 was added and then deleted in last step

	val, ok, err = kv.Get([]byte("k2")) // k2 still there
	assert.True(t, string(val) == "v2" && ok && err == nil)
}
