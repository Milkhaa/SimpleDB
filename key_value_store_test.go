package simpledb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyValueStore(t *testing.T) {
	kv := KeyValueStore{}
	err := kv.Open()
	assert.Nil(t, err)
	defer kv.Close()

	updated, err := kv.Set([]byte("k1"), []byte("v1"))
	assert.True(t, updated && err == nil)

	val, ok, err := kv.Get([]byte("k1"))
	assert.True(t, string(val) == "v1" && ok && err == nil)

	updated, err = kv.Set([]byte("k1"), []byte("v1"))
	assert.True(t, !updated && err == nil)

	updated, err = kv.Set([]byte("k1"), []byte("new-v1"))
	assert.True(t, updated && err == nil)

	val, ok, err = kv.Get([]byte("k1"))
	assert.True(t, string(val) == "new-v1" && ok && err == nil)

	_, ok, err = kv.Get([]byte("non-existing-key"))
	assert.True(t, !ok && err == nil)

	updated, err = kv.Del([]byte("non-existing-key"))
	assert.True(t, !updated && err == nil)

	updated, err = kv.Del([]byte("k1"))
	assert.True(t, updated && err == nil)

	_, ok, err = kv.Get([]byte("k1"))
	assert.True(t, !ok && err == nil)
}
