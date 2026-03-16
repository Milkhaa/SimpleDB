package engine

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testMetadata(t *testing.T, reopen bool) {
	dir := ".tmp_meta"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	require.NoError(t, os.MkdirAll(dir, 0o755))

	store := KVMetaStore{}
	err := store.Open(dir)
	require.Nil(t, err)
	defer store.Close()

	for i := uint64(1); i < 10; i++ {
		if reopen {
			err = store.Close()
			require.Nil(t, err)
			err = store.Open(dir)
			require.Nil(t, err)
		}

		meta := store.Get()
		assert.Equal(t, i-1, meta.Version)
		err = store.Set(KVMetaData{Version: i})
		require.Nil(t, err)
		meta = store.Get()
		assert.Equal(t, i, meta.Version)
	}
}

func TestMetadata(t *testing.T) {
	testMetadata(t, false)
	testMetadata(t, true)
}

func testMetadataRecovery(t *testing.T, truncate bool) {
	dir := ".tmp_meta_rec"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	require.NoError(t, os.MkdirAll(dir, 0o755))

	store := KVMetaStore{}
	err := store.Open(dir)
	require.Nil(t, err)
	defer store.Close()

	err = store.Set(KVMetaData{Version: 99})
	require.Nil(t, err)
	err = store.Set(KVMetaData{Version: 100})
	require.Nil(t, err)

	cur := store.current()
	fp := store.slots[cur].fp
	st, err := fp.Stat()
	require.Nil(t, err)
	if truncate {
		err = fp.Truncate(st.Size() - 1)
	} else {
		_, err = fp.WriteAt([]byte{0}, st.Size()-1)
	}
	require.Nil(t, err)

	err = store.Close()
	require.Nil(t, err)
	err = store.Open(dir)
	require.Nil(t, err)
	meta := store.Get()
	assert.Equal(t, uint64(99), meta.Version)
}

func TestMetadataRecovery(t *testing.T) {
	testMetadataRecovery(t, false)
	testMetadataRecovery(t, true)
}
