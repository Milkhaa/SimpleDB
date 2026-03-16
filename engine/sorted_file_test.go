package engine

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSortedFile(t *testing.T) {
	sf := SortedFile{FileName: ".tmp_sorted_file"}
	defer os.Remove(sf.FileName)

	keys := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	vals := [][]byte{[]byte("one"), []byte(""), []byte("two")}
	deleted := []bool{false, true, false}
	arr := &SortedArray{}
	arr.keys = keys
	arr.vals = vals
	arr.deleted = deleted
	err := sf.CreateFromSorted(arr)
	require.Nil(t, err)
	defer sf.Close()
	assert.Equal(t, 3, sf.EstimatedSize())

	// Header: nkeys=3, then 3 offsets. Entry start at 8+24=32.
	// Entry: 4+4+1 + key + val. alpha/one: 9+5+3=17 → next 32+17=49. beta/"": 9+4+0=13 → 49+13=62. gamma/two: 9+5+3=17.
	expected := []byte{
		3, 0, 0, 0, 0, 0, 0, 0,
		32, 0, 0, 0, 0, 0, 0, 0,
		49, 0, 0, 0, 0, 0, 0, 0,
		62, 0, 0, 0, 0, 0, 0, 0,
		5, 0, 0, 0, 3, 0, 0, 0, 0, 'a', 'l', 'p', 'h', 'a', 'o', 'n', 'e',
		4, 0, 0, 0, 0, 0, 0, 0, 1, 'b', 'e', 't', 'a',
		5, 0, 0, 0, 3, 0, 0, 0, 0, 'g', 'a', 'm', 'm', 'a', 't', 'w', 'o',
	}
	data, err := os.ReadFile(sf.FileName)
	require.Nil(t, err)
	assert.Equal(t, expected, data)

	i := 0
	iter, err := sf.Iter()
	require.Nil(t, err)
	for ; err == nil && iter.Valid(); err = iter.Next() {
		assert.Equal(t, keys[i], iter.Key())
		assert.Equal(t, vals[i], iter.Val())
		i++
	}
	require.Nil(t, err)

	iter, err = sf.Seek([]byte("beta"))
	require.Nil(t, err)
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("beta"), iter.Key())
}
