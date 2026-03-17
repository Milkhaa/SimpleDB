package engine

import (
	"encoding/binary"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSortedFile_KeyOrValueLengthTruncationWhenOverMaxUint32 reproduces the bug where
// key/value lengths > math.MaxUint32 are cast to uint32 for the 4-byte header, causing
// silent truncation and corrupted SSTable data.
func TestSortedFile_KeyOrValueLengthTruncationWhenOverMaxUint32(t *testing.T) {
	// The SSTable entry header stores key length and value length as uint32 (4 bytes each).
	// Encoding a length > math.MaxUint32 truncates; decoding then yields the wrong length.
	lengthOverMax := int64(math.MaxUint32) + 1
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(lengthOverMax))
	decoded := binary.LittleEndian.Uint32(buf)
	// uint32(lengthOverMax) wraps to 0, so decoded length is wrong → corruption when read back.
	assert.NotEqual(t, lengthOverMax, int64(decoded),
		"encoding length > MaxUint32 as uint32 truncates; decoded value must not equal original length")
	assert.Equal(t, uint32(0), decoded, "MaxUint32+1 wraps to 0 in uint32")
}

// TestSortedFile_RejectsKeyOrValueLengthOverMaxUint32 verifies that the write path rejects
// key or value lengths that would truncate when stored as uint32, preventing data corruption.
func TestSortedFile_RejectsKeyOrValueLengthOverMaxUint32(t *testing.T) {
	err := checkEntryLengths(math.MaxUint32+1, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrKeyOrValueTooLarge)

	err = checkEntryLengths(0, math.MaxUint32+1)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrKeyOrValueTooLarge)

	require.NoError(t, checkEntryLengths(math.MaxUint32, math.MaxUint32))
	require.NoError(t, checkEntryLengths(0, 0))
}

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
