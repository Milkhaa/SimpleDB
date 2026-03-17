package engine

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
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

// writeCorruptSSTable writes a minimal file with the given key count in the header (first 8 bytes).
// Used to test Open rejection of corrupt headers (overflow or bad offsets).
func writeCorruptSSTable(t *testing.T, path string, nKeys uint64, offsets []uint64) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, nKeys)
	_, err = f.Write(buf)
	require.NoError(t, err)
	for _, o := range offsets {
		binary.LittleEndian.PutUint64(buf, o)
		_, err = f.Write(buf)
		require.NoError(t, err)
	}
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
}

// TestSortedFile_Open_RejectsHeaderCountThatOverflows reproduces the bug where a corrupt n64
// can make int64(n)*8 overflow, so headerSize > fileSize fails to catch it and make([]uint64, n) can panic/OOM.
func TestSortedFile_Open_RejectsHeaderCountThatOverflows(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt_count")
	// n such that 8 + n*8 overflows int64 (e.g. n = 1 + (math.MaxInt64-8)/8)
	n := uint64(1) + uint64((math.MaxInt64-8)/8)
	// Write only the count; no offset table (file is 8 bytes).
	writeCorruptSSTable(t, path, n, nil)

	sf := SortedFile{FileName: path}
	err := sf.Open()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCorruptSSTable)
}

// TestSortedFile_Open_ValidatesOffsets reproduces the bug where offsets are trusted verbatim:
// offset inside header, past fileSize, or out of order can cause readEntry to misbehave.
func TestSortedFile_Open_ValidatesOffsets(t *testing.T) {
	dir := t.TempDir()

	t.Run("offset_inside_header", func(t *testing.T) {
		path := filepath.Join(dir, "offset_inside")
		// nkeys=1, offset 0 (inside header: 8+8=16 bytes)
		writeCorruptSSTable(t, path, 1, []uint64{0})
		sf := SortedFile{FileName: path}
		err := sf.Open()
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCorruptSSTable)
	})

	t.Run("offset_past_file", func(t *testing.T) {
		path := filepath.Join(dir, "offset_past")
		// nkeys=1, header ends at 16; offset 1000 with file only 16 bytes
		writeCorruptSSTable(t, path, 1, []uint64{1000})
		sf := SortedFile{FileName: path}
		err := sf.Open()
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCorruptSSTable)
	})

	t.Run("offsets_out_of_order", func(t *testing.T) {
		path := filepath.Join(dir, "out_of_order")
		// nkeys=2, offsets must be strictly increasing; 32 then 24 is invalid
		writeCorruptSSTable(t, path, 2, []uint64{32, 24})
		sf := SortedFile{FileName: path}
		err := sf.Open()
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCorruptSSTable)
	})
}

// TestSortedFile_Seek_AfterLastKeepsPositionForPrevNext reproduces the bug where Seek to a key
// greater than all keys set position to -1, breaking Prev() (can't step to last) and Next() (wrapped to first).
func TestSortedFile_Seek_AfterLastKeepsPositionForPrevNext(t *testing.T) {
	sf := SortedFile{FileName: ".tmp_seek_after"}
	defer os.Remove(sf.FileName)

	arr := &SortedArray{}
	arr.keys = [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	arr.vals = [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	arr.deleted = []bool{false, false, false}
	err := sf.CreateFromSorted(arr)
	require.NoError(t, err)
	require.NoError(t, sf.Close())
	require.NoError(t, sf.Open())
	defer sf.Close()

	// Seek to key > all keys: should be "after last", not "before first".
	iter, err := sf.Seek([]byte("z"))
	require.NoError(t, err)
	require.False(t, iter.Valid(), "Seek(z) should be past last, not valid")

	// Prev() from after last must move to last entry.
	err = iter.Prev()
	require.NoError(t, err)
	require.True(t, iter.Valid())
	assert.Equal(t, []byte("c"), iter.Key())
	assert.Equal(t, []byte("3"), iter.Val())

	// Seek past last again; Next() should stay invalid (not wrap to first).
	iter, err = sf.Seek([]byte("z"))
	require.NoError(t, err)
	require.False(t, iter.Valid())
	err = iter.Next()
	require.NoError(t, err)
	require.False(t, iter.Valid(), "Next() from after last must stay invalid, not wrap to first")
}
