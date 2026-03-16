package relations

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableCellVal(t *testing.T) {
	t.Run("I64_roundtrip", func(t *testing.T) {
		values := []int64{
			0, 1, -1,
			-2, 255, -255,
			1 << 20, -(1 << 20),
			0x7fffffffffffffff, -0x8000000000000000,
		}
		for _, v := range values {
			cell := Cell{Type: CellTypeI64, I64: v}
			data := cell.EncodeVal(nil)
			assert.Len(t, data, 8, "I64 encodes to 8 bytes")
			decoded := Cell{Type: CellTypeI64}
			rest, err := decoded.DecodeVal(data)
			assert.NoError(t, err)
			assert.Len(t, rest, 0)
			assert.Equal(t, cell.I64, decoded.I64, "value %d", v)
		}
	})

	t.Run("I64_encode_exact", func(t *testing.T) {
		cell := Cell{Type: CellTypeI64, I64: -2}
		expect := []byte{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
		assert.Equal(t, expect, cell.EncodeVal(nil))
	})

	t.Run("Str_roundtrip", func(t *testing.T) {
		values := [][]byte{
			nil,
			{},
			{'x'},
			[]byte("hello"),
			[]byte("asdf"),
			[]byte("prefix\x00suffix"),
			bytes.Repeat([]byte("ab"), 100),
		}
		for i, v := range values {
			cell := Cell{Type: CellTypeStr, Str: v}
			data := cell.EncodeVal(nil)
			decoded := Cell{Type: CellTypeStr}
			rest, err := decoded.DecodeVal(data)
			assert.NoError(t, err, "case %d", i)
			assert.Len(t, rest, 0, "case %d", i)
			assert.True(t, bytes.Equal(cell.Str, decoded.Str), "case %d: expected %q, got %q", i, cell.Str, decoded.Str)
		}
	})

	t.Run("Str_encode_exact", func(t *testing.T) {
		cell := Cell{Type: CellTypeStr, Str: []byte("asdf")}
		expect := []byte{4, 0, 0, 0, 'a', 's', 'd', 'f'}
		assert.Equal(t, expect, cell.EncodeVal(nil))
	})

	t.Run("EncodeVal_appends_to_dst", func(t *testing.T) {
		prefix := []byte("prefix")
		cell := Cell{Type: CellTypeI64, I64: 42}
		out := cell.EncodeVal(prefix)
		assert.True(t, len(out) == len(prefix)+8)
		assert.Equal(t, []byte("prefix"), out[:6])
		decoded := Cell{Type: CellTypeI64}
		rest, _ := decoded.DecodeVal(out[6:])
		assert.Len(t, rest, 0)
		assert.Equal(t, int64(42), decoded.I64)
	})

	t.Run("DecodeVal_returns_rest", func(t *testing.T) {
		cell1 := Cell{Type: CellTypeI64, I64: 1}
		cell2 := Cell{Type: CellTypeI64, I64: 2}
		buf := append(cell1.EncodeVal(nil), cell2.EncodeVal(nil)...)
		decoded1 := Cell{Type: CellTypeI64}
		rest, err := decoded1.DecodeVal(buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), decoded1.I64)
		assert.Len(t, rest, 8)
		decoded2 := Cell{Type: CellTypeI64}
		rest2, err := decoded2.DecodeVal(rest)
		assert.NoError(t, err)
		assert.Len(t, rest2, 0)
		assert.Equal(t, int64(2), decoded2.I64)
	})

	t.Run("DecodeVal_short_buffer_returns_error", func(t *testing.T) {
		short := []byte{0, 1, 2}
		decoded := Cell{Type: CellTypeI64}
		rest, err := decoded.DecodeVal(short)
		assert.ErrorIs(t, err, ErrTruncatedData)
		assert.Equal(t, short, rest)
	})
}

func TestTableCellKey(t *testing.T) {
	t.Run("I64_key_roundtrip", func(t *testing.T) {
		values := []int64{
			-0x8000000000000000, -1, 0, 1,
			0x7fffffffffffffff,
		}
		for _, v := range values {
			cell := Cell{Type: CellTypeI64, I64: v}
			data := cell.EncodeKey(nil)
			assert.Len(t, data, 8)
			decoded := Cell{Type: CellTypeI64}
			rest, err := decoded.DecodeKey(data)
			assert.NoError(t, err)
			assert.Len(t, rest, 0)
			assert.Equal(t, cell.I64, decoded.I64, "value %d", v)
		}
	})

	t.Run("I64_key_sort_order", func(t *testing.T) {
		// Encoded keys should compare in numeric order: neg < 0 < pos
		vals := []int64{-2, -1, 0, 1, 2}
		var encoded [][]byte
		for _, v := range vals {
			c := Cell{Type: CellTypeI64, I64: v}
			encoded = append(encoded, c.EncodeKey(nil))
		}
		for i := 0; i < len(encoded)-1; i++ {
			assert.Equal(t, -1, bytes.Compare(encoded[i], encoded[i+1]), "encoded[%d] < encoded[%d]", i, i+1)
		}
	})

	t.Run("Str_key_roundtrip", func(t *testing.T) {
		values := [][]byte{
			{},
			[]byte("a"),
			[]byte("click"),
			[]byte("prefix\x00suffix"),
			[]byte("\x01\x00"),
		}
		for i, v := range values {
			cell := Cell{Type: CellTypeStr, Str: v}
			data := cell.EncodeKey(nil)
			decoded := Cell{Type: CellTypeStr}
			rest, err := decoded.DecodeKey(data)
			assert.NoError(t, err, "case %d", i)
			assert.Len(t, rest, 0, "case %d", i)
			assert.True(t, bytes.Equal(cell.Str, decoded.Str), "case %d", i)
		}
	})

	t.Run("Str_key_sort_order", func(t *testing.T) {
		// Null-terminated: "a" < "aa" < "b"
		strs := []string{"a", "aa", "b"}
		var encoded [][]byte
		for _, s := range strs {
			c := Cell{Type: CellTypeStr, Str: []byte(s)}
			encoded = append(encoded, c.EncodeKey(nil))
		}
		for i := 0; i < len(encoded)-1; i++ {
			assert.Equal(t, -1, bytes.Compare(encoded[i], encoded[i+1]), "encoded[%d] < encoded[%d]", i, i+1)
		}
	})

	t.Run("DecodeKey_short_buffer_returns_error", func(t *testing.T) {
		short := []byte{0, 1, 2}
		decoded := Cell{Type: CellTypeI64}
		rest, err := decoded.DecodeKey(short)
		assert.ErrorIs(t, err, ErrTruncatedData)
		assert.Equal(t, short, rest)
	})
}
