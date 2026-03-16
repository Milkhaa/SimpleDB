package relations

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableCell(t *testing.T) {
	t.Run("I64_roundtrip", func(t *testing.T) {
		values := []int64{
			0, 1, -1,
			-2, 255, -255,
			1 << 20, -(1 << 20),
			0x7fffffffffffffff, -0x8000000000000000, // max and min int64
		}
		for _, v := range values {
			cell := Cell{Type: CellTypeI64, I64: v}
			data := cell.Encode(nil)
			assert.Len(t, data, 8, "I64 encodes to 8 bytes")
			decoded := Cell{Type: CellTypeI64}
			rest, err := decoded.Decode(data)
			assert.NoError(t, err)
			assert.Len(t, rest, 0)
			assert.Equal(t, cell.I64, decoded.I64, "value %d", v)
		}
	})

	t.Run("I64_encode_exact", func(t *testing.T) {
		cell := Cell{Type: CellTypeI64, I64: -2}
		expect := []byte{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
		assert.Equal(t, expect, cell.Encode(nil))
	})

	t.Run("Str_roundtrip", func(t *testing.T) {
		values := [][]byte{
			nil,
			{},
			{'x'},
			[]byte("hello"),
			[]byte("asdf"),
			[]byte("prefix\x00suffix"), // with null byte
			bytes.Repeat([]byte("ab"), 100),
		}
		for i, v := range values {
			cell := Cell{Type: CellTypeStr, Str: v}
			data := cell.Encode(nil)
			decoded := Cell{Type: CellTypeStr}
			rest, err := decoded.Decode(data)
			assert.NoError(t, err, "case %d", i)
			assert.Len(t, rest, 0, "case %d", i)
			assert.True(t, bytes.Equal(cell.Str, decoded.Str), "case %d: expected %q, got %q", i, cell.Str, decoded.Str)
		}
	})

	t.Run("Str_encode_exact", func(t *testing.T) {
		cell := Cell{Type: CellTypeStr, Str: []byte("asdf")}
		expect := []byte{4, 0, 0, 0, 'a', 's', 'd', 'f'} // "asdf" has 4 bytes, so length is 4
		assert.Equal(t, expect, cell.Encode(nil))
	})

	t.Run("Encode_appends_to_dst", func(t *testing.T) {
		prefix := []byte("prefix")
		cell := Cell{Type: CellTypeI64, I64: 42}
		out := cell.Encode(prefix)
		assert.True(t, len(out) == len(prefix)+8)
		assert.Equal(t, []byte("prefix"), out[:6])
		decoded := Cell{Type: CellTypeI64}
		rest, _ := decoded.Decode(out[6:])
		assert.Len(t, rest, 0)
		assert.Equal(t, int64(42), decoded.I64)
	})

	t.Run("Decode_returns_rest", func(t *testing.T) {
		cell1 := Cell{Type: CellTypeI64, I64: 1}
		cell2 := Cell{Type: CellTypeI64, I64: 2}
		buf := append(cell1.Encode(nil), cell2.Encode(nil)...)
		decoded1 := Cell{Type: CellTypeI64}
		rest, err := decoded1.Decode(buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), decoded1.I64)
		assert.Len(t, rest, 8)
		decoded2 := Cell{Type: CellTypeI64}
		rest2, err := decoded2.Decode(rest)
		assert.NoError(t, err)
		assert.Len(t, rest2, 0)
		assert.Equal(t, int64(2), decoded2.I64)
	})

	t.Run("Decode_short_buffer_returns_error", func(t *testing.T) {
		short := []byte{0, 1, 2}
		decoded := Cell{Type: CellTypeI64}
		rest, err := decoded.Decode(short)
		assert.ErrorIs(t, err, ErrTruncatedData)
		assert.Equal(t, short, rest, "rest is unchanged on truncated input")
	})
}
