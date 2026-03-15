package simpledb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRowEncode(t *testing.T) {
	schema := &Schema{
		Table: "event",
		Cols: []Column{
			{Name: "ts", Type: CellTypeI64},
			{Name: "kind", Type: CellTypeStr},
			{Name: "payload", Type: CellTypeStr},
		},
		PKey: []int{0, 1}, // (ts, kind)
	}

	row := Row{
		Cell{Type: CellTypeI64, I64: 1000},
		Cell{Type: CellTypeStr, Str: []byte("click")},
		Cell{Type: CellTypeStr, Str: []byte("button_a")},
	}
	// key: "event\0" + I64(1000) LE + len("click") LE + "click"
	key := []byte{'e', 'v', 'e', 'n', 't', 0, 0xe8, 0x03, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 'c', 'l', 'i', 'c', 'k'}
	// val: len("button_a") LE + "button_a"
	val := []byte{8, 0, 0, 0, 'b', 'u', 't', 't', 'o', 'n', '_', 'a'}
	assert.Equal(t, key, row.EncodeKey(schema))
	assert.Equal(t, val, row.EncodeVal(schema))

	decoded := schema.NewRow()
	err := decoded.DecodeKey(schema, key)
	assert.Nil(t, err)
	err = decoded.DecodeVal(schema, val)
	assert.Nil(t, err)
	assert.Equal(t, row, decoded)
}
