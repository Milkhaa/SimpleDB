package relations

import (
	"encoding/binary"
	"fmt"
)

// CellType is the type of a cell value. Used in Schema and Cell.
type CellType int

const (
	CellTypeI64 CellType = iota // int64: 8 bytes LE (value) or BE order-preserving (key)
	CellTypeStr                 // []byte: length-prefixed (value) or null-terminated escaped (key)
)

// Cell holds one value. Set Type and the corresponding field (I64 or Str)
// before EncodeKey/EncodeVal; set Type before DecodeKey/DecodeVal.
type Cell struct {
	Type CellType
	I64  int64
	Str  []byte
}

// encodeStrKey appends the string to toAppend with null-termination and escape encoding.
// 0x00 -> 0x01 0x01, 0x01 -> 0x01 0x02. Order-preserving for bytes.Compare.
func encodeStrKey(toAppend []byte, input []byte) []byte {
	for _, ch := range input {
		if ch == 0x00 || ch == 0x01 {
			toAppend = append(toAppend, 0x01, ch+1)
		} else {
			toAppend = append(toAppend, ch)
		}
	}
	return append(toAppend, 0x00)
}

// decodeStrKey decodes a null-terminated escaped string from data.
// Returns the decoded bytes, the rest of data after the terminator, or an error.
func decodeStrKey(data []byte) (out []byte, rest []byte, err error) {
	for i := 0; i < len(data); i++ {
		if data[i] == 0x00 {
			return out, data[i+1:], nil
		}
		if data[i] == 0x01 {
			if i+1 >= len(data) {
				return nil, nil, ErrTruncatedData
			}
			b := data[i+1]
			if b == 0x01 {
				out = append(out, 0x00)
			} else if b == 0x02 {
				out = append(out, 0x01)
			} else {
				return nil, nil, fmt.Errorf("relations: invalid string key escape 0x01 0x%02x", b)
			}
			i++
			continue
		}
		out = append(out, data[i])
	}
	return nil, nil, ErrTruncatedData
}

// EncodeKey appends the cell's value in order-preserving format for KV keys.
// I64: remap int64 so byte order matches sort order (XOR sign bit, big-endian).
// Str: null-terminated with 0x00/0x01 escaped (see encodeStrKey).
func (c *Cell) EncodeKey(toAppend []byte) []byte {
	switch c.Type {
	case CellTypeI64:
		u := uint64(c.I64) ^ (1 << 63)
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, u)
		return append(toAppend, buf...)
	case CellTypeStr:
		return encodeStrKey(toAppend, c.Str)
	default:
		panic(fmt.Sprintf("relations: unknown CellType: %v", c.Type))
	}
}

// DecodeKey reads one key-encoded value from data into c and returns the remaining bytes.
func (c *Cell) DecodeKey(data []byte) (rest []byte, err error) {
	switch c.Type {
	case CellTypeI64:
		if len(data) < 8 {
			return data, ErrTruncatedData
		}
		u := binary.BigEndian.Uint64(data[:8])
		c.I64 = int64(u ^ (1 << 63))
		return data[8:], nil
	case CellTypeStr:
		out, rest, err := decodeStrKey(data)
		if err != nil {
			return data, err
		}
		c.Str = append(c.Str[:0], out...)
		return rest, nil
	default:
		return data, fmt.Errorf("relations: unknown CellType: %v", c.Type)
	}
}

// EncodeVal appends the cell's value in the value format (for KV values).
// I64: 8 bytes little-endian. Str: 4-byte length (LE) then raw bytes.
func (c *Cell) EncodeVal(toAppend []byte) []byte {
	switch c.Type {
	case CellTypeI64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(c.I64))
		return append(toAppend, buf...)
	case CellTypeStr:
		buf := make([]byte, 4+len(c.Str))
		binary.LittleEndian.PutUint32(buf, uint32(len(c.Str)))
		copy(buf[4:], c.Str)
		return append(toAppend, buf...)
	default:
		panic(fmt.Sprintf("relations: unknown CellType: %v", c.Type))
	}
}

// DecodeVal reads one value-encoded cell from data into c and returns the remaining bytes.
// Returns ErrTruncatedData if the buffer is too short.
func (c *Cell) DecodeVal(data []byte) (rest []byte, err error) {
	switch c.Type {
	case CellTypeI64:
		if len(data) < 8 {
			return data, ErrTruncatedData
		}
		c.I64 = int64(binary.LittleEndian.Uint64(data[:8]))
		return data[8:], nil
	case CellTypeStr:
		if len(data) < 4 {
			return data, ErrTruncatedData
		}
		n := int(binary.LittleEndian.Uint32(data[:4]))
		if n < 0 || len(data) < 4+n {
			return data, ErrTruncatedData
		}
		c.Str = append(c.Str[:0], data[4:4+n]...)
		return data[4+n:], nil
	default:
		return data, fmt.Errorf("relations: unknown CellType: %v", c.Type)
	}
}
