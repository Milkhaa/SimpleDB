package relations

import (
	"encoding/binary"
	"fmt"
)

type CellType int

const (
	CellTypeI64 CellType = iota
	CellTypeStr
)

// Cell holds a single value of type CellTypeI64 or CellTypeStr.
type Cell struct {
	Type CellType
	I64  int64
	Str  []byte
}

// Encode appends the cell's value to dst (or allocates if dst is nil) and returns the slice.
// CellTypeI64: 8 bytes little-endian. CellTypeStr: 4-byte length (LE) then raw bytes.
func (c *Cell) Encode(dst []byte) []byte {
	switch c.Type {
	case CellTypeI64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(c.I64))
		return append(dst, buf...)
	case CellTypeStr:
		buf := make([]byte, 4+len(c.Str))
		binary.LittleEndian.PutUint32(buf, uint32(len(c.Str)))
		copy(buf[4:], c.Str)
		return append(dst, buf...)
	default:
		panic(fmt.Sprintf("relations: unknown CellType: %v", c.Type))
	}
}

// Decode reads one value from src into c and returns the remaining bytes and any error.
// Returns ErrTruncatedData if the buffer is too short.
func (c *Cell) Decode(src []byte) (rest []byte, err error) {
	switch c.Type {
	case CellTypeI64:
		if len(src) < 8 {
			return src, ErrTruncatedData
		}
		c.I64 = int64(binary.LittleEndian.Uint64(src[:8]))
		return src[8:], nil
	case CellTypeStr:
		if len(src) < 4 {
			return src, ErrTruncatedData
		}
		n := int(binary.LittleEndian.Uint32(src[:4]))
		if len(src) < 4+n {
			return src, ErrTruncatedData
		}
		c.Str = append(c.Str[:0], src[4:4+n]...)
		return src[4+n:], nil
	default:
		return src, fmt.Errorf("relations: unknown CellType: %v", c.Type)
	}
}
