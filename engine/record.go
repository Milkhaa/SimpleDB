package engine

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

type RecordOp byte

// WAL record op: Add, Del, or Commit (transaction boundary).
const (
	OpAdd    RecordOp = 0
	OpDel    RecordOp = 1
	OpCommit RecordOp = 2
)

// record is a single WAL record. For OpCommit, key and val are unused.
type record struct {
	key []byte
	val []byte
	op  RecordOp
}

// WAL record layout (one record per Set/Del in the write-ahead log):
//
//	crc32(4) | keyLen(4) | valLen(4) | op(1) | key | val
//
// Checksum is crc32(IEEE) over (keyLen|valLen|op|key|val) and is used to
// detect partial writes on replay.
func (r *record) encode() ([]byte, error) {
	keyLen, valLen := len(r.key), len(r.val)
	data := make([]byte, 4+4+4+1+keyLen+valLen)
	// Put op after lengths (matches the requested encode layout).
	data[4+4+4] = byte(r.op)
	binary.LittleEndian.PutUint32(data[4:8], uint32(keyLen))
	binary.LittleEndian.PutUint32(data[8:12], uint32(valLen))
	copy(data[4+4+4+1:], r.key)
	copy(data[4+4+4+1+keyLen:], r.val)
	binary.LittleEndian.PutUint32(data[0:4], crc32.ChecksumIEEE(data[4:]))
	return data, nil
}

// decodeFrom reads one record from rd. Returns (bytesRead, commitSeen, err). io.EOF means end of file.
func (r *record) decodeFrom(rd io.Reader) (bytesRead int, commitSeen bool, err error) {
	// Fixed prefix: crc32(4) | keyLen(4) | valLen(4) | op(1)
	var header [4 + 4 + 4 + 1]byte
	n, err := io.ReadFull(rd, header[:])
	if err != nil {
		return n, false, err
	}

	stored := binary.LittleEndian.Uint32(header[0:4])
	klen := int(binary.LittleEndian.Uint32(header[4:8]))
	vlen := int(binary.LittleEndian.Uint32(header[8:12]))
	r.op = RecordOp(header[12])

	if r.op != OpAdd && r.op != OpDel && r.op != OpCommit {
		return n, false, ErrBadChecksum
	}

	dataLen := klen + vlen
	data := make([]byte, dataLen)
	n2, err := io.ReadFull(rd, data)
	total := n + n2
	if err != nil {
		return total, false, err
	}

	computed := crc32.ChecksumIEEE(append(header[4:], data...))
	if computed != stored {
		return total, false, ErrBadChecksum
	}

	r.key = data[:klen]
	r.val = nil
	if vlen > 0 {
		r.val = data[klen:]
	}
	return total, r.op == OpCommit, nil
}
