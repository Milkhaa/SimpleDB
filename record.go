package simpledb

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

// Log record layout (see README):
//
//	crc32(4) | keyLen(4) | valLen(4) | deleted(1) | key | val
const (
	recordHeaderSize = 4 + 4 + 4 + 1 // crc32 + keyLen + valLen + deleted
	checksumSize     = 4
	keyLenOffset     = 4
	valLenOffset     = 8
	deletedOffset    = 12
)

// record represents a single WAL entry: a key, optional value, and tombstone flag.
type record struct {
	key     []byte
	val     []byte
	deleted bool
}

// encode serializes the record with a leading CRC32 for atomicity (detect incomplete writes).
func (r *record) encode() ([]byte, error) {
	buf := make([]byte, recordHeaderSize)
	binary.LittleEndian.PutUint32(buf[keyLenOffset:], uint32(len(r.key)))
	binary.LittleEndian.PutUint32(buf[valLenOffset:], uint32(len(r.val)))
	if r.deleted {
		buf[deletedOffset] = 1
	}
	buf = append(buf, r.key...)
	if !r.deleted {
		buf = append(buf, r.val...)
	}
	checksum := crc32.ChecksumIEEE(buf[checksumSize:])
	binary.LittleEndian.PutUint32(buf[0:checksumSize], checksum)
	return buf, nil
}

// decode reads one record from rd. Returns ErrBadChecksum if the stored checksum does not match.
func (r *record) decode(rd io.Reader) error {
	var header [recordHeaderSize]byte
	if _, err := io.ReadFull(rd, header[:]); err != nil {
		return err
	}
	klen := int(binary.LittleEndian.Uint32(header[keyLenOffset:]))
	vlen := int(binary.LittleEndian.Uint32(header[valLenOffset:]))
	deleted := header[deletedOffset]

	data := make([]byte, klen+vlen)
	if _, err := io.ReadFull(rd, data); err != nil {
		return err
	}
	storedChecksum := binary.LittleEndian.Uint32(header[0:checksumSize])
	computed := crc32.ChecksumIEEE(append(header[checksumSize:], data...))
	if computed != storedChecksum {
		return ErrBadChecksum
	}
	r.key = data[:klen]
	r.deleted = deleted != 0
	if !r.deleted {
		r.val = data[klen:]
	} else {
		r.val = nil
	}
	return nil
}
