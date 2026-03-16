package engine

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

// Log record layout: crc32(4) | keyLen(4) | valLen(4) | deleted(1) | key | val
const (
	recordHeaderSize = 4 + 4 + 4 + 1
	checksumSize     = 4
	keyLenOffset     = 4
	valLenOffset     = 8
	deletedOffset    = 12
)

type record struct {
	key     []byte
	val     []byte
	deleted bool
}

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
