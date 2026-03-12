package simpledb

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path"
	"syscall"
)

type WAL struct {
	FileName    string
	filePointer *os.File
}

// Data serialization before writing to disk
type WalEntry struct {
	key     []byte
	val     []byte
	deleted bool
}

func (entry *WalEntry) Encode() ([]byte, error) {
	/*
		Binary Encoding

		Format:
			1. First 4 bytes: CheckSum bytes
			2. First 4 bytes: length of key bytes,integer - little endian format
			3. Next 4 bytes : length of value bytes, integer - little endian format
			4. Next 1 byte  : to denote whether this is a DEL operation
			5. Next         : key bytes
			6. Next         : value bytes
	*/
	encodedBytes := make([]byte, 4+4+4+1)
	binary.LittleEndian.PutUint32(encodedBytes[4:8], uint32(len(entry.key))) //#2

	encodedBytes = append(encodedBytes, entry.key...) //#5
	if entry.deleted {
		encodedBytes[12] = 1 //#4

	} else {
		binary.LittleEndian.PutUint32(encodedBytes[8:12], uint32(len(entry.val))) //#3
		encodedBytes = append(encodedBytes, entry.val...)                         //#6
	}

	/*
		ATOMICITY:
			When appending a record to the log, we want it to either be completely
			written or not written at all. This can be called atomicity

			Incomplete writes can be detected using checksum, and discarded - at the time of decode.

	*/

	//generate checksum
	checksum := crc32.ChecksumIEEE(encodedBytes[4:])
	binary.LittleEndian.PutUint32(encodedBytes[0:4], checksum) // #1

	return encodedBytes, nil

}

var ErrBadSum = errors.New("bad checksum")

func (ent *WalEntry) Decode(r io.Reader) error {
	var header [13]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}

	klen := int(binary.LittleEndian.Uint32(header[4:8]))
	vlen := int(binary.LittleEndian.Uint32(header[8:12]))
	deleted := header[12]

	data := make([]byte, klen+vlen)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}

	//calculate and match checksum
	checksum := binary.LittleEndian.Uint32(header[0:4])

	calcChecksum := crc32.ChecksumIEEE(append(header[4:], data...))
	if calcChecksum != checksum {
		return ErrBadSum
	}

	ent.key = data[:klen]
	if deleted != 0 {
		ent.deleted = true
	} else {
		ent.deleted = false
		ent.val = data[klen:]
	}

	return nil

}

// Operations available on disk

func (wal *WAL) Open() error {
	var err error
	wal.filePointer, err = os.OpenFile(wal.FileName, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}

	if err = syncDir(path.Base(wal.FileName)); err != nil {
		_ = wal.filePointer.Close()
		return err
	}

	return nil
}

/*
DURABILITY

	On Linux, fsync ensures file data is written, but does not ensure the file itself exists.
	This is because a file is recorded by its parent directory. Hence Directory sync is required.
*/
func syncDir(file string) error {
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirfd, err := syscall.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return err
	}
	defer syscall.Close(dirfd)
	return syscall.Fsync(dirfd)
}

func (wal *WAL) Close() error {
	return wal.filePointer.Close()
}

func (wal *WAL) Write(entry *WalEntry) error {
	encodedBytes, err := entry.Encode()
	if err != nil {
		return err
	}

	if _, err = wal.filePointer.Write(encodedBytes); err != nil {
		return err
	}

	/*
		DURABILITY:
		This fSync + Directory sync ensures durability of every WAL write


	*/
	return wal.filePointer.Sync()
}

func (wal *WAL) Read(entry *WalEntry) (bool, error) {
	err := entry.Decode(wal.filePointer)
	if err == io.EOF || err == io.ErrUnexpectedEOF || err == ErrBadSum {
		return true, nil
	} else if err != nil {
		return false, err
	} else {
		return false, nil
	}
}
