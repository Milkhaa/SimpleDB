package simpledb

import (
	"encoding/binary"
	"io"
	"os"
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
			1. First 4 bytes: length of key bytes,integer - little endian format
			2. Next 4 bytes : length of value bytes, integer - little endian format
			3. Next 1 byte  : to denote whether this is a DEL operation
			4. Next         : key bytes
			5. Next         : value bytes
	*/
	encodedBytes := make([]byte, 4+4+1)
	binary.LittleEndian.PutUint32(encodedBytes[0:4], uint32(len(entry.key))) //#1

	encodedBytes = append(encodedBytes, entry.key...) //#4
	if entry.deleted {
		encodedBytes[8] = 1 //3

	} else {
		binary.LittleEndian.PutUint32(encodedBytes[4:8], uint32(len(entry.val))) //#2
		encodedBytes = append(encodedBytes, entry.val...)                        //#5
	}

	return encodedBytes, nil

}

func (entry *WalEntry) Decode(reader io.Reader) error {
	var header [9]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return err
	}

	keyLength := int(binary.LittleEndian.Uint32(header[0:4]))
	valueLength := int(binary.LittleEndian.Uint32(header[4:8]))
	deleted := header[8]

	mainBytes := make([]byte, keyLength+valueLength)
	if _, err := io.ReadFull(reader, mainBytes); err != nil {
		return err
	}

	entry.key = mainBytes[0:keyLength]
	if deleted == 1 {
		entry.deleted = true
		return nil
	}

	entry.val = mainBytes[keyLength : keyLength+valueLength]
	return nil

}

// Operations available on disk

func (wal *WAL) Open() error {
	var err error
	wal.filePointer, err = os.OpenFile(wal.FileName, os.O_RDWR|os.O_CREATE, 0o644)
	return err
}

func (wal *WAL) Close() error {
	return wal.filePointer.Close()
}

func (wal *WAL) Write(entry *WalEntry) error {
	encodedBytes, err := entry.Encode()
	if err != nil {
		return err
	}

	_, err = wal.filePointer.Write(encodedBytes)
	return err
}

func (wal *WAL) Read(entry *WalEntry) (bool, error) {
	err := entry.Decode(wal.filePointer)
	if err == io.EOF {
		return true, nil
	} else if err != nil {
		return false, err
	} else {
		return false, nil
	}
}
