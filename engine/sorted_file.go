package engine

import (
	"bytes"
	"encoding/binary"
	"os"
)

const sortedFileEntryHeader = 4 + 4 + 1 // keyLen, valLen, deleted

// SortedFile is an immutable on-disk sorted key-value file (SSTable).
// Layout: 8-byte key count, N×8-byte offsets, then N entries (each 4+4+1 + key + val).
type SortedFile struct {
	FileName string
	fp       *os.File
	nkeys    int
	offsets  []uint64
}

// Close closes the file. Safe to call multiple times.
func (f *SortedFile) Close() error {
	if f.fp == nil {
		return nil
	}
	err := f.fp.Close()
	f.fp = nil
	return err
}

// Open opens the file for reading and loads the index (key count and offsets).
func (f *SortedFile) Open() error {
	if f.fp != nil {
		return nil
	}
	fp, err := os.Open(f.FileName)
	if err != nil {
		return err
	}
	buf := make([]byte, 8)
	if _, err := fp.ReadAt(buf, 0); err != nil {
		fp.Close()
		return err
	}
	n := int(binary.LittleEndian.Uint64(buf))
	offsets := make([]uint64, n)
	for i := 0; i < n; i++ {
		if _, err := fp.ReadAt(buf, 8+int64(i)*8); err != nil {
			fp.Close()
			return err
		}
		offsets[i] = binary.LittleEndian.Uint64(buf)
	}
	f.fp = fp
	f.nkeys = n
	f.offsets = offsets
	return nil
}

func (f *SortedFile) EstimatedSize() int { return f.nkeys }

// readEntry reads the key, value, and deleted flag for the entry at pos.
func (f *SortedFile) readEntry(pos int) (key, val []byte, deleted bool, err error) {
	if pos < 0 || pos >= f.nkeys {
		return nil, nil, false, nil
	}
	off := f.offsets[pos]
	h := make([]byte, sortedFileEntryHeader)
	if _, err := f.fp.ReadAt(h, int64(off)); err != nil {
		return nil, nil, false, err
	}
	klen := binary.LittleEndian.Uint32(h[0:4])
	vlen := binary.LittleEndian.Uint32(h[4:8])
	deleted = h[8] != 0
	kv := make([]byte, klen+vlen)
	if _, err := f.fp.ReadAt(kv, int64(off)+sortedFileEntryHeader); err != nil {
		return nil, nil, false, err
	}
	return kv[:klen], kv[klen:], deleted, nil
}

func (f *SortedFile) Iter() (SortedKVIter, error) {
	if f.fp == nil {
		if err := f.Open(); err != nil {
			return nil, err
		}
	}
	it := &sortedFileIter{file: f, pos: 0}
	if f.nkeys > 0 {
		k, v, d, err := f.readEntry(0)
		if err != nil {
			return nil, err
		}
		it.key, it.val, it.deleted = k, v, d
	} else {
		it.pos = -1
	}
	return it, nil
}

func (f *SortedFile) Seek(key []byte) (SortedKVIter, error) {
	if f.fp == nil {
		if err := f.Open(); err != nil {
			return nil, err
		}
	}
	pos := f.findPos(key)
	it := &sortedFileIter{file: f, pos: pos}
	if pos >= 0 && pos < f.nkeys {
		k, v, d, err := f.readEntry(pos)
		if err != nil {
			return nil, err
		}
		it.key, it.val, it.deleted = k, v, d
	} else {
		it.pos = -1
	}
	return it, nil
}

func (f *SortedFile) findPos(target []byte) int {
	lo, hi := 0, f.nkeys
	for lo < hi {
		mid := lo + (hi-lo)/2
		key, _, _, err := f.readEntry(mid)
		if err != nil {
			return -1
		}
		r := bytes.Compare(target, key)
		if r > 0 {
			lo = mid + 1
		} else if r < 0 {
			hi = mid
		} else {
			return mid
		}
	}
	return lo
}

type sortedFileIter struct {
	file    *SortedFile
	pos     int
	key     []byte
	val     []byte
	deleted bool
}

func (it *sortedFileIter) Valid() bool {
	return it.pos >= 0 && it.pos < it.file.nkeys
}
func (it *sortedFileIter) Key() []byte   { return it.key }
func (it *sortedFileIter) Val() []byte   { return it.val }
func (it *sortedFileIter) Deleted() bool { return it.deleted }

func (it *sortedFileIter) Next() error {
	it.pos++
	if it.pos >= it.file.nkeys {
		it.key, it.val, it.deleted = nil, nil, false
		return nil
	}
	k, v, d, err := it.file.readEntry(it.pos)
	if err != nil {
		return err
	}
	it.key, it.val, it.deleted = k, v, d
	return nil
}

func (it *sortedFileIter) Prev() error {
	it.pos--
	if it.pos < 0 {
		it.key, it.val, it.deleted = nil, nil, false
		return nil
	}
	k, v, d, err := it.file.readEntry(it.pos)
	if err != nil {
		return err
	}
	it.key, it.val, it.deleted = k, v, d
	return nil
}

// CreateFromSorted writes a SortedKV to a new file.
func (f *SortedFile) CreateFromSorted(kv SortedKV) error {
	fp, err := createFileSync(f.FileName)
	if err != nil {
		return err
	}
	defer fp.Close()

	n := kv.EstimatedSize()
	headerSize := 8 + n*8
	offsets := make([]uint64, 0, n+1)
	offsets = append(offsets, uint64(headerSize))
	iter, err := kv.Iter()
	if err != nil {
		return err
	}
	for iter.Valid() {
		kl, vl := len(iter.Key()), len(iter.Val())
		next := offsets[len(offsets)-1] + sortedFileEntryHeader + uint64(kl) + uint64(vl)
		offsets = append(offsets, next)
		if err := iter.Next(); err != nil {
			return err
		}
	}
	// Write header: count then n offsets
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(len(offsets)-1))
	if _, err := fp.Write(buf); err != nil {
		return err
	}
	for i := 0; i < len(offsets)-1; i++ {
		binary.LittleEndian.PutUint64(buf, offsets[i])
		if _, err := fp.Write(buf); err != nil {
			return err
		}
	}
	// Write entries
	iter, _ = kv.Iter()
	for iter.Valid() {
		k, v := iter.Key(), iter.Val()
		h := make([]byte, sortedFileEntryHeader)
		binary.LittleEndian.PutUint32(h[0:4], uint32(len(k)))
		binary.LittleEndian.PutUint32(h[4:8], uint32(len(v)))
		if iter.Deleted() {
			h[8] = 1
		}
		if _, err := fp.Write(h); err != nil {
			return err
		}
		if _, err := fp.Write(k); err != nil {
			return err
		}
		if _, err := fp.Write(v); err != nil {
			return err
		}
		if err := iter.Next(); err != nil {
			return err
		}
	}
	if err := fp.Sync(); err != nil {
		return err
	}
	f.nkeys = len(offsets) - 1
	f.offsets = append([]uint64(nil), offsets...)
	return nil
}
