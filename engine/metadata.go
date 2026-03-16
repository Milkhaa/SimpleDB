package engine

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// KVMetaData holds LSM metadata: version and list of SSTable filenames.
type KVMetaData struct {
	Version  uint64
	SSTables []string
}

// metaSlot holds an open meta file and its cached data.
type metaSlot struct {
	path string
	fp   *os.File
	data KVMetaData
}

// KVMetaStore persists metadata with double-buffering (two slots, alternate writes).
// Slot data is cached after Open and after Set so Get/current do not read from disk.
type KVMetaStore struct {
	slots [2]metaSlot
}

func (m *KVMetaStore) Open(dir string) error {
	m.slots[0].path = filepath.Join(dir, "meta0")
	m.slots[1].path = filepath.Join(dir, "meta1")
	for i := 0; i < 2; i++ {
		fp, data, err := m.openMetafile(m.slots[i].path)
		if err != nil {
			m.Close()
			return err
		}
		m.slots[i].fp = fp
		m.slots[i].data = data
	}
	return nil
}

// openMetafile opens the meta file (or creates it durably) and reads current data into cache.
func (m *KVMetaStore) openMetafile(path string) (*os.File, KVMetaData, error) {
	fp, err := openFileSync(path)
	if err != nil {
		return nil, KVMetaData{}, err
	}
	data, err := readMetaFile(fp)
	if err != nil {
		fp.Close()
		return nil, KVMetaData{}, err
	}
	return fp, data, nil
}

func readMetaFile(fp *os.File) (KVMetaData, error) {
	b, err := io.ReadAll(fp)
	if err != nil {
		return KVMetaData{}, err
	}
	if len(b) <= 8 {
		return KVMetaData{}, nil
	}
	sum := binary.LittleEndian.Uint32(b[0:4])
	size := binary.LittleEndian.Uint32(b[4:8])
	if len(b) < 8+int(size) {
		return KVMetaData{}, nil
	}
	if sum != crc32.ChecksumIEEE(b[4:8+size]) {
		return KVMetaData{}, nil
	}
	var d KVMetaData
	if err := json.Unmarshal(b[8:8+size], &d); err != nil {
		return KVMetaData{}, nil
	}
	return d, nil
}

func writeMetaFile(fp *os.File, d KVMetaData) error {
	payload, err := json.Marshal(d)
	if err != nil {
		return err
	}
	// Layout: crc32(4) | size(4) | json
	buf := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(payload)))
	copy(buf[8:], payload)
	binary.LittleEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[4:]))
	if _, err := fp.WriteAt(buf, 0); err != nil {
		return err
	}
	return fp.Sync()
}

func (m *KVMetaStore) Close() error {
	var err error
	for i := 0; i < 2; i++ {
		if m.slots[i].fp != nil {
			if e := m.slots[i].fp.Close(); e != nil {
				err = e
			}
			m.slots[i].fp = nil
		}
	}
	return err
}

func (m *KVMetaStore) current() int {
	if m.slots[0].data.Version > m.slots[1].data.Version {
		return 0
	}
	return 1
}

// Get returns the metadata from the slot with the higher version (uses cached data).
func (m *KVMetaStore) Get() KVMetaData {
	return m.slots[m.current()].data
}

// Set writes to the alternate slot and updates the cache.
func (m *KVMetaStore) Set(d KVMetaData) error {
	slot := 1 - m.current()
	if err := writeMetaFile(m.slots[slot].fp, d); err != nil {
		return err
	}
	m.slots[slot].data = d
	return nil
}
