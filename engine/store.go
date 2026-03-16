package engine

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
)

// KV is a durable key-value store using an LSM layout (like 0704): write-ahead log,
// in-memory sorted MemTable (keys/vals arrays in SortedArray), and immutable SSTables.
// There is no separate "map mode"; the in-memory state is always sorted key/value arrays.
type KV struct {
	// options
	dir          string
	logThreshold int
	growthFactor float32

	// persistence
	log  *wal
	meta KVMetaStore

	// in-memory sorted keys/vals (and tombstones)
	mem *SortedArray

	// on-disk levels (newest first)
	main []*SortedFile
}

// Open opens or creates the database. Path is always a directory; WAL and SSTables live under it.
// Zero or invalid Config fields are replaced with defaults (see config.go).
func Open(cfg Config) (*KV, error) {
	cfg.applyDefaults()
	kv := &KV{
		dir:          cfg.Path,
		logThreshold: cfg.LogThreshold,
		growthFactor: cfg.GrowthFactor,
		mem:          &SortedArray{},
		main:         nil,
	}
	if err := kv.openAll(); err != nil {
		_ = kv.Close()
		return nil, err
	}
	return kv, nil
}

func (kv *KV) openAll() error {
	if err := os.MkdirAll(kv.dir, 0o755); err != nil {
		return err
	}
	if err := kv.meta.Open(kv.dir); err != nil {
		return err
	}
	walPath := filepath.Join(kv.dir, "wal")
	kv.log = &wal{}
	if err := kv.log.open(walPath); err != nil {
		kv.meta.Close()
		return err
	}
	// Replay WAL into MemTable (sorted; last write per key wins)
	var rec record
	for {
		done, err := kv.log.read(&rec)
		if err != nil {
			kv.log.close()
			kv.meta.Close()
			return err
		}
		if done {
			break
		}
		if rec.deleted {
			kv.mem.delOrTombstone(rec.key)
		} else {
			kv.mem.set(rec.key, rec.val)
		}
	}
	// Open existing SSTables
	meta := kv.meta.Get()
	for _, name := range meta.SSTables {
		fpath := filepath.Join(kv.dir, name)
		sf := &SortedFile{FileName: fpath}
		if err := sf.Open(); err != nil {
			continue
		}
		kv.main = append(kv.main, sf)
	}
	return nil
}

// Close closes the store. Safe to call multiple times.
func (kv *KV) Close() error {
	for _, f := range kv.main {
		_ = f.Close()
	}
	kv.main = nil
	kv.meta.Close()
	if kv.log != nil {
		err := kv.log.close()
		kv.log = nil
		return err
	}
	return nil
}

// buildLevels returns all levels in merge order: mem first, then main (newest first).
// Callers use this for Seek/Iter over the logical merged view.
func (kv *KV) buildLevels() MergedSortedKV {
	levels := make(MergedSortedKV, 0, 1+len(kv.main))
	levels = append(levels, kv.mem)
	for _, f := range kv.main {
		levels = append(levels, f)
	}
	return levels
}

// Seek returns an iterator at the first entry with key >= key, skipping deleted entries.
func (kv *KV) Seek(key []byte) (SortedKVIter, error) {
	levels := kv.buildLevels()
	iter, err := levels.Seek(key)
	if err != nil {
		return nil, err
	}
	return filterDeleted(iter)
}

// Get returns the value for key. ok is false if the key is missing or was deleted.
func (kv *KV) Get(key []byte) (value []byte, ok bool, err error) {
	iter, err := kv.Seek(key)
	if err != nil {
		return nil, false, err
	}
	ok = iter.Valid() && bytes.Equal(iter.Key(), key)
	if ok {
		value = iter.Val()
	}
	return value, ok, err
}

// Set writes key-value. It returns updated=true if the key was new or the value changed.
func (kv *KV) Set(key, value []byte) (updated bool, err error) {
	oldVal, exist, err := kv.Get(key)
	if err != nil {
		return false, err
	}
	updated = !exist || !bytes.Equal(oldVal, value)
	if !updated {
		return false, nil
	}
	if err := kv.log.append(&record{key: key, val: value, deleted: false}); err != nil {
		return true, err
	}
	kv.mem.Set(key, value)
	if err := kv.Compact(); err != nil {
		return true, err
	}
	return true, nil
}

// Del removes key. It returns updated=true if the key existed.
func (kv *KV) Del(key []byte) (updated bool, err error) {
	_, exist, err := kv.Get(key)
	if err != nil || !exist {
		return false, err
	}
	if err := kv.log.append(&record{key: key, deleted: true}); err != nil {
		return true, err
	}
	kv.mem.Del(key)
	return true, nil
}

// Compact flushes the MemTable if it exceeds LogThreshold, then merges adjacent
// SSTables when shouldMerge(i) is true (cur*growthFactor >= cur+next).
func (kv *KV) Compact() error {
	if kv.mem.Size() >= kv.logThreshold {
		if err := kv.compactLog(); err != nil {
			return err
		}
	}
	for i := 0; i < len(kv.main)-1; i++ {
		if kv.shouldMerge(i) {
			if err := kv.compactSSTable(i); err != nil {
				return err
			}
			i--
			continue
		}
	}
	return nil
}

func (kv *KV) shouldMerge(idx int) bool {
	cur := kv.main[idx].EstimatedSize()
	next := kv.main[idx+1].EstimatedSize()
	return float32(cur)*kv.growthFactor >= float32(cur+next)
}

// compactLog flushes the MemTable to a new SSTable, clears mem, and resets the WAL.
func (kv *KV) compactLog() error {
	meta := kv.meta.Get()
	meta.Version++
	name := fmt.Sprintf("sstable_%d", meta.Version)
	fpath := filepath.Join(kv.dir, name)
	sf := &SortedFile{FileName: fpath}
	var source SortedKV = kv.mem
	if len(kv.main) == 0 {
		source = NoDeletedSortedKV{source}
	}
	if err := sf.CreateFromSorted(source); err != nil {
		os.Remove(fpath)
		return err
	}
	if err := sf.Open(); err != nil {
		os.Remove(fpath)
		return err
	}
	meta.SSTables = append([]string{name}, meta.SSTables...)
	if err := kv.meta.Set(meta); err != nil {
		sf.Close()
		return err
	}
	kv.main = append([]*SortedFile{sf}, kv.main...)
	kv.mem.Clear()
	return kv.log.reset()
}

// compactSSTable merges main[level] and main[level+1] into one SSTable and replaces them.
func (kv *KV) compactSSTable(level int) error {
	meta := kv.meta.Get()
	meta.Version++
	name := fmt.Sprintf("sstable_%d", meta.Version)
	fpath := filepath.Join(kv.dir, name)
	sf := &SortedFile{FileName: fpath}
	merged := MergedSortedKV{kv.main[level], kv.main[level+1]}
	var source SortedKV = merged
	if len(kv.main) == level+2 {
		source = NoDeletedSortedKV{source}
	}
	if err := sf.CreateFromSorted(source); err != nil {
		os.Remove(fpath)
		return err
	}
	if err := sf.Open(); err != nil {
		os.Remove(fpath)
		return err
	}
	meta.SSTables = slices.Replace(meta.SSTables, level, level+2, name)
	if err := kv.meta.Set(meta); err != nil {
		sf.Close()
		return err
	}
	old1, old2 := kv.main[level], kv.main[level+1]
	kv.main = slices.Replace(kv.main, level, level+2, sf)
	old1.Close()
	old2.Close()
	os.Remove(old1.FileName)
	os.Remove(old2.FileName)
	return nil
}
