package engine

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
)

// KV is a durable key-value store using an LSM-style layout.
//
// Data flow: Writes go to the WAL (for durability) and the in-memory sorted
// table (mem). Reads merge mem with on-disk SSTables (sstables) via the
// SortedKV abstraction; newest level wins for duplicate keys. Compaction
// flushes mem to a new SSTable when it exceeds logThreshold, and merges
// adjacent SSTables when growthFactor says so.
type KV struct {
	dir          string
	logThreshold int
	growthFactor float32

	log  *wal
	meta KVMetaStore

	mem *SortedArray // in-memory sorted table (MemTable)

	sstables []*SortedFile // on-disk SSTables, newest first
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
		sstables:     nil,
	}
	if err := kv.openAll(); err != nil {
		_ = kv.Close()
		return nil, err
	}
	return kv, nil
}

// openAll initializes storage and loads persisted state.
//
// MkdirAll does NOT delete existing data: if the directory already exists, it is left intact.
// Startup sequence:
// - Ensure db directory exists
// - Open metadata (create if missing) and read the latest meta slot
// - Open WAL (create if missing) and replay records into the MemTable
// - Open all SSTables listed in metadata (newest first)
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

	// Replay: only apply operations from fully committed transactions.
	// we read the entire WAL, track the last OpCommit boundary,
	// then apply only entries up to that boundary and truncate the WAL tail.
	if _, err := kv.log.file.Seek(0, 0); err != nil {
		kv.log.close()
		kv.meta.Close()
		return err
	}
	var entries []record
	committed := 0
	var lastCommittedOffset int64
	var readPos int64
	for {
		var rec record
		n, commitSeen, err := kv.log.readRecord(&rec)
		if err == io.EOF {
			break
		}
		if err != nil {
			kv.log.close()
			kv.meta.Close()
			return err
		}
		readPos += int64(n)
		if commitSeen {
			committed = len(entries)
			lastCommittedOffset = readPos
		} else {
			entries = append(entries, record{
				key: append([]byte(nil), rec.key...),
				val: append([]byte(nil), rec.val...),
				op:  rec.op,
			})
		}
	}

	kv.mem.Clear()
	for i := 0; i < committed; i++ {
		r := &entries[i]
		if r.op == OpDel {
			kv.mem.delOrTombstone(r.key)
		} else {
			kv.mem.set(r.key, r.val)
		}
	}

	kv.log.writer.committed = lastCommittedOffset
	kv.log.writer.offset = kv.log.writer.committed
	if readPos > kv.log.writer.committed {
		if err := kv.log.file.Truncate(kv.log.writer.committed); err != nil {
			kv.log.close()
			kv.meta.Close()
			return err
		}
	}
	meta := kv.meta.Get()
	for _, name := range meta.SSTableFiles {
		fpath := filepath.Join(kv.dir, name)
		sf := &SortedFile{FileName: fpath}
		if err := sf.Open(); err != nil {
			continue
		}
		kv.sstables = append(kv.sstables, sf)
	}

	return nil
}

// Close closes the store. Safe to call multiple times.
func (kv *KV) Close() error {
	for _, f := range kv.sstables {
		_ = f.Close()
	}
	kv.sstables = nil
	kv.meta.Close()
	if kv.log != nil {
		err := kv.log.close()
		kv.log = nil
		return err
	}
	return nil
}

// buildLevels returns all levels in merge order: mem first, then sstables (newest first).
func (kv *KV) buildLevels() MergedSortedKV {
	levels := make(MergedSortedKV, 0, 1+len(kv.sstables))
	levels = append(levels, kv.mem)
	for _, f := range kv.sstables {
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
// Implemented as a single-operation transaction that aborts without committing.
func (kv *KV) Get(key []byte) (value []byte, ok bool, err error) {

	tx := kv.NewTX()
	defer tx.Abort()
	return tx.Get(key)
}

// Set writes key-value. It returns updated=true if the key was new or the value changed.
// Implemented as a single-operation transaction; commits only when updated.
func (kv *KV) Set(key, value []byte) (updated bool, err error) {
	tx := kv.NewTX()
	updated, err = tx.Set(key, value)
	if err != nil {
		tx.Abort()
		return false, err
	}
	if updated {
		err = tx.Commit()
		return true, err
	}
	tx.Abort()
	return false, nil
}

// Del removes key. It returns updated=true if the key existed.
// Implemented as a single-operation transaction; commits only when the key existed.
func (kv *KV) Del(key []byte) (updated bool, err error) {
	tx := kv.NewTX()
	updated, err = tx.Del(key)
	if err != nil {
		tx.Abort()
		return false, err
	}
	if updated {
		err = tx.Commit()
		return true, err
	}
	tx.Abort()
	return false, nil
}

// Compact flushes the MemTable if it exceeds logThreshold, then merges adjacent
// SSTables when shouldMerge(i) is true (cur*growthFactor >= cur+next).
func (kv *KV) Compact() error {
	if kv.mem.Size() >= kv.logThreshold {
		if err := kv.compactLog(); err != nil {
			return err
		}
	}
	for i := 0; i < len(kv.sstables)-1; i++ {
		if kv.shouldMerge(i) {
			if err := kv.compactSSTable(i); err != nil {
				return err
			}
			i-- // re-check index i after merging (two levels became one)
			continue
		}
	}
	return nil
}

func (kv *KV) shouldMerge(idx int) bool {
	cur := kv.sstables[idx].EstimatedSize()
	next := kv.sstables[idx+1].EstimatedSize()
	return float32(cur)*kv.growthFactor >= float32(cur+next)
}

// createSSTableFromSource writes source to a new SSTable file and opens it. Removes the file on error.
func (kv *KV) createSSTableFromSource(name string, source SortedKV) (*SortedFile, error) {
	fpath := filepath.Join(kv.dir, name)
	sf := &SortedFile{FileName: fpath}
	if err := sf.CreateFromSorted(source); err != nil {
		os.Remove(fpath)
		return nil, err
	}
	if err := sf.Open(); err != nil {
		os.Remove(fpath)
		return nil, err
	}
	return sf, nil
}

// compactLog flushes the MemTable to a new SSTable, clears mem, and resets the WAL.
func (kv *KV) compactLog() error {
	meta := kv.meta.Get()
	meta.Version++
	name := fmt.Sprintf("sstable_%d", meta.Version)
	source := SortedKV(kv.mem)
	if len(kv.sstables) == 0 {
		source = NoDeletedSortedKV{source}
	}
	sf, err := kv.createSSTableFromSource(name, source)
	if err != nil {
		return err
	}
	meta.SSTableFiles = append([]string{name}, meta.SSTableFiles...)
	if err := kv.meta.Set(meta); err != nil {
		sf.Close()
		return err
	}
	kv.sstables = append([]*SortedFile{sf}, kv.sstables...)
	kv.mem.Clear()
	return kv.log.reset()
}

// compactSSTable merges sstables[level] and sstables[level+1] into one SSTable and replaces them.
func (kv *KV) compactSSTable(level int) error {
	meta := kv.meta.Get()
	meta.Version++
	name := fmt.Sprintf("sstable_%d", meta.Version)
	merged := MergedSortedKV{kv.sstables[level], kv.sstables[level+1]}
	source := SortedKV(merged)
	if len(kv.sstables) == level+2 {
		source = NoDeletedSortedKV{source}
	}
	sf, err := kv.createSSTableFromSource(name, source)
	if err != nil {
		return err
	}
	meta.SSTableFiles = slices.Replace(meta.SSTableFiles, level, level+2, name)
	if err := kv.meta.Set(meta); err != nil {
		sf.Close()
		return err
	}
	old1, old2 := kv.sstables[level], kv.sstables[level+1]
	kv.sstables = slices.Replace(kv.sstables, level, level+2, sf)
	old1.Close()
	old2.Close()
	os.Remove(old1.FileName)
	os.Remove(old2.FileName)
	return nil
}
