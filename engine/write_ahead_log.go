package engine

import (
	"io"
	"os"
	"path"
	"syscall"
)

// wal is the write-ahead log. Writes use WriteAt at writer.offset; only advance offset on success.
// writer.committed is the offset after the last OpCommit. ResetTX() rolls back to committed.
type wal struct {
	path   string
	file   *os.File
	writer struct {
		offset    int64
		committed int64
	}
}

func (w *wal) open(path string) error {
	w.path = path
	fp, err := openFileSync(path)
	if err != nil {
		return err
	}
	w.file = fp
	w.writer.offset = 0
	w.writer.committed = 0
	return nil
}

func (w *wal) close() error {
	if w.file == nil {
		return nil
	}
	_ = w.file.Sync()
	err := w.file.Close()
	w.file = nil
	return err
}

// Write appends one record at the current writer offset. Uses WriteAt so on error the offset is unchanged.
func (w *wal) Write(rec *record) error {
	data, err := rec.encode()
	if err != nil {
		return err
	}
	n, err := w.file.WriteAt(data, w.writer.offset)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	w.writer.offset += int64(n)
	return nil
}

// Commit writes an OpCommit record, syncs, and advances the committed offset.
func (w *wal) Commit() error {
	if err := w.Write(&record{op: OpCommit}); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	w.writer.committed = w.writer.offset
	return nil
}

// ResetTX discards the current transaction by resetting the write offset to the last committed offset.
func (w *wal) ResetTX() {
	w.writer.offset = w.writer.committed
}

// readRecord reads one record from the file at the current read position.
// Returns (bytesRead, commitSeen, err). Caller must advance read position by bytesRead.
func (w *wal) readRecord(rec *record) (bytesRead int, commitSeen bool, err error) {
	n, commit, e := rec.decodeFrom(w.file)
	if e == io.EOF || e == io.ErrUnexpectedEOF {
		return n, false, io.EOF
	}
	if e != nil {
		return n, false, e
	}
	return n, commit, nil
}

func (w *wal) reset() error {
	if w.file == nil {
		return nil
	}
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	w.writer.offset = 0
	w.writer.committed = 0
	return w.file.Sync()
}

// createFileSync creates a new file (truncates if it exists) and syncs the directory.
// Use for writing new files from scratch (e.g. a new SSTable). On dir-sync failure, removes the file.
func createFileSync(filePath string) (*os.File, error) {
	f, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	if err := syncDir(filePath); err != nil {
		f.Close()
		os.Remove(filePath)
		return nil, err
	}
	return f, nil
}

// openFileSync opens a file for read/write, creating it only if it does not exist (no truncation).
// Use when the file may already exist and must be preserved (e.g. WAL, metadata). Syncs the directory so the new file survives crash.
func openFileSync(filePath string) (*os.File, error) {
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syncDir(filePath); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func syncDir(filePath string) error {
	dir := path.Dir(filePath)
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	fd, err := syscall.Open(dir, flags, 0)
	if err != nil {
		return err
	}
	defer syscall.Close(fd)
	return syscall.Fsync(fd)
}
