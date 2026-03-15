package simpledb

import (
	"io"
	"os"
	"path"
	"syscall"
)

// wal is an append-only write-ahead log. Each record is checksummed for atomicity;
// on replay, incomplete or corrupted records are skipped (see record.decode).
type wal struct {
	path string
	file *os.File
}

// open creates or opens the WAL file at path and syncs the containing directory
// so the file is durable (on Unix, directory fsync is required for file creation).
func (w *wal) open(path string) error {
	var err error
	w.path = path
	w.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	// Ensure directory entry is durable (Unix: file data alone is not enough).
	if err := syncDir(path); err != nil {
		_ = w.file.Close()
		return err
	}
	return nil
}

func (w *wal) close() error {
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

// append encodes rec and appends it to the log, then syncs for durability.
func (w *wal) append(rec *record) error {
	data, err := rec.encode()
	if err != nil {
		return err
	}
	if _, err := w.file.Write(data); err != nil {
		return err
	}
	return w.file.Sync()
}

// reset truncates the WAL (used after LSM flush). Caller must ensure no concurrent use.
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
	return w.file.Sync()
}

// read decodes the next record from the log into rec.
// Returns (true, nil) when no more records (EOF, truncated, or bad checksum).
func (w *wal) read(rec *record) (done bool, err error) {
	err = rec.decode(w.file)
	if err == io.EOF || err == io.ErrUnexpectedEOF || err == ErrBadChecksum {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

// createFileSync creates a new file at path (overwrites if exists) and syncs the directory.
// Use for new files only (e.g. SSTable). Caller must close the file.
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

// openFileSync opens a file for read-write, creating it if it does not exist (does not truncate).
// Syncs the directory so new file creation is durable. Use for meta files.
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

// syncDir fsyncs the directory containing filePath so that file creation/rename/delete
// are durable. On Linux, fsync on the file alone does not guarantee the directory
// entry is persisted. Unix-specific; Windows does not require this.
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
