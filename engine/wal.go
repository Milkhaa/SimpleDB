package engine

import (
	"io"
	"os"
	"path"
	"syscall"
)

type wal struct {
	path string
	file *os.File
}

func (w *wal) open(path string) error {
	var err error
	w.path = path
	w.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
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
