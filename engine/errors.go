package engine

import "errors"

// ErrBadChecksum indicates a WAL record failed checksum validation (e.g. incomplete or corrupted write).
var ErrBadChecksum = errors.New("engine: bad checksum")

// ErrCorruptSSTable indicates an SSTable file has invalid header or entry (e.g. count or length out of bounds).
var ErrCorruptSSTable = errors.New("engine: corrupt sstable")
