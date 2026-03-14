package simpledb

import "errors"

// Sentinel errors for database operations.
var (
	// ErrBadChecksum indicates a WAL record failed checksum validation (e.g. incomplete or corrupted write).
	ErrBadChecksum = errors.New("simpledb: bad checksum")
)
