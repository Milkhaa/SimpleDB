package engine

import "errors"

// ErrBadChecksum indicates a WAL record failed checksum validation (e.g. incomplete or corrupted write).
var ErrBadChecksum = errors.New("engine: bad checksum")
