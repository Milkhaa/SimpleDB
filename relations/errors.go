package simpledb

import "errors"

var (
	// ErrTruncatedData is returned by Cell.Decode when the input buffer is too short.
	ErrTruncatedData = errors.New("simpledb: truncated data")
	// ErrInvalidPKey is returned by Schema.Validate when a PKey index is out of range.
	ErrInvalidPKey = errors.New("simpledb: invalid PKey index")
)
