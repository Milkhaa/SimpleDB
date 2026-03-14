package simpledb

// Config holds options for opening the database.
type Config struct {
	// Path is the file path for the write-ahead log (WAL).
	// The database state is recovered by replaying this file on Open.
	Path string
}
