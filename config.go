package simpledb

// Config holds options for opening the database.
type Config struct {
	// Path is the database directory (WAL and SSTables live under it).
	Path string

	// LogThreshold is the max keys in the MemTable before flushing to an SSTable.
	// If <= 0, defaults to 1000.
	LogThreshold int

	// GrowthFactor controls when adjacent SSTables are merged: merge main[i] and main[i+1]
	// when cur*GrowthFactor >= cur+next. If < 2.0, defaults to 2.0.
	GrowthFactor float32
}
