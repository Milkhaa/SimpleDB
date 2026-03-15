package simpledb

// Default configuration values when fields are zero or invalid.
const (
	DefaultPath         = ".simpledb"
	DefaultLogThreshold = 1000
	DefaultGrowthFactor = 2.0
)

// Config holds options for opening the database.
type Config struct {
	// Path is the database directory (WAL and SSTables live under it).
	// If empty, DefaultPath is used.
	Path string

	// LogThreshold is the max keys in the MemTable before flushing to an SSTable.
	// If <= 0, DefaultLogThreshold is used.
	LogThreshold int

	// GrowthFactor controls when adjacent SSTables are merged: merge main[i] and main[i+1]
	// when cur*GrowthFactor >= cur+next. If < 2.0, DefaultGrowthFactor is used.
	GrowthFactor float32
}

// applyDefaults fills zero values with defaults. Call once before using Config.
func (c *Config) applyDefaults() {
	if c.Path == "" {
		c.Path = DefaultPath
	}
	if c.LogThreshold <= 0 {
		c.LogThreshold = DefaultLogThreshold
	}
	if c.GrowthFactor < 2.0 {
		c.GrowthFactor = DefaultGrowthFactor
	}
}
