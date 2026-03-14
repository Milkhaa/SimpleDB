// Package simpledb provides a durable key-value store backed by a write-ahead log (WAL).
//
// Open the database with Open, then use Get, Set, and Del on the returned Store.
// All mutations are appended to the WAL and synced before returning. On Open,
// the WAL is replayed to restore in-memory state; incomplete or corrupted
// tail records are skipped (checksum-based atomicity).
//
// Example:
//
//	db, err := simpledb.Open(simpledb.Config{Path: "mydb"})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	db.Set([]byte("key"), []byte("value"))
//	val, ok, _ := db.Get([]byte("key"))
package simpledb
