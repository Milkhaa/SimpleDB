package simpledb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableByPKey(t *testing.T) {
	const path = ".test_db"
	defer os.Remove(path)
	os.Remove(path)

	db := &DB{}
	err := db.Open(path)
	assert.Nil(t, err)
	defer db.Close()

	schema := &Schema{
		Table: "event",
		Cols: []Column{
			{Name: "ts", Type: CellTypeI64},
			{Name: "kind", Type: CellTypeStr},
			{Name: "payload", Type: CellTypeStr},
		},
		PKey: []int{0, 1}, // (ts, kind)
	}

	row := Row{
		Cell{Type: CellTypeI64, I64: 1000},
		Cell{Type: CellTypeStr, Str: []byte("click")},
		Cell{Type: CellTypeStr, Str: []byte("button_a")},
	}
	ok, err := db.Select(schema, row)
	assert.True(t, !ok && err == nil)

	updated, err := db.Insert(schema, row)
	assert.True(t, updated && err == nil)

	out := Row{
		Cell{Type: CellTypeI64, I64: 1000},
		Cell{Type: CellTypeStr, Str: []byte("click")},
		Cell{Type: CellTypeStr}, // payload filled by Select
	}
	ok, err = db.Select(schema, out)
	assert.True(t, ok && err == nil)
	assert.Equal(t, row, out)

	row[2].Str = []byte("button_b")
	updated, err = db.Update(schema, row)
	assert.True(t, updated && err == nil)

	ok, err = db.Select(schema, out)
	assert.True(t, ok && err == nil)
	assert.Equal(t, row, out)

	deleted, err := db.Delete(schema, row)
	assert.True(t, deleted && err == nil)

	ok, err = db.Select(schema, row)
	assert.True(t, !ok && err == nil)
}

func mustParseStmt(t *testing.T, s string) interface{} {
	stmt, err := ParseStmt(s)
	assert.Nil(t, err)
	return stmt
}

func TestSQLByPKey(t *testing.T) {
	const path = ".test_db"
	defer os.Remove(path)
	os.Remove(path)

	db := DB{}
	err := db.Open(path)
	assert.Nil(t, err)
	defer db.Close()

	s := "create table link (time int64, src string, dst string, primary key (src, dst));"
	_, err = db.ExecStmt(mustParseStmt(t, s))
	assert.Nil(t, err)

	s = "insert into link values (123, 'bob', 'alice');"
	r, err := db.ExecStmt(mustParseStmt(t, s))
	assert.Nil(t, err)
	assert.Equal(t, 1, r.Updated)

	s = "select time from link where dst = 'alice' and src = 'bob';"
	r, err = db.ExecStmt(mustParseStmt(t, s))
	assert.Nil(t, err)
	assert.Equal(t, []Row{{Cell{Type: CellTypeI64, I64: 123}}}, r.Values)

	s = "update link set time = 456 where dst = 'alice' and src = 'bob';"
	r, err = db.ExecStmt(mustParseStmt(t, s))
	assert.Nil(t, err)
	assert.Equal(t, 1, r.Updated)

	s = "select time from link where dst = 'alice' and src = 'bob';"
	r, err = db.ExecStmt(mustParseStmt(t, s))
	assert.Nil(t, err)
	assert.Equal(t, []Row{{Cell{Type: CellTypeI64, I64: 456}}}, r.Values)

	// reopen (schema is in-memory only, re-register it)
	err = db.Close()
	assert.Nil(t, err)
	db = DB{}
	err = db.Open(path)
	assert.Nil(t, err)
	s = "create table link (time int64, src string, dst string, primary key (src, dst));"
	_, err = db.ExecStmt(mustParseStmt(t, s))
	assert.Nil(t, err)

	s = "delete from link where src = 'bob' and dst = 'alice';"
	r, err = db.ExecStmt(mustParseStmt(t, s))
	assert.Nil(t, err)
	assert.Equal(t, 1, r.Updated)

	s = "select time from link where dst = 'alice' and src = 'bob';"
	r, err = db.ExecStmt(mustParseStmt(t, s))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(r.Values))
}
