package simpledb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableByPKey(t *testing.T) {
	const path = ".tmp_table"
	defer os.RemoveAll(path)
	os.RemoveAll(path)

	db := &DB{}
	err := db.Open(path)
	assert.Nil(t, err)
	defer db.Close()

	schema := &Schema{
		Table: "session",
		Cols: []Column{
			{Name: "id", Type: CellTypeI64},
			{Name: "action", Type: CellTypeStr},
			{Name: "data", Type: CellTypeStr},
		},
		PKey: []int{0, 1}, // (id, action)
	}

	row := Row{
		Cell{Type: CellTypeI64, I64: 42},
		Cell{Type: CellTypeStr, Str: []byte("open")},
		Cell{Type: CellTypeStr, Str: []byte("item_x")},
	}
	ok, err := db.Select(schema, row)
	assert.True(t, !ok && err == nil)

	updated, err := db.Insert(schema, row)
	assert.True(t, updated && err == nil)

	out := Row{
		Cell{Type: CellTypeI64, I64: 42},
		Cell{Type: CellTypeStr, Str: []byte("open")},
		Cell{Type: CellTypeStr}, // data filled by Select
	}
	ok, err = db.Select(schema, out)
	assert.True(t, ok && err == nil)
	assert.Equal(t, row, out)

	row[2].Str = []byte("item_y")
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

// mustParseStmt parses s and fails the test on error. Used by SQL tests.
func mustParseStmt(t *testing.T, s string) interface{} {
	t.Helper()
	stmt, err := ParseStmt(s)
	require.Nil(t, err)
	return stmt
}

// TestSQLByPKey runs the same CRUD flow as TestTableByPKey but via SQL (ParseStmt + ExecStmt).
// WHERE is limited to equality (col = val and ...); range operators like >= are not supported by the parser.
func TestSQLByPKey(t *testing.T) {
	const path = ".tmp_sql"
	defer os.RemoveAll(path)
	os.RemoveAll(path)

	db := &DB{}
	err := db.Open(path)
	require.Nil(t, err)
	defer db.Close()

	s := "create table transfer (ts int64, src string, dst string, primary key (src, dst));"
	_, err = db.ExecStmt(mustParseStmt(t, s))
	require.Nil(t, err)

	s = "insert into transfer values (10, 'eve', 'mallory');"
	r, err := db.ExecStmt(mustParseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select ts from transfer where dst = 'mallory' and src = 'eve';"
	r, err = db.ExecStmt(mustParseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []Row{{Cell{Type: CellTypeI64, I64: 10}}}, r.Values)

	s = "update transfer set ts = 20 where dst = 'mallory' and src = 'eve';"
	r, err = db.ExecStmt(mustParseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select ts from transfer where dst = 'mallory' and src = 'eve';"
	r, err = db.ExecStmt(mustParseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []Row{{Cell{Type: CellTypeI64, I64: 20}}}, r.Values)

	s = "insert into transfer values (10, 'charlie', 'delta');"
	r, err = db.ExecStmt(mustParseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select ts from transfer where src = 'charlie' and dst = 'delta';"
	r, err = db.ExecStmt(mustParseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []Row{{Cell{Type: CellTypeI64, I64: 10}}}, r.Values)
}
