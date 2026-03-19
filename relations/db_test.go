package relations

import (
	"os"
	"path/filepath"
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
		Indices: [][]int{{0, 1}}, // primary key: (id, action)
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

func TestSecondaryIndexMaintenance(t *testing.T) {
	const path = ".tmp_secondary_index"
	defer os.RemoveAll(path)
	os.RemoveAll(path)

	db := &DB{}
	err := db.Open(path)
	require.NoError(t, err)
	defer db.Close()

	stmt := "create table t (id int64, email string, name string, primary key (id), index (email));"
	_, err = db.ExecStmt(mustParseStmt(t, stmt))
	require.NoError(t, err)

	schema, err := db.GetSchema("t")
	require.NoError(t, err)
	require.Len(t, schema.Indices, 2) // pkey + secondary

	// Insert row, secondary index entry should be created.
	stmt = "insert into t values (1, 'a@x', 'alice');"
	_, err = db.ExecStmt(mustParseStmt(t, stmt))
	require.NoError(t, err)

	row := schema.NewRow()
	row[0] = Cell{Type: CellTypeI64, I64: 1}
	row[1] = Cell{Type: CellTypeStr, Str: []byte("a@x")}
	row[2] = Cell{Type: CellTypeStr, Str: []byte("alice")}
	idxKey, err := indexEntryKey(schema, 1, row)
	require.NoError(t, err)
	_, ok, err := db.store.Get(idxKey)
	require.NoError(t, err)
	require.True(t, ok)

	// Update indexed column, old index key must be removed, new one added.
	stmt = "update t set email = 'b@x' where id = 1;"
	_, err = db.ExecStmt(mustParseStmt(t, stmt))
	require.NoError(t, err)

	_, ok, err = db.store.Get(idxKey)
	require.NoError(t, err)
	require.False(t, ok)

	row[1] = Cell{Type: CellTypeStr, Str: []byte("b@x")}
	newIdxKey, err := indexEntryKey(schema, 1, row)
	require.NoError(t, err)
	_, ok, err = db.store.Get(newIdxKey)
	require.NoError(t, err)
	require.True(t, ok)

	// Delete row, secondary index entry should be removed.
	stmt = "delete from t where id = 1;"
	_, err = db.ExecStmt(mustParseStmt(t, stmt))
	require.NoError(t, err)
	_, ok, err = db.store.Get(newIdxKey)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestSelectByIndex(t *testing.T) {
	const path = ".tmp_select_by_index"
	defer os.RemoveAll(path)
	os.RemoveAll(path)

	db := &DB{}
	err := db.Open(path)
	require.NoError(t, err)
	defer db.Close()

	// Table with index on (c). SELECT where c = val uses the index.
	_, err = db.ExecStmt(mustParseStmt(t, "create table t (a string, b string, c string, primary key (a), index (c));"))
	require.NoError(t, err)

	_, err = db.ExecStmt(mustParseStmt(t, "insert into t values ('x', 'y', '45');"))
	require.NoError(t, err)
	_, err = db.ExecStmt(mustParseStmt(t, "insert into t values ('p', 'q', '99');"))
	require.NoError(t, err)
	_, err = db.ExecStmt(mustParseStmt(t, "insert into t values ('r', 's', '45');"))
	require.NoError(t, err)

	// Select by index (c): where c = '45' should return two rows. (Parser does not support "select *".)
	r, err := db.ExecStmt(mustParseStmt(t, "select a, b, c from t where c = '45';"))
	require.NoError(t, err)
	require.Len(t, r.Values, 2)
	// Rows can be in any order; check we have (x,y,45) and (r,s,45).
	names := make([]string, len(r.Values))
	for i, row := range r.Values {
		names[i] = string(row[0].Str) + string(row[1].Str) + string(row[2].Str)
	}
	require.Contains(t, names, "xy45")
	require.Contains(t, names, "rs45")

	// Select by PK still works.
	r, err = db.ExecStmt(mustParseStmt(t, "select a, b, c from t where a = 'x';"))
	require.NoError(t, err)
	require.Len(t, r.Values, 1)
	require.Equal(t, []byte("x"), r.Values[0][0].Str)
	require.Equal(t, []byte("y"), r.Values[0][1].Str)
	require.Equal(t, []byte("45"), r.Values[0][2].Str)
}

func TestUpdateByIndex(t *testing.T) {
	const path = ".tmp_update_by_index"
	defer os.RemoveAll(path)
	os.RemoveAll(path)

	db := &DB{}
	err := db.Open(path)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecStmt(mustParseStmt(t, "create table t (a string, b string, c string, primary key (a), index (c));"))
	require.NoError(t, err)
	_, err = db.ExecStmt(mustParseStmt(t, "insert into t values ('x', 'y', '45');"))
	require.NoError(t, err)
	_, err = db.ExecStmt(mustParseStmt(t, "insert into t values ('r', 's', '45');"))
	require.NoError(t, err)

	// Update both rows where c = '45' via index.
	r, err := db.ExecStmt(mustParseStmt(t, "update t set b = 'updated' where c = '45';"))
	require.NoError(t, err)
	require.Equal(t, 2, r.Updated)

	// Update both rows where b = 'something' via index. This should fail because b is not indexed.
	r, err = db.ExecStmt(mustParseStmt(t, "update t set c = 'updated' where b = 'something';"))
	require.Error(t, err)
	require.Equal(t, 0, r.Updated)

	r, err = db.ExecStmt(mustParseStmt(t, "select a, b, c from t where c = '45';"))
	require.NoError(t, err)
	require.Len(t, r.Values, 2)
	for _, row := range r.Values {
		require.Equal(t, []byte("updated"), row[1].Str)
	}

	//Fail select because b is not indexed.
	r, err = db.ExecStmt(mustParseStmt(t, "select a, b, c from t where b = '45';"))
	require.Error(t, err)
	require.Equal(t, 0, r.Updated)

	// Update by PK still works (one row).
	r, err = db.ExecStmt(mustParseStmt(t, "update t set b = 'one' where a = 'x';"))
	require.NoError(t, err)
	require.Equal(t, 1, r.Updated)
	r, err = db.ExecStmt(mustParseStmt(t, "select a, b, c from t where a = 'x';"))
	require.NoError(t, err)
	require.Equal(t, []byte("one"), r.Values[0][1].Str)
}

func TestDeleteByIndex(t *testing.T) {
	const path = ".tmp_delete_by_index"
	defer os.RemoveAll(path)
	os.RemoveAll(path)

	db := &DB{}
	err := db.Open(path)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecStmt(mustParseStmt(t, "create table t (a string, b string, c string, primary key (a), index (c));"))
	require.NoError(t, err)
	_, err = db.ExecStmt(mustParseStmt(t, "insert into t values ('x', 'y', '45');"))
	require.NoError(t, err)
	_, err = db.ExecStmt(mustParseStmt(t, "insert into t values ('r', 's', '45');"))
	require.NoError(t, err)
	_, err = db.ExecStmt(mustParseStmt(t, "insert into t values ('p', 'q', '99');"))
	require.NoError(t, err)

	// Delete both rows where c = '45' via index.
	r, err := db.ExecStmt(mustParseStmt(t, "delete from t where c = '45';"))
	require.NoError(t, err)
	require.Equal(t, 2, r.Updated)

	r, err = db.ExecStmt(mustParseStmt(t, "select a, b, c from t where c = '45';"))
	require.NoError(t, err)
	require.Len(t, r.Values, 0)

	// Only row with c = '99' remains.
	r, err = db.ExecStmt(mustParseStmt(t, "select a, b, c from t where a = 'p';"))
	require.NoError(t, err)
	require.Len(t, r.Values, 1)
	require.Equal(t, []byte("99"), r.Values[0][2].Str)

	// Delete by PK still works.
	r, err = db.ExecStmt(mustParseStmt(t, "delete from t where a = 'p';"))
	require.NoError(t, err)
	require.Equal(t, 1, r.Updated)
	r, err = db.ExecStmt(mustParseStmt(t, "select a, b, c from t where a = 'p';"))
	require.NoError(t, err)
	require.Len(t, r.Values, 0)
}

func truncateWALLastByte(t *testing.T, dir string) {
	t.Helper()
	walPath := filepath.Join(dir, "wal")
	fp, err := os.OpenFile(walPath, os.O_RDWR, 0o644)
	require.NoError(t, err)
	st, err := fp.Stat()
	require.NoError(t, err)
	require.Greater(t, st.Size(), int64(0))
	require.NoError(t, fp.Truncate(st.Size()-1))
	require.NoError(t, fp.Close())
}

func TestNoWhereSelectUpdateDelete(t *testing.T) {
	dir := t.TempDir()

	db := &DB{}
	require.NoError(t, db.Open(dir))
	defer db.Close()

	_, err := db.ExecStmt(mustParseStmt(t, "create table t (id int64, name string, primary key (id), index (name));"))
	require.NoError(t, err)

	_, err = db.ExecStmt(mustParseStmt(t, "insert into t values (1, 'a');"))
	require.NoError(t, err)
	_, err = db.ExecStmt(mustParseStmt(t, "insert into t values (2, 'b');"))
	require.NoError(t, err)

	// SELECT without WHERE => full-table scan.
	r, err := db.ExecStmt(mustParseStmt(t, "select id, name from t;"))
	require.NoError(t, err)
	require.Len(t, r.Values, 2)

	// UPDATE without WHERE => update all rows.
	r, err = db.ExecStmt(mustParseStmt(t, "update t set name = 'z';"))
	require.NoError(t, err)
	require.Equal(t, 2, r.Updated)

	// Confirm secondary index updates after UPDATE (no crash simulation here).
	r, err = db.ExecStmt(mustParseStmt(t, "select id, name from t where name = 'z';"))
	require.NoError(t, err)
	require.Len(t, r.Values, 2)

	// DELETE without WHERE => delete all rows.
	r, err = db.ExecStmt(mustParseStmt(t, "delete from t;"))
	require.NoError(t, err)
	require.Equal(t, 2, r.Updated)

	r, err = db.ExecStmt(mustParseStmt(t, "select id from t;"))
	require.NoError(t, err)
	require.Len(t, r.Values, 0)
}

func TestUpdateRejectsPrimaryKeyChange(t *testing.T) {
	dir := t.TempDir()

	db := &DB{}
	require.NoError(t, db.Open(dir))
	defer db.Close()

	_, err := db.ExecStmt(mustParseStmt(t, "create table t (id int64, data string, primary key (id));"))
	require.NoError(t, err)
	_, err = db.ExecStmt(mustParseStmt(t, "insert into t values (1, 'a');"))
	require.NoError(t, err)

	_, err = db.ExecStmt(mustParseStmt(t, "update t set id = 2, data = 'b' where id = 1;"))
	require.Error(t, err)

	// Old row should remain.
	r, err := db.ExecStmt(mustParseStmt(t, "select data from t where id = 1;"))
	require.NoError(t, err)
	require.Len(t, r.Values, 1)
	require.Equal(t, []byte("a"), r.Values[0][0].Str)

	// New PK row must not exist.
	r, err = db.ExecStmt(mustParseStmt(t, "select data from t where id = 2;"))
	require.NoError(t, err)
	require.Len(t, r.Values, 0)
}

func TestSecondaryIndexUpdateAtomicOnCrash(t *testing.T) {
	dir := t.TempDir()

	db := &DB{}
	require.NoError(t, db.Open(dir))

	_, err := db.ExecStmt(mustParseStmt(t, "create table t (id int64, email string, name string, primary key (id), index (email));"))
	require.NoError(t, err)
	_, err = db.ExecStmt(mustParseStmt(t, "insert into t values (1, 'a@x', 'alice');"))
	require.NoError(t, err)

	// Change indexed column so index mutations must be consistent with the primary row.
	_, err = db.ExecStmt(mustParseStmt(t, "update t set email = 'b@x' where id = 1;"))
	require.NoError(t, err)

	require.NoError(t, db.Close())
	truncateWALLastByte(t, dir)

	db2 := &DB{}
	require.NoError(t, db2.Open(dir))
	defer db2.Close()

	// After rollback of the last transaction, the old email must remain.
	r, err := db2.ExecStmt(mustParseStmt(t, "select email from t where id = 1;"))
	require.NoError(t, err)
	require.Len(t, r.Values, 1)
	require.Equal(t, []byte("a@x"), r.Values[0][0].Str)

	// Old index should still exist...
	r, err = db2.ExecStmt(mustParseStmt(t, "select id from t where email = 'a@x';"))
	require.NoError(t, err)
	require.Len(t, r.Values, 1)

	// ...and new index must not.
	r, err = db2.ExecStmt(mustParseStmt(t, "select id from t where email = 'b@x';"))
	require.NoError(t, err)
	require.Len(t, r.Values, 0)
}

func TestSecondaryIndexDeleteAtomicOnCrash(t *testing.T) {
	dir := t.TempDir()

	db := &DB{}
	require.NoError(t, db.Open(dir))

	_, err := db.ExecStmt(mustParseStmt(t, "create table t (id int64, email string, name string, primary key (id), index (email));"))
	require.NoError(t, err)
	_, err = db.ExecStmt(mustParseStmt(t, "insert into t values (1, 'a@x', 'alice');"))
	require.NoError(t, err)

	// Delete the row; crash after completion by truncating the WAL tail.
	r, err := db.ExecStmt(mustParseStmt(t, "delete from t where id = 1;"))
	require.NoError(t, err)
	require.Equal(t, 1, r.Updated)

	require.NoError(t, db.Close())
	truncateWALLastByte(t, dir)

	db2 := &DB{}
	require.NoError(t, db2.Open(dir))
	defer db2.Close()

	// The row should still exist after rollback.
	r, err = db2.ExecStmt(mustParseStmt(t, "select email from t where id = 1;"))
	require.NoError(t, err)
	require.Len(t, r.Values, 1)
	require.Equal(t, []byte("a@x"), r.Values[0][0].Str)

	// And the old index should still exist as well.
	r, err = db2.ExecStmt(mustParseStmt(t, "select id from t where email = 'a@x';"))
	require.NoError(t, err)
	require.Len(t, r.Values, 1)
}
