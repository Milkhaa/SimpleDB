package simpledb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseName(t *testing.T) {
	p := newParser(" a b0 _0_ 123 ")
	name, ok := p.tryName()
	assert.True(t, ok && name == "a")
	name, ok = p.tryName()
	assert.True(t, ok && name == "b0")
	name, ok = p.tryName()
	assert.True(t, ok && name == "_0_")
	_, ok = p.tryName()
	assert.False(t, ok)
}

func TestParseKeyword(t *testing.T) {
	// "sel" is a prefix of "select", so it must not match (next char is ident)
	p := newParser(" select  HELLO ")
	assert.False(t, p.tryKeyword("sel"))

	// Single keyword: match "select" (case-insensitive)
	p = newParser("select")
	assert.True(t, p.tryKeyword("SELECT") && p.isEnd())

	// Multiple keywords: consume in order
	p = newParser(" select  hello ")
	assert.True(t, p.tryKeyword("select", "hello") && p.isEnd())
}

func testParseValue(t *testing.T, s string, ref Cell) {
	p := newParser(s)
	out := Cell{}
	err := p.parseValue(&out)
	assert.Nil(t, err)
	assert.True(t, p.isEnd())
	assert.Equal(t, ref, out)
}

func TestParseValue(t *testing.T) {
	testParseValue(t, " -123 ", Cell{Type: TypeI64, I64: -123})
	testParseValue(t, ` 'abc\'\"d' `, Cell{Type: TypeStr, Str: []byte("abc'\"d")})
	testParseValue(t, ` "abc\'\"d" `, Cell{Type: TypeStr, Str: []byte("abc'\"d")})
}

func testParseStmt(t *testing.T, s string, ref interface{}) {
	stmt, err := ParseStmt(s)
	assert.Nil(t, err)
	assert.Equal(t, ref, stmt)
}

func TestParseStmt(t *testing.T) {
	var stmt interface{}
	s := "select a from t where c=1;"
	stmt = &stmtSelect{
		Table: "t",
		Cols:  []string{"a"},
		Keys:  []sqlNamedCell{{Column: "c", Value: Cell{Type: TypeI64, I64: 1}}},
	}
	testParseStmt(t, s, stmt)

	s = "select a,b_02 from T where c=1 and d='e';"
	stmt = &stmtSelect{
		Table: "T",
		Cols:  []string{"a", "b_02"},
		Keys: []sqlNamedCell{
			{Column: "c", Value: Cell{Type: TypeI64, I64: 1}},
			{Column: "d", Value: Cell{Type: TypeStr, Str: []byte("e")}},
		},
	}
	testParseStmt(t, s, stmt)

	s = "select a, b_02 from T where c = 1 and d = 'e' ; "
	testParseStmt(t, s, stmt)

	s = "create table t (a string, b int64, primary key (b));"
	stmt = &stmtCreatTable{
		Table: "t",
		Cols:  []Column{{Name: "a", Type: TypeStr}, {Name: "b", Type: TypeI64}},
		Pkey:  []string{"b"},
	}
	testParseStmt(t, s, stmt)

	s = "insert into t values (1, 'hi');"
	stmt = &stmtInsert{
		Table: "t",
		Value: []Cell{{Type: TypeI64, I64: 1}, {Type: TypeStr, Str: []byte("hi")}},
	}
	testParseStmt(t, s, stmt)

	s = "update t set a = 1, b = 2 where c = 3 and d = 4;"
	stmt = &stmtUpdate{
		Table: "t",
		Value: []sqlNamedCell{{Column: "a", Value: Cell{Type: TypeI64, I64: 1}}, {Column: "b", Value: Cell{Type: TypeI64, I64: 2}}},
		Keys:  []sqlNamedCell{{Column: "c", Value: Cell{Type: TypeI64, I64: 3}}, {Column: "d", Value: Cell{Type: TypeI64, I64: 4}}},
	}
	testParseStmt(t, s, stmt)

	s = "delete from t where c = 3 and d = 4;"
	stmt = &stmtDelete{
		Table: "t",
		Keys:  []sqlNamedCell{{Column: "c", Value: Cell{Type: TypeI64, I64: 3}}, {Column: "d", Value: Cell{Type: TypeI64, I64: 4}}},
	}
	testParseStmt(t, s, stmt)
}
