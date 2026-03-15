package simpledb

import (
	"errors"
	"strconv"
	"strings"
	"unicode"
)

// parser parses SQL-like statements.
type parser struct {
	s string
	i int
}

// newParser returns a parser for the given input string.
func newParser(s string) *parser {
	return &parser{s: s}
}

// skipSpace advances p.i past any spaces and tabs.
func (p *parser) skipSpace() {
	for p.i < len(p.s) && (p.s[p.i] == ' ' || p.s[p.i] == '\t') {
		p.i++
	}
}

// tryName parses an identifier (letter or underscore, then alphanumeric/underscore). Returns name and true, or "", false.
func (p *parser) tryName() (string, bool) {
	p.skipSpace()
	if p.i >= len(p.s) {
		return "", false
	}
	r := rune(p.s[p.i])
	if r != '_' && !unicode.IsLetter(r) {
		return "", false
	}
	start := p.i
	p.i++
	for p.i < len(p.s) {
		r = rune(p.s[p.i])
		if r != '_' && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			break
		}
		p.i++
	}
	return p.s[start:p.i], true
}

// tryKeyword consumes one or more keywords (case-insensitive) in order. Returns true if all matched.
func (p *parser) tryKeyword(keywords ...string) bool {
	p.skipSpace()
	if len(keywords) == 0 {
		return false
	}
	for i, kw := range keywords {
		if p.i+len(kw) > len(p.s) {
			return false
		}
		if !strings.EqualFold(p.s[p.i:p.i+len(kw)], kw) {
			return false
		}
		if p.i+len(kw) < len(p.s) && isIdentCont(rune(p.s[p.i+len(kw)])) {
			return false
		}
		p.i += len(kw)
		if i+1 < len(keywords) {
			p.skipSpace()
		}
	}
	return true
}

// isIdentCont reports whether r is a character that may continue an identifier (letter, digit, or underscore).
func isIdentCont(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// isEnd reports whether only whitespace remains in the input.
func (p *parser) isEnd() bool {
	p.skipSpace()
	return p.i >= len(p.s)
}

var errInvalidValue = errors.New("invalid value")

// parseValue parses an integer or quoted string into c. Returns an error if no valid value is found.
func (p *parser) parseValue(c *Cell) error {
	p.skipSpace()
	if p.i >= len(p.s) {
		return errInvalidValue
	}
	if p.s[p.i] == '\'' || p.s[p.i] == '"' {
		quote := p.s[p.i]
		p.i++
		var buf []byte
		for p.i < len(p.s) {
			if p.s[p.i] == '\\' && p.i+1 < len(p.s) {
				p.i++
				buf = append(buf, p.s[p.i])
				p.i++
				continue
			}
			if p.s[p.i] == quote {
				p.i++
				c.Type = CellTypeStr
				c.Str = buf
				return nil
			}
			buf = append(buf, p.s[p.i])
			p.i++
		}
		return errInvalidValue
	}
	start := p.i
	if p.s[p.i] == '-' {
		p.i++
	}
	if p.i >= len(p.s) || p.s[p.i] < '0' || p.s[p.i] > '9' {
		return errInvalidValue
	}
	for p.i < len(p.s) && p.s[p.i] >= '0' && p.s[p.i] <= '9' {
		p.i++
	}
	n, err := strconv.ParseInt(p.s[start:p.i], 10, 64)
	if err != nil {
		return errInvalidValue
	}
	c.Type = CellTypeI64
	c.I64 = n
	return nil
}

// sqlNamedCell holds a column name and value for WHERE or SET clauses.
type sqlNamedCell struct {
	Column string
	Value  Cell
}

// stmtSelect is the parsed form of a SELECT statement.
type stmtSelect struct {
	Table string
	Cols  []string
	Keys  []sqlNamedCell
}

// stmtCreateTable is the parsed form of a CREATE TABLE statement.
type stmtCreateTable struct {
	Table string
	Cols  []Column
	Pkey  []string
}

// stmtInsert is the parsed form of an INSERT statement.
type stmtInsert struct {
	Table string
	Value []Cell
}

// stmtUpdate is the parsed form of an UPDATE statement.
type stmtUpdate struct {
	Table string
	Value []sqlNamedCell
	Keys  []sqlNamedCell
}

// stmtDelete is the parsed form of a DELETE statement.
type stmtDelete struct {
	Table string
	Keys  []sqlNamedCell
}

// parseSelect parses a SELECT statement (select cols from table [where key=val and ...]).
func (p *parser) parseSelect() (*stmtSelect, error) {
	if !p.tryKeyword("select") {
		return nil, errInvalidValue
	}
	var cols []string
	for {
		name, ok := p.tryName()
		if !ok {
			return nil, errInvalidValue
		}
		cols = append(cols, name)
		p.skipSpace()
		if p.i < len(p.s) && p.s[p.i] == ',' {
			p.i++
			continue
		}
		break
	}
	if !p.tryKeyword("from") {
		return nil, errInvalidValue
	}
	table, ok := p.tryName()
	if !ok {
		return nil, errInvalidValue
	}
	keys, err := p.parseWhereKeys()
	if err != nil {
		return nil, err
	}
	return &stmtSelect{Table: table, Cols: cols, Keys: keys}, nil
}

// consumeEqual skips space and consumes a single '='. Returns true if found.
func (p *parser) consumeEqual() bool {
	p.skipSpace()
	if p.i < len(p.s) && p.s[p.i] == '=' {
		p.i++
		return true
	}
	return false
}

// parseWhereKeys parses an optional "where col=val and ..." and returns the key predicates.
func (p *parser) parseWhereKeys() ([]sqlNamedCell, error) {
	var keys []sqlNamedCell
	if p.tryKeyword("where") {
		for {
			col, ok := p.tryName()
			if !ok {
				return nil, errInvalidValue
			}
			if !p.consumeEqual() {
				return nil, errInvalidValue
			}
			var c Cell
			if err := p.parseValue(&c); err != nil {
				return nil, err
			}
			keys = append(keys, sqlNamedCell{Column: col, Value: c})
			p.skipSpace()
			if p.tryKeyword("and") {
				continue
			}
			break
		}
	}
	return keys, nil
}

// parseCreateTable parses a CREATE TABLE statement (create table name (col type, ... , primary key (pkey))).
func (p *parser) parseCreateTable() (*stmtCreateTable, error) {
	if !p.tryKeyword("create", "table") {
		return nil, errInvalidValue
	}
	table, ok := p.tryName()
	if !ok {
		return nil, errInvalidValue
	}
	p.skipSpace()
	if p.i >= len(p.s) || p.s[p.i] != '(' {
		return nil, errInvalidValue
	}
	p.i++
	p.skipSpace()
	var cols []Column
	for {
		name, ok := p.tryName()
		if !ok {
			return nil, errInvalidValue
		}
		if name == "primary" && p.tryKeyword("key") {
			break
		}
		typ := CellTypeStr
		if p.tryKeyword("int64") {
			typ = CellTypeI64
		} else if !p.tryKeyword("string") {
			return nil, errInvalidValue
		}
		cols = append(cols, Column{Name: name, Type: typ})
		p.skipSpace()
		if p.i < len(p.s) && p.s[p.i] == ',' {
			p.i++
			p.skipSpace()
			continue
		}
		if !p.tryKeyword("primary", "key") {
			return nil, errInvalidValue
		}
		break
	}
	p.skipSpace()
	if p.i >= len(p.s) || p.s[p.i] != '(' {
		return nil, errInvalidValue
	}
	p.i++
	p.skipSpace()
	var pkey []string
	for {
		name, ok := p.tryName()
		if !ok {
			return nil, errInvalidValue
		}
		pkey = append(pkey, name)
		p.skipSpace()
		if p.i < len(p.s) && p.s[p.i] == ',' {
			p.i++
			p.skipSpace()
			continue
		}
		break
	}
	if p.i >= len(p.s) || p.s[p.i] != ')' {
		return nil, errInvalidValue
	}
	p.i++
	p.skipSpace()
	if p.i >= len(p.s) || p.s[p.i] != ')' {
		return nil, errInvalidValue
	}
	p.i++
	return &stmtCreateTable{Table: table, Cols: cols, Pkey: pkey}, nil
}

// parseInsert parses an INSERT statement (insert into table values (val, ...)).
func (p *parser) parseInsert() (*stmtInsert, error) {
	if !p.tryKeyword("insert", "into") {
		return nil, errInvalidValue
	}
	table, ok := p.tryName()
	if !ok {
		return nil, errInvalidValue
	}
	if !p.tryKeyword("values") {
		return nil, errInvalidValue
	}
	p.skipSpace()
	if p.i >= len(p.s) || p.s[p.i] != '(' {
		return nil, errInvalidValue
	}
	p.i++
	p.skipSpace()
	var vals []Cell
	for {
		var c Cell
		if err := p.parseValue(&c); err != nil {
			return nil, err
		}
		vals = append(vals, c)
		p.skipSpace()
		if p.i < len(p.s) && p.s[p.i] == ',' {
			p.i++
			p.skipSpace()
			continue
		}
		break
	}
	if p.i >= len(p.s) || p.s[p.i] != ')' {
		return nil, errInvalidValue
	}
	p.i++
	return &stmtInsert{Table: table, Value: vals}, nil
}

// parseUpdate parses an UPDATE statement (update table set col=val, ... [where key=val and ...]).
func (p *parser) parseUpdate() (*stmtUpdate, error) {
	if !p.tryKeyword("update") {
		return nil, errInvalidValue
	}
	table, ok := p.tryName()
	if !ok {
		return nil, errInvalidValue
	}
	if !p.tryKeyword("set") {
		return nil, errInvalidValue
	}
	var value []sqlNamedCell
	for {
		col, ok := p.tryName()
		if !ok {
			return nil, errInvalidValue
		}
		if !p.consumeEqual() {
			return nil, errInvalidValue
		}
		var c Cell
		if err := p.parseValue(&c); err != nil {
			return nil, err
		}
		value = append(value, sqlNamedCell{Column: col, Value: c})
		p.skipSpace()
		if p.i < len(p.s) && p.s[p.i] == ',' {
			p.i++
			p.skipSpace()
			continue
		}
		break
	}
	keys, err := p.parseWhereKeys()
	if err != nil {
		return nil, err
	}
	return &stmtUpdate{Table: table, Value: value, Keys: keys}, nil
}

// parseDelete parses a DELETE statement (delete from table [where key=val and ...]).
func (p *parser) parseDelete() (*stmtDelete, error) {
	if !p.tryKeyword("delete", "from") {
		return nil, errInvalidValue
	}
	table, ok := p.tryName()
	if !ok {
		return nil, errInvalidValue
	}
	keys, err := p.parseWhereKeys()
	if err != nil {
		return nil, err
	}
	return &stmtDelete{Table: table, Keys: keys}, nil
}

// parseStmt parses one statement from the current position; input must end with ';'.
func (p *parser) parseStmt() (interface{}, error) {
	p.skipSpace()
	if p.i >= len(p.s) {
		return nil, errInvalidValue
	}
	start := p.i
	name, _ := p.tryName()
	p.i = start
	p.skipSpace()

	var stmt interface{}
	var err error
	switch name {
	case "select":
		stmt, err = p.parseSelect()
	case "create":
		stmt, err = p.parseCreateTable()
	case "insert":
		stmt, err = p.parseInsert()
	case "update":
		stmt, err = p.parseUpdate()
	case "delete":
		stmt, err = p.parseDelete()
	default:
		return nil, errInvalidValue
	}
	if err != nil {
		return nil, err
	}
	p.skipSpace()
	if p.i >= len(p.s) || p.s[p.i] != ';' {
		return nil, errInvalidValue
	}
	p.i++
	return stmt, nil
}

// ParseStmt parses a single SQL statement string and returns the statement value
// for use with DB.ExecStmt. Statements must end with a semicolon.
func ParseStmt(s string) (interface{}, error) {
	p := newParser(s)
	return p.parseStmt()
}
