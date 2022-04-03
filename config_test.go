package dbh

import (
	"strconv"
	"testing"
)

func TestMysqlMark(t *testing.T) {
	DefaultConfig.Mark = MysqlMark
	cols, rows := 3, 4

	expected := "(?,?,?),(?,?,?),(?,?,?),(?,?,?)"
	got := DefaultConfig.MarkInsertValueSql(cols, rows)

	if got != expected {
		t.Errorf("expected: %s, got: %s", expected, got)
	}
}

func TestPostgresMark(t *testing.T) {
	DefaultConfig.Mark = PostgresMark
	cols, rows := 3, 4

	expected := "($1,$2,$3),($4,$5,$6),($7,$8,$9),($10,$11,$12)"
	got := DefaultConfig.MarkInsertValueSql(cols, rows)

	if got != expected {
		t.Errorf("expected: %s, got: %s", expected, got)
	}
}

func TestSqlserverMark(t *testing.T) {
	DefaultConfig.Mark = SqlserverMark
	cols, rows := 2, 3

	expected := "(@p0,@p1),(@p2,@p3),(@p4,@p5)"
	got := DefaultConfig.MarkInsertValueSql(cols, rows)

	if got != expected {
		t.Errorf("expected: %s, got: %s", expected, got)
	}
}

func TestMarkInsertValueSqlSqlServerStyleSameName(t *testing.T) {
	DefaultConfig.Mark = func(i, col, row int) string {
		if col == 0 {
			return "@id" + strconv.Itoa(row)
		}
		return "@name"
	}
	cols, rows := 2, 3

	expected := "(@id0,@name),(@id1,@name),(@id2,@name)"
	got := DefaultConfig.MarkInsertValueSql(cols, rows)

	if got != expected {
		t.Errorf("expected: %s, got: %s", expected, got)
	}
}
