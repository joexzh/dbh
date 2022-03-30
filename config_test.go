package dbh

import (
	"context"
	"strconv"
	"testing"
)

func TestMarkInsertValueSqlMysqlStyle(t *testing.T) {
	DefaultConfig.Mark = func(i, col, row int) string {
		return "?"
	}
	cols, rows := 3, 4

	expected := "(?,?,?),(?,?,?),(?,?,?),(?,?,?)"
	got := DefaultConfig.MarkInsertValueSql(cols, rows)

	if got != expected {
		t.Errorf("expected: %s, got: %s", expected, got)
	}
}

func TestMarkInsertValueSqlPostgresStyle(t *testing.T) {
	DefaultConfig.Mark = func(i, col, row int) string {
		return "$" + strconv.Itoa(i+1)
	}
	cols, rows := 3, 4

	expected := "($1,$2,$3),($4,$5,$6),($7,$8,$9),($10,$11,$12)"
	got := DefaultConfig.MarkInsertValueSql(cols, rows)

	if got != expected {
		t.Errorf("expected: %s, got: %s", expected, got)
	}
}

func TestMarkInsertValueSqlSqlServerStyle(t *testing.T) {
	DefaultConfig.Mark = func(i, col, row int) string {
		return "@P" + strconv.Itoa(i)
	}
	cols, rows := 2, 3

	expected := "(@P0,@P1),(@P2,@P3),(@P4,@P5)"
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

func TestFindFromContextKeyNotMatch(t *testing.T) {
	ctx := context.Background()
	c := findFromContext(ctx)

	if c != nil {
		t.Errorf("expected: nil, got: %v", c)
	}
}

func TestFindFromContextKeyMatchValueNotMatch(t *testing.T) {
	ctx := context.WithValue(context.Background(), ConfigKey, "test")
	c := findFromContext(ctx)

	if c != nil {
		t.Errorf("expected: nil, got: %v", c)
	}
}

func TestFindFromContextKeyValueMatch(t *testing.T) {
	ctx := context.WithValue(context.Background(), ConfigKey, &Config{})
	c := findFromContext(ctx)

	if c == nil {
		t.Errorf("expected: not nil, got: nil")
	}
}