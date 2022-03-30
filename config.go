package dbh

import (
	"context"
	"strconv"
	"strings"
)

type Config struct {
	// PrintSql if true, will print sql for insert
	PrintSql bool
	// BulkInsertStmtThreshold controls if prepared statement will be used for BulkInsert when len(list)/bulkSize > BulkInsertStmtThreshold.
	//
	// Default value is 0, which prepared statement is disabled.
	BulkInsertStmtThreshold int
	// Mark is used to generate param marks for value part of insert statement
	Mark func(i, col, row int) string
}

type configKey string

var ConfigKey = configKey("dbh.Config")

func findFromContext(ctx context.Context) *Config {
	if v := ctx.Value(ConfigKey); v != nil {
		if c, ok := v.(*Config); ok {
			return c
		}

	}
	return nil
}

var DefaultConfig = &Config{
	Mark: func(i, col, row int) string {
		return "?"
	},
}

func MysqlMark(i, col, row int) string {
	return "?"
}

func PostgresMark(i, col, row int) string {
	return "$" + strconv.Itoa(i+1)
}

func SqlserverMark(i, col, row int) string {
	return "@p" + strconv.Itoa(i)
}

// MarkInsertValueSql generates insert value part string, param marks are depended on Mark function.
//
// Result string example: (?, ?, ?, ...), (?, ?, ?, ...), (?, ?, ?, ...)
func (r *Config) MarkInsertValueSql(colLen, rowLen int) string {
	b := strings.Builder{}
	markLen := len(r.Mark(0, 0, 0))
	b.Grow(2 + (markLen+1)*colLen*rowLen)

	for i := 0; i < rowLen; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("(")
		for j := 0; j < colLen; j++ {
			if j > 0 {
				b.WriteString(",")
			}
			b.WriteString(r.Mark(i*colLen+j, j, i))
		}
		if colLen == 0 {
			b.WriteString("null")
		}
		b.WriteString(")")
	}

	return b.String()
}
