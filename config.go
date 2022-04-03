package dbh

import (
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

type MarkFunc func(i, col, row int) string

type Config struct {
	// PrintSql if true, will print sql for insert
	PrintSql bool
	// Mark is used to generate param marks for value part of insert statement
	Mark    MarkFunc
	cache   map[string]string
	cacheMu sync.RWMutex
}

func NewConfig(printSql bool, markFunc MarkFunc) *Config {
	return &Config{
		PrintSql: printSql,
		Mark:     markFunc,
		cache:    make(map[string]string),
	}
}

var DefaultConfig = &Config{
	Mark:  MysqlMark,
	cache: make(map[string]string),
}

var maxInt64b = make([]byte, 19)

func MysqlMark(i, col, row int) string {
	return "?"
}

func PostgresMark(i, col, row int) string {
	maxInt64b[0] = '$'
	si := strconv.Itoa(i + 1)
	copy(maxInt64b[1:len(si)], si)
	return *(*string)(unsafe.Pointer(&maxInt64b))
}

func SqlserverMark(i, col, row int) string {
	maxInt64b[0] = '@'
	maxInt64b[1] = 'p'
	si := strconv.Itoa(i)
	copy(maxInt64b[2:len(si)], si)
	return *(*string)(unsafe.Pointer(&maxInt64b))
}

// MarkInsertValueSql generates insert value part string, param marks are depended on Mark function.
//
// Result string example: (?, ?, ?, ...), (?, ?, ?, ...), (?, ?, ?, ...)
func (c *Config) MarkInsertValueSql(colLen, rowLen int) string {
	b := strings.Builder{}
	markLen := len(c.Mark(0, 0, 0))
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
			b.WriteString(c.Mark(i*colLen+j, j, i))
		}
		if colLen == 0 {
			b.WriteString("null")
		}
		b.WriteString(")")
	}

	return b.String()
}

func (r *Config) GetCachedSql(tableName string) string {
	r.cacheMu.RLock()
	defer r.cacheMu.RUnlock()

	return r.cache[tableName]
}

func (r *Config) SetCachedSql(tableName string, sql string) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()

	r.cache[tableName] = sql
}

func (r *Config) GetAndSetCachedSql(tableName string, f func() string) string {
	r.cacheMu.RLock()
	v, ok := r.cache[tableName]
	r.cacheMu.RUnlock()
	if ok {
		return v
	}

	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	sql := f()
	r.cache[tableName] = sql
	return sql
}
