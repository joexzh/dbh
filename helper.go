package dbh

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// ArgsProvider provide arguments for Query functions.
type ArgsProvider interface {
	// Args must return a slice of pointers of the field values in column order, for *sql.Rows.Scan(dest ...any) and for insert args.
	// It must be implemented by the pointer of the model type.
	Args() []any
}

// TableInfoProvider provide arguments, column names and table name for Insert functions.
type TableInfoProvider interface {
	ArgsProvider
	Columns() []string
	TableName() string
	Config() *Config
}

type queryable interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type queryableRow interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type executable interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

func QueryRowContext[T ArgsProvider](q queryableRow, ctx context.Context, queryString string, vals ...any) (T, error) {
	row := q.QueryRowContext(ctx, queryString, vals...)
	t := newT[T]()
	if err := row.Scan(t.Args()...); err != nil {
		return t, err
	}
	return t, nil
}

func QueryRow[T ArgsProvider](q queryableRow, queryString string, vals ...any) (T, error) {
	return QueryRowContext[T](q, context.Background(), queryString, vals...)
}

func QueryContext[T ArgsProvider](q queryable, ctx context.Context, queryString string, vals ...any) ([]T, error) {
	rows, err := q.QueryContext(ctx, queryString, vals...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	list := make([]T, 0)
	if err = ScanList(rows, &list); err != nil {
		return nil, err
	}
	return list, nil
}

func Query[T ArgsProvider](q queryable, queryString string, vals ...any) ([]T, error) {
	return QueryContext[T](q, context.Background(), queryString, vals...)
}

func BulkInsertContext[T TableInfoProvider](ex executable, ctx context.Context, bulkSize int, list ...T) (int64, error) {
	for len(list) == 0 {
		return 0, nil
	}
	if bulkSize <= 0 {
		bulkSize = 1
	}
	tableName := list[0].TableName()
	cols := list[0].Columns()
	config := list[0].Config()

	var (
		total   int64
		stmt    *sql.Stmt
		useStmt bool
		err     error
	)
	if len(list)/bulkSize >= 2 {
		useStmt = true
		prepareSql := fmt.Sprintf("insert into %s (%s) values %s",
			tableName, strings.Join(cols, ","), config.MarkInsertValueSql(len(cols), bulkSize))
		if config.PrintSql {
			fmt.Println("prepared statement:", prepareSql)
		}
		stmt, err = ex.PrepareContext(ctx, prepareSql)
		if err != nil {
			return 0, err
		}
		defer stmt.Close()
	}
	for i := 0; i < len(list); i += bulkSize {
		end := i + bulkSize
		if end > len(list) {
			end = len(list)
			useStmt = false
		}
		_l := list[i:end]
		vals := make([]any, 0, len(cols)*len(_l))
		for _, t := range _l {
			vals = append(vals, t.Args()...)
		}
		if useStmt {
			ret, err := stmt.ExecContext(ctx, vals...)
			if err != nil {
				return 0, err
			}
			ra, _ := ret.RowsAffected()
			total += ra
		} else {
			var sqlString string
			if len(_l) == 1 {
				sqlString = config.GetAndSetCachedSql(tableName+"_insert_one", func() string {
					return fmt.Sprintf("insert into %s (%s) values %s",
						tableName, strings.Join(cols, ","), config.MarkInsertValueSql(len(cols), 1))
				})
			} else {
				sqlString = fmt.Sprintf("insert into %s (%s) values %s",
					tableName, strings.Join(cols, ","), config.MarkInsertValueSql(len(cols), len(_l)))
			}
			if config.PrintSql {
				fmt.Println(sqlString)
			}
			ret, err := ex.ExecContext(ctx, sqlString, vals...)
			if err != nil {
				return 0, err
			}
			ra, _ := ret.RowsAffected()
			total += ra
		}
	}

	return total, nil
}

func BulkInsert[T TableInfoProvider](ex executable, bulkSize int, list ...T) (int64, error) {
	return BulkInsertContext(ex, context.Background(), bulkSize, list...)
}

func Insert[T TableInfoProvider](ex executable, t T) (int64, error) {
	return BulkInsertContext(ex, context.Background(), 1, t)
}

func InsertContext[T TableInfoProvider](ex executable, ctx context.Context, t T) (int64, error) {
	return BulkInsertContext(ex, ctx, 1, t)
}

func ScanList[T ArgsProvider](rows *sql.Rows, list *[]T) error {
	for i := 0; rows.Next(); i++ {
		t := newT[T]()
		err := rows.Scan(t.Args()...)
		if err != nil {
			return err
		}
		if i < len(*list) {
			(*list)[i] = t
		} else {
			*list = append(*list, t)
		}
	}
	return nil
}

func newT[T any]() T {
	// TODO we need a better way to init T efficiently
	t := *new(T)
	if typ := reflect.TypeOf(t); typ.Kind() == reflect.Ptr {
		return reflect.New(typ.Elem()).Interface().(T)
	}
	return t
}
