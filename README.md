# Db helper for Go1.18

Wraps `*sql.DB`, `*sql.Tx` for convenient query and insert.

Uses generics for table model mapping.

## Install

`go get github.com/joexzh/dbh`

## Usage

```go
package main

import ...

type TestUser struct {
    Id   int
    Name string
    Age  int
}

// implement TableInfoProvider interface
func (u *TestUser) Args() []any {
    return []any{&u.Id, &u.Name, &u.Age}
}
func (u *TestUser) Columns() []string {
    return []string{"id", "name", "age"}
}
func (u *TestUser) TableName() string {
    return "users"
}

func main() {
    dbh.DefaultConfig.PrintSql = true
    dbh.DefaultConfig.Mark = dbh.MysqlMark
    dbh.DefaultConfig.BulkInsertStmtThreshold = 20

    db, _ := sql.Open(...)
    ctx := context.Background()

    // select []*TestUser
    users, err := dbh.QueryContext[*TestUser](db, ctx, "select * from users where name=? and age=?", "John", 30)
    if err != nil {
        log.Fatal(err)
    }

    // insert
    user := TestUser{Id: 2, Name: "John", Age: 30}
    insertedCount, err := dbh.InsertContext(db, ctx, &user)

    // transaction
    tx, _ := db.BeginTx(ctx, nil)
    u := &TestUser{Id: 2, Name: "John", Age: 30}
    insertedCount, err := dbh.InsertContext(tx, ctx, u1)
    tx.Commit()

    // Bulk insert
    var users []*TestUser
    for i := 0; i < 500000; i++ {
        users = append(users, &TestUser{Id: i, Name: "Joe", Age: 30})
    }
    bulkSize := 1000
    insertdCount, err := dbh.BulkInsertContext(db, ctx, bulkSize, users...)

    // use another config in context
    config := NewConfig()
    config.Mark = func(i, col, row int) string {
        return "@param" + strconv.Itoa(i)
    }
    // config must be a pointer adding to context value
    ctx = context.WithValue(ctx, dbh.ConfigKey, config)
    insertedCount, err := dbh.InsertContext(db, ctx, &user)
}
```

`Config` is only used for insert.

`Config.Mark` function is used for insert value parameter marks.
Simple Mark function is provided, `MysqlMark`, `PostgresMark`, `SqlserverMark`

For select query only, implement `ArgsProvider` is enough.
