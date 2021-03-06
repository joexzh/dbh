# Simple Db helper for Go1.18

Wraps `*sql.DB`, `*sql.Tx` and `*sql.Conn`'s `QueryContext` and `ExecContext` for convenient query and insert.

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

var config = dbh.NewConfig(false, dbh.MysqlMark)

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
func (u *TestUser) Config() *dbh.Config {
    return config
}

func main() {
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

    // sql.Conn
    conn, _ := db.Conn(ctx)
    insertedCount, err := dbh.InsertContext(conn, ctx, u1)
    conn.Close()

    // Bulk insert
    var users []*TestUser
    for i := 0; i < 500000; i++ {
        users = append(users, &TestUser{Id: i, Name: "Joe", Age: 30})
    }
    bulkSize := 1000
    insertdCount, err := dbh.BulkInsertContext(db, ctx, bulkSize, users...)
}
```

dbh query and insert functions accept `*sql.DB`, `*sql.Tx` or `*sql.Conn` as first argument.

`Config` is only used for insert. A `DefaultConfig` is provided. `Config.Mark` function is used for insert value parameter marks.
Simple Mark function is provided, `MysqlMark`, `PostgresMark`, `SqlserverMark`

`Args()` funtion must be implemented by pointer to the model struct/type, and return a slice of pointers. It's for rows scan and exec arguments.
For select query only, implement `ArgsProvider` (the `Args()` function) is enough.
