package dbh

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"log"
	"reflect"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

type TestUser struct {
	Id   int
	Name string
	Age  int
}

func (u *TestUser) Args() []any {
	return []any{&u.Id, &u.Name, &u.Age}
}
func (u *TestUser) Columns() []string {
	return []string{"id", "name", "age"}
}
func (u *TestUser) TableName() string {
	return "users"
}
func (u *TestUser) Config() *Config {
	return DefaultConfig
}

var u1 = TestUser{
	Id:   1,
	Name: "John",
	Age:  30,
}
var u2 = TestUser{
	Id:   2,
	Name: "Joe",
	Age:  18,
}

func NewMock() (*sql.DB, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	if err != nil {
		log.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	return db, mock
}

func PrepareQueryData(mock sqlmock.Sqlmock, query string, users []TestUser, id int) {
	rows := sqlmock.NewRows([]string{"id", "name", "age"})
	for i := range users {
		rows.AddRow(users[i].Id, users[i].Name, users[i].Age)
	}
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(id).WillReturnRows(rows)
}

func PrepareInsert(mock sqlmock.Sqlmock) {
	mock.ExpectExec("insert into users").WithArgs(u1.Id, u1.Name, u1.Age).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into users").WithArgs(u2.Id, u2.Name, u2.Age).WillReturnResult(sqlmock.NewResult(2, 1))
}

func TestMain(m *testing.M) {
	DefaultConfig.PrintSql = false
	DefaultConfig.Mark = MysqlMark
	m.Run()
}

func TestQueryRow(t *testing.T) {
	db, mock := NewMock()
	defer db.Close()
	query := "select id, name, age from users where id = ?"
	PrepareQueryData(mock, query, []TestUser{u1}, u1.Id)

	var user TestUser
	err := QueryRowContext(db, context.Background(), query, &user, u1.Id)
	if err != nil {
		t.Fatalf("QueryRow error: %s", err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expectations: %s", err)
	}
	if user != u1 {
		t.Fatalf("user not equal, %v, %v", user, u1)
	}
}

func TestQuery(t *testing.T) {
	db, mock := NewMock()
	defer db.Close()
	query := "select * from users where id=?"
	PrepareQueryData(mock, query, []TestUser{u1}, u1.Id)

	ctx := context.Background()

	users1, err := QueryContext[*TestUser](db, ctx, query, u1.Id)
	if err != nil {
		t.Fatalf("QueryContext error: %s", err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expectations: %s", err)
	}
	if len(users1) != 1 {
		t.Fatalf("QueryContext error: len(users1) != 1")
	}
	if *users1[0] != u1 {
		t.Fatalf("QueryContext error: *users1[0] != *u")
	}
}

func TestTxQuery(t *testing.T) {
	db, mock := NewMock()
	defer db.Close()
	query := "select * from users where id=? for update"
	mock.ExpectBegin()
	PrepareQueryData(mock, query, []TestUser{u1}, u1.Id)
	mock.ExpectCommit()

	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx error: %s", err)
	}
	defer tx.Rollback()
	users1, err := QueryContext[*TestUser](tx, ctx, query, u1.Id)
	if err != nil {
		t.Fatalf("QueryContext error: %s", err)
	}
	_ = tx.Commit()

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expectations: %s", err)
	}
	if len(users1) != 1 {
		t.Fatalf("QueryContext error: len(users1) != 1")
	}
	if *users1[0] != u1 {
		t.Fatalf("QueryContext error: *users1[0] != *u")
	}
}

func TestInsert(t *testing.T) {
	db, mock := NewMock()
	PrepareInsert(mock)

	ctx := context.Background()

	db.Driver()
	_, err := InsertContext(db, ctx, &u1)
	if err != nil {
		t.Fatalf("InsertContext error: %s", err)
	}
	_, err = InsertContext(db, ctx, &u2)
	if err != nil {
		t.Fatalf("InsertContext error: %s", err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expectations: %s", err)
	}

}

func TestTxInsert(t *testing.T) {
	db, mock := NewMock()
	mock.ExpectBegin()
	PrepareInsert(mock)
	mock.ExpectCommit()

	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx error: %s", err)
	}
	_, err = InsertContext(tx, ctx, &u1)
	if err != nil {
		t.Fatalf("InsertContext error: %s", err)
	}
	_, err = InsertContext(tx, ctx, &u2)
	if err != nil {
		t.Fatalf("InsertContext error: %s", err)
	}
	tx.Commit()

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expectations: %s", err)
	}
}

func mockBulkInsert(t *testing.T, db executable, mock sqlmock.Sqlmock, bulkSize, listSize int,
	prepare func(sqlmock.Sqlmock) *sqlmock.ExpectedPrepare) []*TestUser {
	users := make([]*TestUser, listSize)
	var stmt *sqlmock.ExpectedPrepare
	var useStmt bool
	if len(users)/bulkSize >= 2 {
		useStmt = true
		stmt = prepare(mock)
	}
	for i := 0; i < len(users); i += bulkSize {
		args := make([]driver.Value, 0)
		end := i + bulkSize
		if end > len(users) {
			end = len(users)
			useStmt = false
		}
		for j := i; j < end; j++ {
			_user := TestUser{
				Id:   j,
				Name: "Joe",
				Age:  18,
			}
			users[j] = &_user
			args = append(args, _user.Id, _user.Name, _user.Age)
		}
		if useStmt {
			stmt.ExpectExec().WithArgs(args...).WillReturnResult(sqlmock.NewResult(int64(end-1), int64(end-i)))
		} else {
			mock.ExpectExec("insert into users").WithArgs(args...).WillReturnResult(sqlmock.NewResult(int64(end-1), int64(end-i)))
		}
	}

	return users
}

func testBulkInsert(t *testing.T, db executable, ctx context.Context, bulkSize int, users []*TestUser) int64 {
	total, err := BulkInsertContext(db, ctx, bulkSize, users...)
	if err != nil {
		t.Fatalf("BulkInsertContext error: %s", err)
	}

	return total
}

func validateInsertResult(t *testing.T, mock sqlmock.Sqlmock, total int64, users []*TestUser) {
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expectations: %s", err)
	}

	if total != int64(len(users)) {
		t.Fatalf("BulkInsertContext error: total != len(users), total=%d, len(users)=%d", total, len(users))
	}
}

var prepare = func(mock sqlmock.Sqlmock) *sqlmock.ExpectedPrepare {
	return mock.ExpectPrepare("insert into users")
}
var notPrepare = func(mock sqlmock.Sqlmock) *sqlmock.ExpectedPrepare { return nil }

func TestBulkInsertPrepared(t *testing.T) {
	db, mock := NewMock()
	bulkSize, listSize := 10, 100
	users := mockBulkInsert(t, db, mock, bulkSize, listSize, prepare)

	ctx := context.Background()
	total := testBulkInsert(t, db, ctx, bulkSize, users)

	validateInsertResult(t, mock, total, users)
}

func TestBulkInsertNotPrepared(t *testing.T) {
	db, mock := NewMock()
	bulkSize, listSize := 51, 100
	users := mockBulkInsert(t, db, mock, bulkSize, listSize, notPrepare)

	ctx := context.Background()
	total := testBulkInsert(t, db, ctx, bulkSize, users)

	validateInsertResult(t, mock, total, users)
}

func TestTxBulkInsert(t *testing.T) {
	db, mock := NewMock()
	bulkSize, listSize := 1000, 2001
	mock.ExpectBegin()
	users := mockBulkInsert(t, db, mock, bulkSize, listSize, prepare)
	mock.ExpectCommit()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	total := testBulkInsert(t, db, ctx, bulkSize, users)
	tx.Commit()

	validateInsertResult(t, mock, total, users)
}

func TestSessionBulkInsert(t *testing.T) {
	db, mock := NewMock()
	bulkSize, listSize := 1000, 2001
	mock.ExpectBegin()
	users := mockBulkInsert(t, db, mock, bulkSize, listSize, prepare)
	mock.ExpectCommit()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	total := testBulkInsert(t, db, ctx, bulkSize, users)
	tx.Commit()
	conn.Close()

	validateInsertResult(t, mock, total, users)

}

func TestScanListFromZeroLen(t *testing.T) {
	db, mock := NewMock()
	query := "select id, name, age from users where id=?"
	PrepareQueryData(mock, query, []TestUser{u1}, u1.Id)

	var list []*TestUser
	rows, err := db.Query(query, u1.Id)
	if err != nil {
		t.Fatalf("db.Query error: %s", err)
	}
	ScanList(rows, &list)

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expectations: %s", err)
	}
	if len(list) != 1 && *list[0] == u1 {
		t.Fatalf("len(list) after ScanList got %d, expected %d", len(list), 1)
	}
}

func TestScanListFromOneLen(t *testing.T) {
	db, mock := NewMock()
	query := "select id, name, age from users where id=?"
	PrepareQueryData(mock, query, []TestUser{u1}, u1.Id)

	list := make([]*TestUser, 1)
	rows, err := db.Query(query, u1.Id)
	if err != nil {
		t.Fatalf("db.Query error: %s", err)
	}
	ScanList(rows, &list)

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expectations: %s", err)
	}
	if len(list) != 1 && *list[0] == u1 {
		t.Fatalf("len(list) after ScanList got %d, expected %d", len(list), 1)
	}
}

func TestScanListWithCreateT(t *testing.T) {
	db, mock := NewMock()
	query := "select id, name, age from users where id=?"
	PrepareQueryData(mock, query, []TestUser{u1}, u1.Id)

	list := make([]*TestUser, 0)
	rows, err := db.Query(query, u1.Id)
	if err != nil {
		t.Fatalf("db.Query error: %s", err)
	}
	ScanList(rows, &list)

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expectations: %s", err)
	}
	if len(list) != 1 && *list[0] == u1 {
		t.Fatalf("len(list) after ScanList got %d, expected %d", len(list), 1)
	}
}

func TestNewTStructPointer(t *testing.T) {
	user := &TestUser{}
	newUser := newT[*TestUser]()

	if *user != *newUser {
		t.Fatalf("newTStructPointer error, got %v, expected %v", *newUser, *user)
	}
}

type MyTestInt int

func (i *MyTestInt) Args() []any {
	return []any{i}
}

func TestNewTNotStructTypePointer(t *testing.T) {

	i := MyTestInt(0)
	newI := newT[*MyTestInt]()

	if i != *newI {
		t.Fatalf("newTStructPointer error, got %v, expected %v", newI, i)
	}
}

func BenchmarkNormalInsert(b *testing.B) {
	b.ReportAllocs()
	db, mock := NewMock()
	defer db.Close()

	for i := 0; i < b.N; i++ {

		user := TestUser{Id: i}

		mock.ExpectExec("insert into users").WithArgs(user.Id, user.Name, user.Age).WillReturnResult(sqlmock.NewResult(int64(user.Id), 1))

		ctx := context.Background()

		r, err := db.ExecContext(ctx, "insert into users (id, name, age) VALUES (?, ?, ?)", user.Id, user.Name, user.Age)
		if err != nil {
			log.Fatal(err)
		}
		ra, _ := r.RowsAffected()
		if ra != 1 {
			log.Fatal(ra)
		}
	}
}

func BenchmarkGenericInsert(b *testing.B) {
	b.ReportAllocs()
	db, mock := NewMock()
	defer db.Close()
	for i := 0; i < b.N; i++ {

		user := &TestUser{Id: i}
		mock.ExpectExec("insert into users").WithArgs(user.Id, user.Name, user.Age).WillReturnResult(sqlmock.NewResult(int64(user.Id), 1))

		ctx := context.Background()

		r, err := InsertContext(db, ctx, user)
		if err != nil {
			log.Fatal(err)
		}
		if r != 1 {
			log.Fatal(r)
		}
	}
}

func BenchmarkNormalQuery(b *testing.B) {
	b.ReportAllocs()
	db, mock := NewMock()
	defer db.Close()
	query := "select * from users where id=?"
	for i := 0; i < b.N; i++ {

		PrepareQueryData(mock, query, []TestUser{u1}, u1.Id)

		ctx := context.Background()

		rows, err := db.QueryContext(ctx, query, u1.Id)
		if err != nil {
			log.Fatal(err)
		}
		users := make([]*TestUser, 1)
		for rows.Next() {
			user := new(TestUser)
			rows.Scan(&user.Id, &user.Name, &user.Age)
			users[0] = user
		}
		rows.Close()
		if len(users) != 1 {
			log.Fatal(len(users))
		}
	}
}

func BenchmarkGenericQuery(b *testing.B) {
	b.ReportAllocs()
	db, mock := NewMock()
	defer db.Close()
	query := "select * from users where id=?"
	for i := 0; i < b.N; i++ {
		PrepareQueryData(mock, query, []TestUser{u1}, u1.Id)

		ctx := context.Background()
		users, err := QueryContext[*TestUser](db, ctx, query, u1.Id)
		if err != nil {
			log.Fatal(err)
		}
		if len(users) != 1 {
			log.Fatal(len(users))
		}
	}
}

func newUser() *TestUser {
	return new(TestUser)
}

func BenchmarkNew(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		newUser()
	}
}

func BenchmarkNewTPointer(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		newT[*TestUser]()
	}
}

func BenchmarkNewTNonPointer(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		newT[TestUser]()
	}
}

var benchuser = TestUser{}
var typ = reflect.TypeOf(benchuser)

func reflectNew() *TestUser {
	return reflect.New(typ).Interface().(*TestUser)
}

func BenchmarkReflectNew(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reflectNew()
	}
}
