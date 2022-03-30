package dbh

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"log"
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

var u1 = &TestUser{
	Id:   1,
	Name: "John",
	Age:  30,
}
var u2 = &TestUser{
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

func PrepareQueryData(mock sqlmock.Sqlmock, query string) {
	rows1 := mock.NewRows([]string{"id", "name", "age"}).AddRow(u1.Id, u1.Name, u1.Age)
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(u1.Id).WillReturnRows(rows1)
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
	PrepareQueryData(mock, query)

	user, err := QueryRowContext[*TestUser](db, context.Background(), query, u1.Id)
	if err != nil {
		t.Fatalf("QueryRow error: %s", err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expectations: %s", err)
	}
	if *user != *u1 {
		t.Fatalf("user not equal, %v, %v", *user, *u1)
	}
}

func TestQuery(t *testing.T) {
	db, mock := NewMock()
	defer db.Close()
	query := "select * from users where id=?"
	PrepareQueryData(mock, query)

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
	if *users1[0] != *u1 {
		t.Fatalf("QueryContext error: *users1[0] != *u")
	}
}

func TestTxQuery(t *testing.T) {
	db, mock := NewMock()
	defer db.Close()
	query := "select * from users where id=? for update"
	mock.ExpectBegin()
	PrepareQueryData(mock, query)
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
	if *users1[0] != *u1 {
		t.Fatalf("QueryContext error: *users1[0] != *u")
	}
}

func TestInsert(t *testing.T) {
	db, mock := NewMock()
	PrepareInsert(mock)

	ctx := context.Background()

	db.Driver()
	_, err := InsertContext(db, ctx, u1)
	if err != nil {
		t.Fatalf("InsertContext error: %s", err)
	}
	_, err = InsertContext(db, ctx, u2)
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
	_, err = InsertContext(tx, ctx, u1)
	if err != nil {
		t.Fatalf("InsertContext error: %s", err)
	}
	_, err = InsertContext(tx, ctx, u2)
	if err != nil {
		t.Fatalf("InsertContext error: %s", err)
	}
	tx.Commit()

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expectations: %s", err)
	}
}

func TestBulkInsert(t *testing.T) {
	db, mock := NewMock()
	users := make([]*TestUser, 2001)
	bulkSize := 1000
	for i := 0; i < len(users); i += bulkSize {
		args := make([]driver.Value, 0)
		end := i + bulkSize
		if end > len(users) {
			end = len(users)
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

		mock.ExpectExec("insert into users").WithArgs(args...).WillReturnResult(sqlmock.NewResult(int64(end-1), int64(end-i)))
	}

	ctx := context.Background()

	total, err := BulkInsertContext(db, ctx, bulkSize, users...)
	if err != nil {
		t.Fatalf("BulkInsertContext error: %s", err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expectations: %s", err)
	}

	if total != int64(len(users)) {
		t.Fatalf("BulkInsertContext error: total != len(users), total=%d, len(users)=%d", total, len(users))
	}
}

func TestTxBulkInsert(t *testing.T) {
	db, mock := NewMock()
	mock.ExpectBegin()
	users := make([]*TestUser, 2001)
	bulkSize := 1000
	for i := 0; i < len(users); i += bulkSize {
		args := make([]driver.Value, 0)
		end := i + bulkSize
		if end > len(users) {
			end = len(users)
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

		mock.ExpectExec("insert into users").WithArgs(args...).WillReturnResult(sqlmock.NewResult(int64(end-1), int64(end-i)))
	}
	mock.ExpectCommit()

	ctx := context.Background()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("tx error, %s", err)
	}
	total, err := BulkInsertContext(tx, ctx, bulkSize, users...)
	if err != nil {
		t.Fatalf("BulkInsertContext error: %s", err)
	}
	_ = tx.Commit()

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expectations: %s", err)
	}

	if total != int64(len(users)) {
		t.Fatalf("BulkInsertContext error: total != len(users), total=%d, len(users)=%d", total, len(users))
	}
}

func TestBulkInsertUseStmtExact(t *testing.T) {
	db, mock := NewMock()
	userLen := 100
	bulkSize := 20
	stmtThreshold := 4
	users := make([]*TestUser, userLen)

	mockPrepare := mock.ExpectPrepare("insert into users")
	for i := 0; i < userLen; i += bulkSize {
		args := make([]driver.Value, 0)
		end := i + bulkSize
		for j := i; j < end; j++ {
			_user := TestUser{
				Id:   j,
				Name: "Joe",
				Age:  18,
			}
			users[j] = &_user
			args = append(args, _user.Id, _user.Name, _user.Age)
		}
		mockPrepare.ExpectExec().WithArgs(args...).WillReturnResult(sqlmock.NewResult(int64(end-1), int64(end-i)))
	}

	DefaultConfig.BulkInsertStmtThreshold = stmtThreshold
	total, err := BulkInsert(db, bulkSize, users...)

	if err != nil {
		t.Fatalf("BulkInsert error: %s", err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expectations: %s", err)
	}
	if total != int64(len(users)) {
		t.Fatalf("BulkInsert error: total != len(users), total=%d, len(users)=%d", total, len(users))
	}

}

func TestBulkInsertUseStmtEdge(t *testing.T) {
	db, mock := NewMock()
	userLen := 101
	bulkSize := 20
	stmtThreshold := 4
	users := make([]*TestUser, userLen)

	mockPrepare := mock.ExpectPrepare("insert into users")
	useStmt := userLen/bulkSize > stmtThreshold
	for i := 0; i < userLen; i += bulkSize {
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
			mockPrepare.ExpectExec().WithArgs(args...).WillReturnResult(sqlmock.NewResult(int64(end-1), int64(end-i)))
		} else {
			mock.ExpectExec("insert into users").WithArgs(args...).WillReturnResult(sqlmock.NewResult(int64(end-1), int64(end-i)))
		}

	}

	DefaultConfig.BulkInsertStmtThreshold = stmtThreshold
	total, err := BulkInsert(db, bulkSize, users...)

	if err != nil {
		t.Fatalf("BulkInsert error: %s", err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expectations: %s", err)
	}
	if total != int64(len(users)) {
		t.Fatalf("BulkInsert error: total != len(users), total=%d, len(users)=%d", total, len(users))
	}
}

func TestScanListFromZeroLen(t *testing.T) {
	db, mock := NewMock()
	query := "select id, name, age from users where id=?"
	PrepareQueryData(mock, query)

	var list []*TestUser
	rows, err := db.Query(query, u1.Id)
	if err != nil {
		t.Fatalf("db.Query error: %s", err)
	}
	ScanList(rows, &list)

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expectations: %s", err)
	}
	if len(list) != 1 {
		t.Fatalf("len(list) after ScanList got %d, expected %d", len(list), 1)
	}
}

func TestScanListFromOneLen(t *testing.T) {
	db, mock := NewMock()
	query := "select id, name, age from users where id=?"
	PrepareQueryData(mock, query)

	list := make([]*TestUser, 1)
	rows, err := db.Query(query, u1.Id)
	if err != nil {
		t.Fatalf("db.Query error: %s", err)
	}
	ScanList(rows, &list)

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expectations: %s", err)
	}
	if len(list) != 1 {
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
