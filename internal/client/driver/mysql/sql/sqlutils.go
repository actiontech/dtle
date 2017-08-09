package sql

import (
	gosql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
	"strings"
	"sync"
)

// RowMap represents one row in a result set. Its objective is to allow
// for easy, typed getters by column name.
type RowMap map[string]CellData

// Cell data is the result of a single (atomic) column in a single row
type CellData gosql.NullString

func (this *CellData) MarshalJSON() ([]byte, error) {
	if this.Valid {
		return json.Marshal(this.String)
	} else {
		return json.Marshal(nil)
	}
}

func (this *CellData) NullString() *gosql.NullString {
	return (*gosql.NullString)(this)
}

// RowData is the result of a single row, in positioned array format
type RowData []CellData

// MarshalJSON will marshal this map as JSON
func (this *RowData) MarshalJSON() ([]byte, error) {
	cells := make([](*CellData), len(*this), len(*this))
	for i, val := range *this {
		d := CellData(val)
		cells[i] = &d
	}
	return json.Marshal(cells)
}

// ResultData is an ordered row set of RowData
type ResultData []RowData

var EmptyResultData = ResultData{}

func (this *RowMap) GetString(key string) string {
	return (*this)[key].String
}

// GetStringD returns a string from the map, or a default value if the key does not exist
func (this *RowMap) GetStringD(key string, def string) string {
	if cell, ok := (*this)[key]; ok {
		return cell.String
	}
	return def
}

func (this *RowMap) GetInt64(key string) int64 {
	res, _ := strconv.ParseInt(this.GetString(key), 10, 0)
	return res
}

func (this *RowMap) GetNullInt64(key string) gosql.NullInt64 {
	i, err := strconv.ParseInt(this.GetString(key), 10, 0)
	if err == nil {
		return gosql.NullInt64{Int64: i, Valid: true}
	} else {
		return gosql.NullInt64{Valid: false}
	}
}

func (this *RowMap) GetInt(key string) int {
	res, _ := strconv.Atoi(this.GetString(key))
	return res
}

func (this *RowMap) GetIntD(key string, def int) int {
	res, err := strconv.Atoi(this.GetString(key))
	if err != nil {
		return def
	}
	return res
}

func (this *RowMap) GetUint(key string) uint {
	res, _ := strconv.Atoi(this.GetString(key))
	return uint(res)
}

func (this *RowMap) GetUintD(key string, def uint) uint {
	res, err := strconv.Atoi(this.GetString(key))
	if err != nil {
		return def
	}
	return uint(res)
}

func (this *RowMap) GetBool(key string) bool {
	return this.GetInt(key) != 0
}

// knownDBs is a DB cache by uri
var knownDBs map[string]*gosql.DB = make(map[string]*gosql.DB)
var knownDBsMutex = &sync.Mutex{}

// GetDB returns a DB instance based on uri.
// bool result indicates whether the DB was returned from cache; err
func GetDB(mysql_uri string) (*gosql.DB, bool, error) {
	knownDBsMutex.Lock()
	defer func() {
		knownDBsMutex.Unlock()
	}()

	var exists bool
	if _, exists = knownDBs[mysql_uri]; !exists {
		if db, err := gosql.Open("mysql", mysql_uri); err == nil {
			knownDBs[mysql_uri] = db
		} else {
			return db, exists, err
		}
	}
	return knownDBs[mysql_uri], exists, nil
}

type DB struct {
	Db      *gosql.DB
	Fde     string
}

func CreateDB(mysql_uri string) (*gosql.DB, error) {
	db, err := gosql.Open("mysql", mysql_uri)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func CreateDBs(mysql_uri string, count int) ([]*DB, error) {
	dbs := make([]*DB, 0, count)
	for i := 0; i < count; i++ {
		db, err := CreateDB(mysql_uri)
		if err != nil {
			return nil, err
		}
		dbApplier := &DB{
			DbMutex: &sync.Mutex{},
			Db:      db,
		}
		dbs = append(dbs, dbApplier)
	}

	return dbs, nil
}

// RowToArray is a convenience function, typically not called directly, which maps a
// single read database row into a NullString
func RowToArray(rows *gosql.Rows, columns []string) []CellData {
	buff := make([]interface{}, len(columns))
	data := make([]CellData, len(columns))
	for i := range buff {
		buff[i] = data[i].NullString()
	}
	rows.Scan(buff...)
	return data
}

// ScanRowsToArrays is a convenience function, typically not called directly, which maps rows
// already read from the databse into arrays of NullString
func ScanRowsToArrays(rows *gosql.Rows, on_row func([]CellData) error) error {
	columns, _ := rows.Columns()
	for rows.Next() {
		arr := RowToArray(rows, columns)
		err := on_row(arr)
		if err != nil {
			return err
		}
	}
	return nil
}

func rowToMap(row []CellData, columns []string) map[string]CellData {
	m := make(map[string]CellData)
	for k, data_col := range row {
		m[columns[k]] = data_col
	}
	return m
}

// ScanRowsToMaps is a convenience function, typically not called directly, which maps rows
// already read from the databse into RowMap entries.
func ScanRowsToMaps(rows *gosql.Rows, on_row func(RowMap) error) error {
	columns, _ := rows.Columns()
	err := ScanRowsToArrays(rows, func(arr []CellData) error {
		m := rowToMap(arr, columns)
		err := on_row(m)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

// QueryRowsMap is a convenience function allowing querying a result set while poviding a callback
// function activated per read row.
func QueryRowsMap(db *gosql.DB, query string, on_row func(RowMap) error, args ...interface{}) error {
	var err error
	defer func() {
		if derr := recover(); derr != nil {
			err = errors.New(fmt.Sprintf("QueryRowsMap unexpected error: %+v", derr))
		}
	}()

	rows, err := db.Query(query, args...)
	defer rows.Close()
	if err != nil && err != gosql.ErrNoRows {
		return err
	}
	err = ScanRowsToMaps(rows, on_row)
	return err
}

func QueryRowsMapWithTx(db *gosql.Tx, query string, on_row func(RowMap) error, args ...interface{}) error {
	var err error
	defer func() {
		if derr := recover(); derr != nil {
			err = errors.New(fmt.Sprintf("QueryRowsMap unexpected error: %+v", derr))
		}
	}()

	rows, err := db.Query(query, args...)
	defer rows.Close()
	if err != nil && err != gosql.ErrNoRows {
		return err
	}
	err = ScanRowsToMaps(rows, on_row)
	return err
}

// queryResultData returns a raw array of rows for a given query, optionally reading and returning column names
func queryResultData(db *gosql.DB, query string, retrieveColumns bool, args ...interface{}) (ResultData, []string, error) {
	var err error
	defer func() {
		if derr := recover(); derr != nil {
			err = errors.New(fmt.Sprintf("QueryRowsMap unexpected error: %+v", derr))
		}
	}()

	columns := []string{}
	rows, err := db.Query(query, args...)
	defer rows.Close()
	if err != nil && err != gosql.ErrNoRows {
		return EmptyResultData, columns, err
	}
	if retrieveColumns {
		// Don't pay if you don't want to
		columns, _ = rows.Columns()
	}
	resultData := ResultData{}
	err = ScanRowsToArrays(rows, func(rowData []CellData) error {
		resultData = append(resultData, rowData)
		return nil
	})
	return resultData, columns, err
}

// QueryResultData returns a raw array of rows
func QueryResultData(db *gosql.DB, query string, args ...interface{}) (ResultData, error) {
	resultData, _, err := queryResultData(db, query, false, args...)
	return resultData, err
}

// QueryResultDataNamed returns a raw array of rows, with column names
func QueryResultDataNamed(db *gosql.DB, query string, args ...interface{}) (ResultData, []string, error) {
	return queryResultData(db, query, true, args...)
}

// QueryRowsMapBuffered reads data from the database into a buffer, and only then applies the given function per row.
// This allows the application to take its time with processing the data, albeit consuming as much memory as required by
// the result set.
func QueryRowsMapBuffered(db *gosql.DB, query string, on_row func(RowMap) error, args ...interface{}) error {
	resultData, columns, err := queryResultData(db, query, true, args...)
	if err != nil {
		// Already logged
		return err
	}
	for _, row := range resultData {
		err = on_row(rowToMap(row, columns))
		if err != nil {
			return err
		}
	}
	return nil
}

// ExecNoPrepare executes given query using given args on given DB, without using prepared statements.
func ExecNoPrepare(db *gosql.DB, query string, args ...interface{}) (gosql.Result, error) {
	var err error
	defer func() {
		if derr := recover(); derr != nil {
			err = errors.New(fmt.Sprintf("ExecNoPrepare unexpected error: %+v", derr))
		}
	}()

	var res gosql.Result
	res, err = db.Exec(query, args...)
	return res, err
}

// ExecQuery executes given query using given args on given DB. It will safele prepare, execute and close
// the statement.
func execInternal(silent bool, db *gosql.DB, query string, args ...interface{}) (gosql.Result, error) {
	var err error
	defer func() {
		if derr := recover(); derr != nil {
			err = errors.New(fmt.Sprintf("execInternal unexpected error: %+v", derr))
		}
	}()

	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	var res gosql.Result
	res, err = stmt.Exec(args...)
	return res, err
}

// Exec executes given query using given args on given DB. It will safele prepare, execute and close
// the statement.
func Exec(db *gosql.DB, query string, args ...interface{}) (gosql.Result, error) {
	return execInternal(false, db, query, args...)
}

// ExecSilently acts like Exec but does not report any error
func ExecSilently(db *gosql.DB, query string, args ...interface{}) (gosql.Result, error) {
	return execInternal(true, db, query, args...)
}

func InClauseStringValues(terms []string) string {
	quoted := []string{}
	for _, s := range terms {
		quoted = append(quoted, fmt.Sprintf("'%s'", strings.Replace(s, ",", "''", -1)))
	}
	return strings.Join(quoted, ", ")
}

// Convert variable length arguments into arguments array
func Args(args ...interface{}) []interface{} {
	return args
}

func CloseDB(db *gosql.DB) error {
	if db == nil {
		return nil
	}

	err := db.Close()
	if err != nil {
		return err
	}

	return nil
}

func CloseDBs(dbs ...*DB) error {
	for _, db := range dbs {
		err := CloseDB(db.Db)
		if err != nil {
			return fmt.Errorf("close db failed - %v", err)
		}
	}
	return nil
}
