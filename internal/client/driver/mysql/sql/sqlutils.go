/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package sql

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"github.com/actiontech/dtle/internal/config"

	_ "github.com/go-sql-driver/mysql"
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

type Conn struct {
	DbMutex *sync.Mutex
	Db      *gosql.Conn
	Fde     string

	CurrentSchema        string
	PsDeleteExecutedGtid *gosql.Stmt
	PsInsertExecutedGtid *gosql.Stmt
}

type DB struct {
	DbMutex *sync.Mutex
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

func CreateConns(db *gosql.DB, count int) ([]*Conn, error) {
	conns := make([]*Conn, count)
	for i := 0; i < count; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			return nil, err
		}

		_, err = conn.ExecContext(context.Background(), "SET @@session.foreign_key_checks = 0")
		if err != nil {
			return nil, err
		}

		conns[i] = &Conn{
			DbMutex: &sync.Mutex{},
			Db:      conn,
		}
	}
	return conns, nil
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
func QueryRowsMap(db QueryAble, query string, on_row func(RowMap) error, args ...interface{}) error {
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

// from https://github.com/golang/go/issues/14468
type QueryAble interface {
	Exec(query string, args ...interface{}) (gosql.Result, error)
	Prepare(query string) (*gosql.Stmt, error)
	Query(query string, args ...interface{}) (*gosql.Rows, error)
	QueryRow(query string, args ...interface{}) *gosql.Row
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
func ExecNoPrepare(db *gosql.Conn, query string, args ...interface{}) (gosql.Result, error) {
	var err error
	defer func() {
		if derr := recover(); derr != nil {
			err = errors.New(fmt.Sprintf("ExecNoPrepare unexpected error: %+v", derr))
		}
	}()

	var res gosql.Result
	res, err = db.ExecContext(context.Background(), query, args...)
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

//LOCK TABLES {{ .Name }} WRITE;
//INSERT INTO {{ .Name }} VALUES {{ .Values }};
//UNLOCK TABLES;

func ShowDatabases(db *gosql.DB) ([]string, error) {
	dbs := make([]string, 0)

	// Get table list
	rows, err := db.Query("SHOW DATABASES")
	if err != nil {
		return dbs, err
	}
	defer rows.Close()

	// Read result
	for rows.Next() {
		var database gosql.NullString
		if err := rows.Scan(&database); err != nil {
			return dbs, err
		}
		switch strings.ToLower(database.String) {
		case "sys", "mysql", "information_schema", "performance_schema", "actiontech_udup":
			continue
		default:
			dbs = append(dbs, database.String)
		}
	}
	return dbs, rows.Err()
}

func ShowTables(db *gosql.DB, dbName string, showType bool) (tables []*config.Table, err error) {
	// Get table list
	var query string
	if showType {
		// TODO escape with backquote?
		query = fmt.Sprintf("SHOW FULL TABLES IN %s", dbName)
	} else {
		query = fmt.Sprintf("SHOW TABLES IN %s", dbName)
	}
	rows, err := db.Query(query)
	if err != nil {
		return tables, err
	}
	defer rows.Close()

	// Read result
	for rows.Next() {
		var table gosql.NullString
		var tableType gosql.NullString

		var err error
		if showType {
			err = rows.Scan(&table, &tableType)
		} else {
			err = rows.Scan(&table)
		}
		if err != nil {
			return tables, err
		}
		tb := &config.Table{TableSchema: dbName, TableName: table.String}
		if showType {
			tb.TableType = tableType.String
		}
		tables = append(tables, tb)
	}
	return tables, rows.Err()
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

func CloseConns(dbs ...*Conn) error {
	for _, db := range dbs {
		if db.Db != nil {
			err := db.Db.Close()
			if err != nil {
				return fmt.Errorf("close db failed - %v", err)
			}
		}
	}
	return nil
}
