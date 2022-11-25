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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/actiontech/dtle/driver/common"
	"github.com/actiontech/dtle/driver/mysql/mysqlconfig"

	"github.com/actiontech/dtle/g"
	_ "github.com/go-sql-driver/mysql"
)

const (
	ConnMaxLifetime = 300 * time.Second // #376
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

type Conn struct {
	DbMutex *sync.Mutex
	Db      *gosql.Conn

	PsDeleteExecutedGtid *gosql.Stmt
	PsInsertExecutedGtid *gosql.Stmt
	Tx                   *gosql.Tx
}

func CreateDB(mysql_uri string) (*gosql.DB, error) {
	db, err := gosql.Open("mysql", mysql_uri)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(ConnMaxLifetime)

	return db, nil
}

func CreateConns(ctx context.Context, db *gosql.DB, count int) ([]*Conn, error) {
	conns := make([]*Conn, count)
	for i := 0; i < count; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return nil, err
		}

		_, err = conn.ExecContext(ctx, "SET @@session.foreign_key_checks = 0")
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

func GetServerUUID(db QueryAble) (result string, err error) {
	err = db.QueryRow(`SELECT @@SERVER_UUID /*dtle*/`).Scan(&result)
	if err != nil {
		return "", err
	}
	return result, nil
}

func ShowMasterStatus(db QueryAble) *gosql.Row {
	return db.QueryRow("show master status /*dtle*/")
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
		case "sys", "mysql", "information_schema", "performance_schema", g.DtleSchemaName:
			continue
		default:
			dbs = append(dbs, database.String)
		}
	}
	return dbs, rows.Err()
}

func ShowCreateSchema(ctx context.Context, db *gosql.DB, dbName string) (r string, err error) {
	query := fmt.Sprintf("SHOW CREATE SCHEMA IF NOT EXISTS %s", mysqlconfig.EscapeName(dbName))
	g.Logger.Debug("ShowCreateSchema", "query", query)
	row := db.QueryRowContext(ctx, query)
	var dummy interface{}
	// | Database | Create Database |
	err = row.Scan(&dummy, &r)
	if err != nil {
		return "", errors.Wrap(err, "ShowCreateSchema")
	}
	return r, nil
}

func ShowTables(db *gosql.DB, dbName string, showType bool) (tables []*common.Table, err error) {
	// Get table list
	var query string
	escapedDbName := mysqlconfig.EscapeName(dbName)
	if showType {
		query = fmt.Sprintf("SHOW FULL TABLES IN %s", escapedDbName)
	} else {
		query = fmt.Sprintf("SHOW TABLES IN %s", escapedDbName)
	}
	g.Logger.Debug("ShowTables", "query", query)
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
		tb := &common.Table{TableSchema: dbName, TableName: table.String}
		if showType {
			tb.TableType = tableType.String
		}
		tables = append(tables, tb)
	}
	return tables, rows.Err()
}

func ListColumns(db *gosql.DB, dbName, tableName string) (columns []string, err error) {
	// Get table columns name
	query := fmt.Sprintf("select COLUMN_NAME from information_schema.columns where table_name='%s' and table_schema = '%s';", tableName, dbName)
	rows, err := db.Query(query)
	if err != nil {
		return columns, err
	}
	defer rows.Close()

	// Read result
	for rows.Next() {
		var column gosql.NullString
		err = rows.Scan(&column)
		if err != nil {
			return columns, err
		}
		columns = append(columns, column.String)
	}
	return columns, rows.Err()
}

func CloseDB(db *gosql.DB) error {
	if db == nil {
		return nil
	}

	return db.Close()
}

func CloseConns(dbs ...*Conn) error {
	for _, db := range dbs {
		if db.Db != nil {
			_ = db.Db.Close()
		}
	}
	return nil
}
