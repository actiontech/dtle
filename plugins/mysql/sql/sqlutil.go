package sql

import (
	"database/sql"
	"fmt"
	"github.com/ngaut/log"
	"sync"
	"time"

	uconf "udup/config"
)

var (
	maxRetryCount = 10
	retryTimeout  = 3 * time.Second
)

// RowMap represents one row in a result set. Its objective is to allow
// for easy, typed getters by column name.
type RowMap map[string]CellData

// Cell data is the result of a single (atomic) column in a single row
type CellData sql.NullString

func (this *CellData) NullString() *sql.NullString {
	return (*sql.NullString)(this)
}

// knownDBs is a DB cache by uri
var knownDBs map[string]*sql.DB = make(map[string]*sql.DB)
var knownDBsMutex = &sync.Mutex{}

// GetDB returns a DB instance based on uri.
// bool result indicates whether the DB was returned from cache; err
func GetDB(mysql_uri string) (*sql.DB, bool, error) {
	knownDBsMutex.Lock()
	defer func() {
		knownDBsMutex.Unlock()
	}()

	var exists bool
	if _, exists = knownDBs[mysql_uri]; !exists {
		if db, err := sql.Open("mysql", mysql_uri); err == nil {
			knownDBs[mysql_uri] = db
		} else {
			return db, exists, err
		}
	}
	return knownDBs[mysql_uri], exists, nil
}

// QueryRowsMap is a convenience function allowing querying a result set while poviding a callback
// function activated per read row.
func QueryRowsMap(db *sql.DB, query string, on_row func(RowMap) error, args ...interface{}) error {
	var err error
	defer func() {
		if derr := recover(); derr != nil {
			err = fmt.Errorf("QueryRowsMap unexpected error: %+v", derr)
		}
	}()

	rows, err := db.Query(query, args...)
	defer rows.Close()
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	err = ScanRowsToMaps(rows, on_row)
	return err
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
func ScanRowsToMaps(rows *sql.Rows, on_row func(RowMap) error) error {
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

// RowToArray is a convenience function, typically not called directly, which maps a
// single read database row into a NullString
func RowToArray(rows *sql.Rows, columns []string) []CellData {
	buff := make([]interface{}, len(columns))
	data := make([]CellData, len(columns))
	for i, _ := range buff {
		buff[i] = data[i].NullString()
	}
	rows.Scan(buff...)
	return data
}

// ScanRowsToArrays is a convenience function, typically not called directly, which maps rows
// already read from the databse into arrays of NullString
func ScanRowsToArrays(rows *sql.Rows, on_row func([]CellData) error) error {
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

// ExecNoPrepare executes given query using given args on given DB, without using prepared statements.
func ExecNoPrepare(db *sql.DB, query string, args ...interface{}) (sql.Result, error) {
	var err error
	defer func() {
		if derr := recover(); derr != nil {
			err = fmt.Errorf("ExecNoPrepare unexpected error: %+v", derr)
		}
	}()

	var res sql.Result
	res, err = db.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return res, err
}

func ExecuteSQL(db *sql.DB, sqls []string, args [][]interface{}, retry bool) error {
	if len(sqls) == 0 {
		return nil
	}

	var (
		err error
		txn *sql.Tx
	)

	retryCount := 1
	if retry {
		retryCount = maxRetryCount
	}

LOOP:
	for i := 0; i < retryCount; i++ {
		if i > 0 {
			log.Warnf("exec sql retry %d - %v - %v", i, sqls[0], args[0])
			time.Sleep(retryTimeout)
		}

		txn, err = db.Begin()
		if err != nil {
			log.Errorf("Failed to execute SQL :[%v] when begin tx，[error] :%v", sqls, err)
			continue
		}

		for i := range sqls {
			_, err = txn.Exec(sqls[i], args[i]...)
			if err != nil {
				log.Warnf("Failed to execute SQL :%s with args :%v,[error] :%v", sqls[i], args[i], err)
				rerr := txn.Rollback()
				if rerr != nil {
					log.Errorf("Failed to rollback SQL :%s with args :%v,[error] :%v", sqls[i], args[i], rerr)
				}
				continue LOOP
			}
		}

		err = txn.Commit()
		if err != nil {
			log.Errorf("Failed to commit SQL :[%v],[error] :%v", sqls, err)
			continue
		}

		return nil
	}

	return err
}

func QuerySQL(db *sql.DB, query string) (*sql.Rows, error) {
	var (
		err  error
		rows *sql.Rows
	)

	for i := 0; i < maxRetryCount; i++ {
		if i > 0 {
			log.Warnf("query sql retry %d - %s", i, query)
			time.Sleep(retryTimeout)
		}

		rows, err = db.Query(query)
		if err != nil {
			continue
		}

		return rows, nil
	}

	if err != nil {
		return nil, err
	}

	return nil, err
}

func closeDB(db *sql.DB) error {
	if db == nil {
		return nil
	}

	err := db.Close()
	if err != nil {
		log.Errorf("close db failed - %v", err)
		return err
	}

	return nil
}

func CreateDB(cfg *uconf.ConnectionConfig) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4,utf8,latin1&multiStatements=true", cfg.User, cfg.Password, cfg.Host, cfg.Port)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func CreateDBs(cfg *uconf.ConnectionConfig, count int) ([]*sql.DB, error) {
	dbs := make([]*sql.DB, 0, count)
	for i := 0; i < count; i++ {
		db, err := CreateDB(cfg)
		if err != nil {
			return nil, err
		}

		dbs = append(dbs, db)
	}

	return dbs, nil
}

func CloseDBs(dbs ...*sql.DB) error{
	for _, db := range dbs {
		err := closeDB(db)
		if err != nil {
			return fmt.Errorf("close db failed - %v", err)
		}
	}
	return nil
}
