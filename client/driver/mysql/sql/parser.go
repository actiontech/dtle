package sql

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/ast"
	tddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/terror"
)

var (
	maxRetryCount = 100

	retryTimeout = 3 * time.Second
	waitTime     = 10 * time.Millisecond
	maxWaitTime  = 3 * time.Second
	eventTimeout = 3 * time.Second
	statusTime   = 30 * time.Second
)

type Table struct {
	Schema string
	Name   string

	Columns      []*Column
	IndexColumns []*Column
}

type TableName struct {
	Schema string
	Name   string
}

func BuildDMLInsertQuery(schema string, table string, datas [][]interface{}, columns []*Column, indexColumns []*Column) ([]string, []string, [][]interface{}, error) {
	sqls := make([]string, 0, len(datas))
	keys := make([]string, 0, len(datas))
	values := make([][]interface{}, 0, len(datas))
	columnList := genColumnList(columns)
	columnPlaceholders := genColumnPlaceholders(len(columns))
	for _, data := range datas {
		if len(data) != len(columns) {
			return nil, nil, nil, fmt.Errorf("invalid columns and data - %d, %d", len(data), len(columns))
		}

		value := make([]interface{}, 0, len(data))
		for i := range data {
			value = append(value, castUnsigned(data[i], columns[i].Unsigned))
		}

		sql := fmt.Sprintf("replace into %s.%s (%s) values (%s);", schema, table, columnList, columnPlaceholders)
		sqls = append(sqls, sql)
		values = append(values, value)

		keyColumns, keyValues := getColumnDatas(columns, indexColumns, value)
		keys = append(keys, genKeyList(keyColumns, keyValues))
	}

	return sqls, keys, values, nil
}

func getColumnDatas(columns []*Column, indexColumns []*Column, data []interface{}) ([]*Column, []interface{}) {
	cols := make([]*Column, 0, len(columns))
	values := make([]interface{}, 0, len(columns))
	for _, column := range indexColumns {
		cols = append(cols, column)
		values = append(values, castUnsigned(data[column.Idx], column.Unsigned))
	}

	return cols, values
}

func genWhere(columns []*Column, data []interface{}) string {
	var kvs []byte
	for i := range columns {
		kvSplit := "="
		if data[i] == nil {
			kvSplit = "is"
		}

		if i == len(columns)-1 {
			kvs = append(kvs, []byte(fmt.Sprintf("%s %s ?", columns[i].Name, kvSplit))...)
		} else {
			kvs = append(kvs, []byte(fmt.Sprintf("%s %s ? and ", columns[i].Name, kvSplit))...)
		}
	}

	return string(kvs)
}

func genKVs(columns []*Column) string {
	var kvs []byte
	for i := range columns {
		if i == len(columns)-1 {
			kvs = append(kvs, []byte(fmt.Sprintf("%s = ?", columns[i].Name))...)
		} else {
			kvs = append(kvs, []byte(fmt.Sprintf("%s = ?, ", columns[i].Name))...)
		}
	}

	return string(kvs)
}

func BuildDMLUpdateQuery(schema string, table string, datas [][]interface{}, columns []*Column, indexColumns []*Column) ([]string, []string, [][]interface{}, error) {
	sqls := make([]string, 0, len(datas)/2)
	keys := make([]string, 0, len(datas)/2)
	values := make([][]interface{}, 0, len(datas)/2)
	for i := 0; i < len(datas); i += 2 {
		oldData := datas[i]
		newData := datas[i+1]
		if len(oldData) != len(newData) {
			return nil, nil, nil, fmt.Errorf("invalid update data - %d, %d", len(oldData), len(newData))
		}

		if len(oldData) != len(columns) {
			return nil, nil, nil, fmt.Errorf("invalid columns and data - %d, %d", len(oldData), len(columns))
		}

		oldValues := make([]interface{}, 0, len(oldData))
		newValues := make([]interface{}, 0, len(newData))
		updateColumns := make([]*Column, 0, len(indexColumns))

		for j := range oldData {
			if reflect.DeepEqual(oldData[j], newData[j]) {
				continue
			}

			updateColumns = append(updateColumns, columns[j])
			oldValues = append(oldValues, castUnsigned(oldData[j], columns[j].Unsigned))
			newValues = append(newValues, castUnsigned(newData[j], columns[j].Unsigned))
		}

		value := make([]interface{}, 0, len(oldData))
		kvs := genKVs(updateColumns)
		value = append(value, newValues...)

		whereColumns, whereValues := updateColumns, oldValues
		if len(indexColumns) > 0 {
			whereColumns, whereValues = getColumnDatas(columns, indexColumns, oldData)
		}

		where := genWhere(whereColumns, whereValues)
		value = append(value, whereValues...)

		sql := fmt.Sprintf("update %s.%s set %s where %s limit 1;", schema, table, kvs, where)
		sqls = append(sqls, sql)
		values = append(values, value)

		keys = append(keys, genKeyList(whereColumns, whereValues))
	}

	return sqls, keys, values, nil
}

func BuildDMLDeleteQuery(schema string, table string, datas [][]interface{}, columns []*Column, indexColumns []*Column) ([]string, []string, [][]interface{}, error) {
	sqls := make([]string, 0, len(datas))
	keys := make([]string, 0, len(datas))
	values := make([][]interface{}, 0, len(datas))
	for _, data := range datas {
		if len(data) != len(columns) {
			return nil, nil, nil, fmt.Errorf("invalid columns and data - %d, %d", len(data), len(columns))
		}

		value := make([]interface{}, 0, len(data))
		for i := range data {
			value = append(value, castUnsigned(data[i], columns[i].Unsigned))
		}

		whereColumns, whereValues := columns, value
		if len(indexColumns) > 0 {
			whereColumns, whereValues = getColumnDatas(columns, indexColumns, value)
		}

		where := genWhere(whereColumns, whereValues)
		values = append(values, whereValues)

		sql := fmt.Sprintf("delete from %s.%s where %s limit 1;", schema, table, where)
		sqls = append(sqls, sql)
		keys = append(keys, genKeyList(whereColumns, whereValues))
	}

	return sqls, keys, values, nil
}

func ignoreDDLError(err error) bool {
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}

	errCode := terror.ErrCode(mysqlErr.Number)
	switch errCode {
	case infoschema.ErrDatabaseExists.Code(), infoschema.ErrDatabaseNotExists.Code(), infoschema.ErrDatabaseDropExists.Code(),
		infoschema.ErrTableExists.Code(), infoschema.ErrTableNotExists.Code(), infoschema.ErrTableDropExists.Code(),
		infoschema.ErrColumnExists.Code(), infoschema.ErrColumnNotExists.Code(),
		infoschema.ErrIndexExists.Code(), tddl.ErrCantDropFieldOrKey.Code():
		return true
	default:
		return false
	}
}

func isDDLSQL(sql string) (bool, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return false, fmt.Errorf("[sql]%s[error]%v", sql, err)
	}

	_, isDDL := stmt.(ast.DDLNode)
	return isDDL, nil
}

// resolveDDLSQL resolve to one ddl sql
// example: drop table test.a,test2.b -> drop table test.a; drop table test2.b;
func ResolveDDLSQL(sql string) (sqls []string, ok bool, err error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, false, err
	}

	_, isDDL := stmt.(ast.DDLNode)
	if !isDDL {
		sqls = append(sqls, sql)
		return
	}

	switch v := stmt.(type) {
	case *ast.DropTableStmt:
		var ex string
		if v.IfExists {
			ex = "if exists"
		}
		for _, t := range v.Tables {
			var db string
			if t.Schema.O != "" {
				db = fmt.Sprintf("%s.", t.Schema.O)
			}
			s := fmt.Sprintf("drop table %s %s%s", ex, db, t.Name.L)
			sqls = append(sqls, s)
		}

	default:
		sqls = append(sqls, sql)
	}
	return sqls, true, nil
}

func BuildDDLSQL(sql string, schema string) (string, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return "", err
	}
	_, isCreateDatabase := stmt.(*ast.CreateDatabaseStmt)
	if isCreateDatabase {
		return fmt.Sprintf("%s;", sql), nil
	}
	if schema == "" {
		return fmt.Sprintf("%s;", sql), nil
	}

	return fmt.Sprintf("use %s; %s;", schema, sql), nil
}

func genTableName(schema string, table string) TableName {
	return TableName{Schema: schema, Name: table}

}

func ParserDDLTableName(sql string) (TableName, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return TableName{}, err
	}

	var res TableName
	switch v := stmt.(type) {
	case *ast.CreateDatabaseStmt:
		res = genTableName(v.Name, "")
	case *ast.DropDatabaseStmt:
		res = genTableName(v.Name, "")
	case *ast.CreateIndexStmt:
		res = genTableName(v.Table.Schema.O, v.Table.Name.L)
	case *ast.CreateTableStmt:
		res = genTableName(v.Table.Schema.O, v.Table.Name.L)
	case *ast.DropIndexStmt:
		res = genTableName(v.Table.Schema.O, v.Table.Name.L)
	case *ast.TruncateTableStmt:
		res = genTableName(v.Table.Schema.O, v.Table.Name.L)
	case *ast.AlterTableStmt:
		res = genTableName(v.Table.Schema.O, v.Table.Name.L)
	case *ast.DropTableStmt:
		if len(v.Tables) != 1 {
			return res, fmt.Errorf("may resovle DDL sql failed")
		}
		res = genTableName(v.Tables[0].Schema.O, v.Tables[0].Name.L)
	default:
		return res, fmt.Errorf("unkown DDL type")
	}

	return res, nil
}

func QuerySQL(db *sql.DB, query string) (*sql.Rows, error) {
	var (
		err  error
		rows *sql.Rows
	)

	for i := 0; i < maxRetryCount; i++ {
		if i > 0 {
			log.Printf("[INFO] client: query sql retry %d - %s", i, query)
			time.Sleep(retryTimeout)
		}

		log.Printf("[INFO] client: query sql:%s", query)

		rows, err = db.Query(query)
		if err != nil {
			log.Printf("[ERR] client: query sql:%s ,error:%v", query, err)
			continue
		}

		return rows, nil
	}

	if err != nil {
		log.Printf("[ERR] client: query sql: %s ,failed: %v", query, err)
		return nil, err
	}

	return nil, fmt.Errorf("query sql[%s] failed", query)
}

func executeSQL(db *sql.DB, sqls []string, args [][]interface{}, retry bool) error {
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
			log.Printf("[INFO] client: exec sql retry %d - %v - %v", i, sqls, args)
			time.Sleep(retryTimeout)
		}

		txn, err = db.Begin()
		if err != nil {
			log.Printf("[INFO] client: exec sqls[%v] begin failed %v", sqls, err)
			continue
		}

		for i := range sqls {
			log.Printf("[INFO] client: exec sql:%s ,args:%v", sqls[i], args[i])

			_, err = txn.Exec(sqls[i], args[i]...)
			if err != nil {
				log.Printf("[INFO] client: exec sql:%s ,args:%v ,error:%v", sqls[i], args[i], err)
				rerr := txn.Rollback()
				if rerr != nil {
					log.Printf("[ERR] client: exec sql:%s ,args:%v ,error:%v", sqls[i], args[i], rerr)
				}
				continue LOOP
			}
		}

		err = txn.Commit()
		if err != nil {
			log.Printf("[ERR] client: exec sqls:%v ,commit failed:%v", sqls, err)
			continue
		}

		return nil
	}

	if err != nil {
		log.Printf("[ERR] client: exec sqls:%v ,failed:%v", sqls, err)
		return err
	}

	return fmt.Errorf("exec sqls[%v] failed", sqls)
}
