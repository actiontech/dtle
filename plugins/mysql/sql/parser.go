package sql

import (
	"fmt"
	"hash/crc32"
	"reflect"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"

	uconf "udup/config"
)

type OpType byte

const (
	Insert = iota + 1
	Update
	Del
	Ddl
	Gtid
)

type StreamEvent struct {
	Tp    OpType
	Sql   string
	Args  []interface{}
	Key   string
	Retry bool
}

func NewStreamEvent(tp OpType, sql string, args []interface{}, key string, retry bool) *StreamEvent {
	return &StreamEvent{Tp: tp, Sql: sql, Args: args, Key: key, Retry: retry}
}

type Column struct {
	Idx      int
	Name     string
	Unsigned bool
}

type Table struct {
	Schema string
	Name   string

	Columns      []*Column
	IndexColumns []*Column
}

func castUnsigned(data interface{}, unsigned bool) interface{} {
	if !unsigned {
		return data
	}

	switch v := data.(type) {
	case int:
		return uint(v)
	case int8:
		return uint8(v)
	case int16:
		return uint16(v)
	case int32:
		return uint32(v)
	case int64:
		return strconv.FormatUint(uint64(v), 10)
	}

	return data
}

func columnValue(value interface{}, unsigned bool) string {
	castValue := castUnsigned(value, unsigned)

	var data string
	switch v := castValue.(type) {
	case nil:
		data = "null"
	case bool:
		if v {
			data = "1"
		} else {
			data = "0"
		}
	case int:
		data = strconv.FormatInt(int64(v), 10)
	case int8:
		data = strconv.FormatInt(int64(v), 10)
	case int16:
		data = strconv.FormatInt(int64(v), 10)
	case int32:
		data = strconv.FormatInt(int64(v), 10)
	case int64:
		data = strconv.FormatInt(int64(v), 10)
	case uint8:
		data = strconv.FormatUint(uint64(v), 10)
	case uint16:
		data = strconv.FormatUint(uint64(v), 10)
	case uint32:
		data = strconv.FormatUint(uint64(v), 10)
	case uint64:
		data = strconv.FormatUint(uint64(v), 10)
	case float32:
		data = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		data = strconv.FormatFloat(float64(v), 'f', -1, 64)
	case string:
		data = v
	case []byte:
		data = string(v)
	default:
		data = fmt.Sprintf("%v", v)
	}

	return data
}

func findColumn(columns []*Column, indexColumn string) *Column {
	for _, column := range columns {
		if column.Name == indexColumn {
			return column
		}
	}

	return nil
}

func FindColumns(columns []*Column, indexColumns []string) []*Column {
	result := make([]*Column, 0, len(indexColumns))

	for _, name := range indexColumns {
		column := findColumn(columns, name)
		if column != nil {
			result = append(result, column)
		}
	}

	return result
}

func genColumnList(columns []*Column) string {
	var columnList []byte
	for i, column := range columns {
		columnList = append(columnList, []byte(column.Name)...)

		if i != len(columns)-1 {
			columnList = append(columnList, ',')
		}
	}

	return string(columnList)
}

func GenHashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func genKeyList(columns []*Column, datas []interface{}) string {
	values := make([]string, 0, len(datas))
	for i, data := range datas {
		values = append(values, columnValue(data, columns[i].Unsigned))
	}

	return strings.Join(values, ",")
}

func genColumnPlaceholders(length int) string {
	values := make([]string, length, length)
	for i := 0; i < length; i++ {
		values[i] = "?"
	}
	return strings.Join(values, ",")
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

func IgnoreDDLError(err error) bool {
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}

	switch mysqlErr.Number {
	case ErrDatabaseExists, ErrDatabaseNotExists, ErrDatabaseDropExists,
		ErrTableExists, ErrTableNotExists, ErrTableDropExists,
		ErrColumnExists, ErrColumnNotExists,
		ErrIndexExists, ErrCantDropFieldOrKey:
		return true
	default:
		return false
	}
}

// resolveDDLSQL resolve to one ddl sql
// example: drop table test.a,test2.b -> drop table test.a; drop table test2.b;
func ResolveDDLSQL(sql string) (sqls []string, ok bool, err error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, false, err
	}

	/*_, isDDL := stmt.(ast.DDLNode)
	if !isDDL {
		sqls = append(sqls, sql)
		return
	}*/

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

func GenDDLSQL(sql string, schema string) (string, error) {
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

func genTableName(schema string, table string) uconf.TableName {
	return uconf.TableName{Schema: schema, Name: table}

}

func ParserDDLTableName(sql string) (uconf.TableName, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return uconf.TableName{}, err
	}

	var res uconf.TableName
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
	case *ast.CreateUserStmt:
		res = genTableName("mysql", "user")
	case *ast.GrantStmt:
		res = genTableName("mysql", "user")
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

// EscapeName will escape a db/table/column/... name by wrapping with backticks.
// It is not fool proof. I'm just trying to do the right thing here, not solving
// SQL injection issues, which should be irrelevant for this tool.
func EscapeName(name string) string {
	if unquoted, err := strconv.Unquote(name); err == nil {
		name = unquoted
	}
	return fmt.Sprintf("`%s`", name)
}
