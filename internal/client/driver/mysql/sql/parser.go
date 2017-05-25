package sql

import (
	"fmt"
	"strconv"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"

	uconf "udup/internal/config"
)

type OpType byte

type StreamEvent struct {
	Tp    OpType
	Sql   string
	Args  []interface{}
	Key   string
	Retry bool
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

func IgnoreDDLError(err error) bool {
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}

	switch mysqlErr.Number {
	case ErrDatabaseExists, ErrDatabaseNotExists, ErrDatabaseDropExists,
		ErrTableExists, ErrTableNotExists, ErrTableDropExists,
		ErrColumnExists, ErrColumnNotExists,ErrDupKeyName,
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
	return uconf.TableName{Schema: schema, Table: table}

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
