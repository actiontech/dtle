package common

import (
	"fmt"
	"github.com/actiontech/dtle/drivers/mysql/mysql/mysqlconfig"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/vm"
	"strings"
)

// TableName is the table configuration
// slave restrict replication to a given table
type DataSource struct {
	TableSchema            string
	TableSchemaRegex       string
	TableSchemaRename      string
	Tables                 []*Table
}

func (d *DataSource) String() string {
	return fmt.Sprintf(d.TableSchema)
}

func IgnoreDbByReplicateIgnoreDb(replicateIgnoreDb []*DataSource, dbName string) bool {
	for _, ignoreDb := range replicateIgnoreDb {
		if ignoreDb.TableSchema == dbName && len(ignoreDb.Tables) == 0 {
			return true
		}
	}
	return false
}

func IgnoreTbByReplicateIgnoreDb(replicateIgnoreDb []*DataSource, dbName, tbName string) bool {
	for _, ignoreDb := range replicateIgnoreDb {
		if ignoreDb.TableSchema == dbName {
			for _, ignoreTb := range ignoreDb.Tables {
				if ignoreTb.TableName == tbName {
					return true
				}
			}
		}
	}
	return false
}

type Table struct {
	TableName         string
	TableRegex        string
	TableRename       string
	TableSchema       string
	TableSchemaRename string
	Counter           int64
	ColumnMapFrom     []string
	//ColumnMapTo       []string
	//ColumnMapUseRe    bool

	OriginalTableColumns *ColumnList
	UseUniqueKey         *UniqueKey
	ColumnMap            []int

	TableType    string

	Where string // Call GetWhere() instead of directly accessing.
}

func (t *Table) GetWhere() string {
	if t.Where == "" {
		return "true"
	} else {
		return t.Where
	}
}

func NewTable(schemaName string, tableName string) *Table {
	return &Table{
		TableSchema: schemaName,
		TableName:   tableName,
		Where:       "true",
	}
}

type TableContext struct {
	Table          *Table
	WhereCtx       *WhereContext
	DefChangedSent bool
}

func NewTableContext(table *Table, whereCtx *WhereContext) *TableContext {
	return &TableContext{
		Table:          table,
		WhereCtx:       whereCtx,
		DefChangedSent: false,
	}
}

func (t *TableContext) WhereTrue(values *ColumnValues) (bool, error) {
	var m = make(map[string]interface{})
	for field, idx := range t.WhereCtx.FieldsMap {
		nCols := len(values.AbstractValues)
		if idx >= nCols {
			return false, fmt.Errorf("cannot eval 'where' predicate: no enough columns (%v < %v)", nCols, idx)
		}

		//fmt.Printf("**** type of %v %T\n", field, *values.ValuesPointers[idx])
		rawValue := values.AbstractValues[idx]
		var value interface{}
		if rawValue == nil {
			value = rawValue
		} else {
			switch t.Table.OriginalTableColumns.ColumnList()[idx].Type {
			case mysqlconfig.TextColumnType:
				bs, ok := rawValue.([]byte)
				if !ok {
					return false,
						fmt.Errorf("where_predicate. expect []byte for TextColumnType, but got %T", rawValue)
				}
				value = string(bs)
			default:
				value = rawValue
			}
		}

		m[field] = value
	}
	ctx := datasource.NewContextSimpleNative(m)
	val, ok := vm.Eval(ctx, t.WhereCtx.Ast)
	if !ok {
		return false, fmt.Errorf("cannot eval 'where' predicate with the row value")
	}
	r, ok := val.Value().(bool)
	if !ok {
		return false, fmt.Errorf("'where' predicate does not eval to bool")
	}

	return r, nil
}

type WhereContext struct {
	Where     string
	Ast       expr.Node
	FieldsMap map[string]int
	IsDefault bool // is 'true'
}

func NewWhereCtx(where string, table *Table) (*WhereContext, error) {
	ast, err := expr.ParseExpression(where)
	if err != nil {
		return nil, err
	} else {
		fields := expr.FindAllIdentityField(ast)
		fieldsMap := make(map[string]int)
		for _, field := range fields {
			escapedFieldName := strings.ToLower(field) // TODO thorough escape
			if escapedFieldName == "true" || escapedFieldName == "false" {
				// qlbridge limitation
			} else if _, ok := fieldsMap[field]; !ok {
				if _, ok := table.OriginalTableColumns.Ordinals[field]; !ok {
					return nil, fmt.Errorf("bad 'where' for table %v.%v: field %v does not exist. known fields: %v",
						table.TableSchema, table.TableName, field, table.OriginalTableColumns.Ordinals)
				} else {
					fieldsMap[field] = table.OriginalTableColumns.Ordinals[field]
				}
			} else {
				// already mapped
			}
		}

		// We parse it even it is just 'true', but use the 'IsDefault' flag to optimize.
		return &WhereContext{
			Where:     where,
			Ast:       ast,
			FieldsMap: fieldsMap,
			IsDefault: strings.ToLower(where) == "true",
		}, nil
	}
}
