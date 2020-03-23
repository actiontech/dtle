// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package dbutil

import (
	"context"
	"database/sql"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	_ "github.com/pingcap/tidb/types/parser_driver" // for parser driver
)

// GetTableInfo returns table information.
func GetTableInfo(ctx context.Context, db *sql.DB, schemaName string, tableName string) (*model.TableInfo, error) {
	createTableSQL, err := GetCreateTableSQL(ctx, db, schemaName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return GetTableInfoBySQL(createTableSQL)
}

// GetTableInfoBySQL returns table information by given create table sql.
func GetTableInfoBySQL(createTableSQL string) (table *model.TableInfo, err error) {
	stmt, err := parser.New().ParseOneStmt(createTableSQL, "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}

	s, ok := stmt.(*ast.CreateTableStmt)
	if ok {
		cols, newConstraints := BuildColumnsAndConstraints(s.Cols, s.Constraints)
		table, err := BuildTableInfo(s.Table.Name, cols, newConstraints)
		if err != nil {
			return nil, errors.Trace(err)
		}

		return table, nil
	}

	return nil, errors.Errorf("get table info from sql %s failed!", createTableSQL)
}

// BuildColumnsAndConstraints reference https://github.com/pingcap/tidb/blob/12c87929b8444571b9e84d2c0d5b85303d27da64/ddl/ddl_api.go#L168
func BuildColumnsAndConstraints(colDefs []*ast.ColumnDef, constraints []*ast.Constraint) ([]*model.ColumnInfo, []*ast.Constraint) {
	var cols []*model.ColumnInfo
	for i, colDef := range colDefs {
		col, cts := buildColumnAndConstraint(i, colDef)
		cols = append(cols, col)
		constraints = append(constraints, cts...)
	}
	return cols, constraints
}

func buildColumnAndConstraint(offset int, colDef *ast.ColumnDef) (*model.ColumnInfo, []*ast.Constraint) {
	return columnDefToCol(offset, colDef)
}

// columnDefToCol converts ColumnDef to Col and TableConstraints.
// now only save the index information for column
// reference https://github.com/pingcap/tidb/blob/12c87929b8444571b9e84d2c0d5b85303d27da64/ddl/ddl_api.go#L254
func columnDefToCol(offset int, colDef *ast.ColumnDef) (*model.ColumnInfo, []*ast.Constraint) {
	var constraints = make([]*ast.Constraint, 0)

	column := &model.ColumnInfo{
		Offset:    offset,
		Name:      colDef.Name.Name,
		FieldType: *colDef.Tp,
	}
	col := table.ToColumn(column)

	if colDef.Options != nil {
		length := types.UnspecifiedLength
		keys := []*ast.IndexColName{
			{
				Column: colDef.Name,
				Length: length,
			},
		}

		for _, v := range colDef.Options {
			switch v.Tp {
			case ast.ColumnOptionNotNull:
				// do nothing
			case ast.ColumnOptionNull:
				// do nothing
			case ast.ColumnOptionAutoIncrement:
				// do nothing
			case ast.ColumnOptionPrimaryKey:
				constraint := &ast.Constraint{Tp: ast.ConstraintPrimaryKey, Keys: keys}
				constraints = append(constraints, constraint)
				col.Flag |= mysql.PriKeyFlag
			case ast.ColumnOptionUniqKey:
				constraint := &ast.Constraint{Tp: ast.ConstraintUniqKey, Name: colDef.Name.Name.O, Keys: keys}
				constraints = append(constraints, constraint)
				col.Flag |= mysql.UniqueKeyFlag
			case ast.ColumnOptionDefaultValue:
				// do nothing
			case ast.ColumnOptionOnUpdate:
				// do nothing
			case ast.ColumnOptionComment:
				// do nothing
			case ast.ColumnOptionGenerated:
				// FIXME: use a default string now to make col.IsGenerated() return true, will use real generated expr string later
				col.GeneratedExprString = "Generated"
				// do nothing
			case ast.ColumnOptionFulltext:
				// do nothing
			}
		}
	}

	return column, constraints
}

// BuildTableInfo builds table information using column constraints.
func BuildTableInfo(tableName model.CIStr, columns []*model.ColumnInfo, constraints []*ast.Constraint) (tbInfo *model.TableInfo, err error) {
	tbInfo = &model.TableInfo{
		Name: tableName,
	}
	cols := make([]*table.Column, 0, len(columns))
	for _, v := range columns {
		v.ID = allocateColumnID(tbInfo)
		tbInfo.Columns = append(tbInfo.Columns, v)
		cols = append(cols, table.ToColumn(v))
	}

	for _, constr := range constraints {
		if constr.Tp == ast.ConstraintForeignKey {
			for _, fk := range tbInfo.ForeignKeys {
				if fk.Name.L == strings.ToLower(constr.Name) {
					return nil, infoschema.ErrCannotAddForeign
				}
			}
			var fk model.FKInfo
			fk.Name = model.NewCIStr(constr.Name)
			fk.RefTable = constr.Refer.Table.Name
			for _, key := range constr.Keys {
				fk.Cols = append(fk.Cols, key.Column.Name)
			}
			for _, key := range constr.Refer.IndexColNames {
				fk.RefCols = append(fk.RefCols, key.Column.Name)
			}
			if len(fk.Cols) != len(fk.RefCols) {
				return nil, infoschema.ErrForeignKeyNotMatch.GenWithStackByArgs(tbInfo.Name.O)
			}
			if len(fk.Cols) == 0 {
				// TODO: In MySQL, this case will report a parse error.
				return nil, infoschema.ErrCannotAddForeign
			}
			tbInfo.ForeignKeys = append(tbInfo.ForeignKeys, &fk)
			continue
		}
		if constr.Tp == ast.ConstraintPrimaryKey {
			var col *table.Column
			for _, key := range constr.Keys {
				col = table.FindCol(cols, key.Column.Name.O)
				if col == nil {
					return nil, errors.Errorf("key column %s doesn't exist in table", key.Column.Name)
				}
				// Virtual columns cannot be used in primary key.
				if col.IsGenerated() && !col.GeneratedStored {
					return nil, errors.Errorf("Defining a virtual generated column as primary key")
				}
			}
			if len(constr.Keys) == 1 {
				switch col.Tp {
				case mysql.TypeLong, mysql.TypeLonglong,
					mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
					tbInfo.PKIsHandle = true
				}
			}
		}
		// build index info.
		idxInfo, err := buildIndexInfo(tbInfo, model.NewCIStr(constr.Name), constr.Keys, model.StatePublic)
		if err != nil {
			return nil, errors.Trace(err)
		}
		//check if the index is primary or uniqiue.
		switch constr.Tp {
		case ast.ConstraintPrimaryKey:
			idxInfo.Primary = true
			idxInfo.Unique = true
			idxInfo.Name = model.NewCIStr(mysql.PrimaryKeyName)
		case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			idxInfo.Unique = true
		}
		tbInfo.Indices = append(tbInfo.Indices, idxInfo)
	}

	return
}

func buildIndexInfo(tblInfo *model.TableInfo, indexName model.CIStr, idxColNames []*ast.IndexColName, state model.SchemaState) (*model.IndexInfo, error) {
	idxColumns, err := buildIndexColumns(tblInfo.Columns, idxColNames)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Create index info.
	idxInfo := &model.IndexInfo{
		Name:    indexName,
		Columns: idxColumns,
		State:   state,
	}
	return idxInfo, nil
}

func buildIndexColumns(columns []*model.ColumnInfo, idxColNames []*ast.IndexColName) ([]*model.IndexColumn, error) {
	idxColumns := make([]*model.IndexColumn, 0, len(idxColNames))

	for _, ic := range idxColNames {
		col := FindColumnByName(columns, ic.Column.Name.O)

		idxColumns = append(idxColumns, &model.IndexColumn{
			Name:   col.Name,
			Offset: col.Offset,
			Length: ic.Length,
		})
	}

	return idxColumns, nil
}

func allocateColumnID(tblInfo *model.TableInfo) int64 {
	tblInfo.MaxColumnID++
	return tblInfo.MaxColumnID
}

// FindColumnByName finds column by name.
func FindColumnByName(cols []*model.ColumnInfo, name string) *model.ColumnInfo {
	// column name don't distinguish capital and small letter
	name = strings.ToLower(name)
	for _, col := range cols {
		if col.Name.L == name {
			return col
		}
	}

	return nil
}

// EqualTableInfo returns true if this two table info have same columns and indices
func EqualTableInfo(tableInfo1, tableInfo2 *model.TableInfo) bool {
	// check columns
	if len(tableInfo1.Columns) != len(tableInfo2.Columns) {
		return false
	}

	for j, col := range tableInfo1.Columns {
		if col.Name.O != tableInfo2.Columns[j].Name.O {
			return false
		}
		if col.Tp != tableInfo2.Columns[j].Tp {
			return false
		}
	}

	// check index
	if len(tableInfo1.Indices) != len(tableInfo2.Indices) {
		return false
	}

	index2Map := make(map[string]*model.IndexInfo)
	for _, index := range tableInfo2.Indices {
		index2Map[index.Name.O] = index
	}

	for _, index1 := range tableInfo1.Indices {
		index2, ok := index2Map[index1.Name.O]
		if !ok {
			return false
		}

		if len(index1.Columns) != len(index2.Columns) {
			return false
		}
		for j, col := range index1.Columns {
			if col.Name.O != index2.Columns[j].Name.O {
				return false
			}
		}
	}

	return true
}
