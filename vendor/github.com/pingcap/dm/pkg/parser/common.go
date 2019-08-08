// Copyright 2019 PingCAP, Inc.
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

package parser

import (
	"bytes"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/filter"
	_ "github.com/pingcap/tidb/types/parser_driver" // for import parser driver
)

var (
	// ErrDropMultipleTables is error that don't allow to drop multiple tables in one statement
	ErrDropMultipleTables = errors.New("not allow operation: drop multiple tables in one statement")
	// ErrRenameMultipleTables is error that don't allow to rename multiple tables in one statement
	ErrRenameMultipleTables = errors.New("not allow operation: rename multiple tables in one statement")
	// ErrAlterMultipleTables is error that don't allow to alter multiple tables in one statement
	ErrAlterMultipleTables = errors.New("not allow operation: alter multiple tables in one statement")
)

// Parse wraps parser.Parse(), makes `parser` suitable for dm
func Parse(p *parser.Parser, sql, charset, collation string) (stmt []ast.StmtNode, err error) {
	stmts, warnings, err := p.Parse(sql, charset, collation)

	for _, warning := range warnings {
		log.Warnf("parsing sql %s:%v", sql, warning)
	}

	if err != nil {
		log.Errorf("parsing sql %s:%v", sql, err)
	}

	return stmts, errors.Trace(err)
}

// FetchDDLTableNames returns table names in ddl
// the result contains [tableName] excepted create table like and rename table
// for `create table like` DDL, result contains [sourceTableName, sourceRefTableName]
// for rename table ddl, result contains [oldTableName, newTableName]
func FetchDDLTableNames(schema string, stmt ast.StmtNode) ([]*filter.Table, error) {
	var res []*filter.Table
	switch v := stmt.(type) {
	case *ast.CreateDatabaseStmt:
		res = append(res, genTableName(v.Name, ""))
	case *ast.DropDatabaseStmt:
		res = append(res, genTableName(v.Name, ""))
	case *ast.CreateTableStmt:
		res = append(res, genTableName(v.Table.Schema.O, v.Table.Name.O))
		if v.ReferTable != nil {
			res = append(res, genTableName(v.ReferTable.Schema.O, v.ReferTable.Name.O))
		}
	case *ast.DropTableStmt:
		if len(v.Tables) != 1 {
			return res, ErrDropMultipleTables
		}
		res = append(res, genTableName(v.Tables[0].Schema.O, v.Tables[0].Name.O))
	case *ast.TruncateTableStmt:
		res = append(res, genTableName(v.Table.Schema.O, v.Table.Name.O))
	case *ast.AlterTableStmt:
		res = append(res, genTableName(v.Table.Schema.O, v.Table.Name.O))
		if v.Specs[0].NewTable != nil {
			res = append(res, genTableName(v.Specs[0].NewTable.Schema.O, v.Specs[0].NewTable.Name.O))
		}
	case *ast.RenameTableStmt:
		res = append(res, genTableName(v.OldTable.Schema.O, v.OldTable.Name.O))
		res = append(res, genTableName(v.NewTable.Schema.O, v.NewTable.Name.O))
	case *ast.CreateIndexStmt:
		res = append(res, genTableName(v.Table.Schema.O, v.Table.Name.O))
	case *ast.DropIndexStmt:
		res = append(res, genTableName(v.Table.Schema.O, v.Table.Name.O))
	default:
		return res, errors.Errorf("unknown type ddl %s", stmt)
	}

	for i := range res {
		if res[i].Schema == "" {
			res[i].Schema = schema
		}
	}

	return res, nil
}

// RenameDDLTable renames table names in ddl by given `targetTableNames`
// argument `targetTableNames` is same with return value of FetchDDLTableNames
func RenameDDLTable(stmt ast.StmtNode, targetTableNames []*filter.Table) (string, error) {
	switch v := stmt.(type) {
	case *ast.CreateDatabaseStmt:
		v.Name = targetTableNames[0].Schema

	case *ast.DropDatabaseStmt:
		v.Name = targetTableNames[0].Schema

	case *ast.CreateTableStmt:
		v.Table.Schema = model.NewCIStr(targetTableNames[0].Schema)
		v.Table.Name = model.NewCIStr(targetTableNames[0].Name)

		if v.ReferTable != nil {
			v.ReferTable.Schema = model.NewCIStr(targetTableNames[1].Schema)
			v.ReferTable.Name = model.NewCIStr(targetTableNames[1].Name)
		}

	case *ast.DropTableStmt:
		if len(v.Tables) > 1 {
			return "", ErrDropMultipleTables
		}

		v.Tables[0].Schema = model.NewCIStr(targetTableNames[0].Schema)
		v.Tables[0].Name = model.NewCIStr(targetTableNames[0].Name)

	case *ast.TruncateTableStmt:
		v.Table.Schema = model.NewCIStr(targetTableNames[0].Schema)
		v.Table.Name = model.NewCIStr(targetTableNames[0].Name)

	case *ast.DropIndexStmt:
		v.Table.Schema = model.NewCIStr(targetTableNames[0].Schema)
		v.Table.Name = model.NewCIStr(targetTableNames[0].Name)
	case *ast.CreateIndexStmt:
		v.Table.Schema = model.NewCIStr(targetTableNames[0].Schema)
		v.Table.Name = model.NewCIStr(targetTableNames[0].Name)
	case *ast.RenameTableStmt:
		if len(v.TableToTables) > 1 {
			return "", ErrRenameMultipleTables
		}

		v.TableToTables[0].OldTable.Schema = model.NewCIStr(targetTableNames[0].Schema)
		v.TableToTables[0].OldTable.Name = model.NewCIStr(targetTableNames[0].Name)
		v.TableToTables[0].NewTable.Schema = model.NewCIStr(targetTableNames[1].Schema)
		v.TableToTables[0].NewTable.Name = model.NewCIStr(targetTableNames[1].Name)

	case *ast.AlterTableStmt:
		if len(v.Specs) > 1 {
			return "", ErrAlterMultipleTables
		}

		v.Table.Schema = model.NewCIStr(targetTableNames[0].Schema)
		v.Table.Name = model.NewCIStr(targetTableNames[0].Name)

		if v.Specs[0].Tp == ast.AlterTableRenameTable {
			v.Specs[0].NewTable.Schema = model.NewCIStr(targetTableNames[1].Schema)
			v.Specs[0].NewTable.Name = model.NewCIStr(targetTableNames[1].Name)
		}

	default:
		return "", errors.Errorf("unkown type ddl %+v", stmt)
	}

	var b []byte
	bf := bytes.NewBuffer(b)
	err := stmt.Restore(&format.RestoreCtx{
		Flags: format.DefaultRestoreFlags,
		In:    bf,
	})
	if err != nil {
		return "", errors.Annotate(err, "restore ast node")
	}

	return bf.String(), nil
}

// SplitDDL splits multiple operations in one DDL statement into multiple DDL statements
// if fail to restore, it would not restore the value of `stmt` (it changes it's values if `stmt` is one of  DropTableStmt, RenameTableStmt, AlterTableStmt)
func SplitDDL(stmt ast.StmtNode, schema string) (sqls []string, err error) {
	var (
		schemaName = model.NewCIStr(schema) // fill schema name
		bf         = new(bytes.Buffer)
		ctx        = &format.RestoreCtx{
			Flags: format.DefaultRestoreFlags,
			In:    bf,
		}
	)

	switch v := stmt.(type) {
	case *ast.CreateDatabaseStmt:
		v.IfNotExists = true
	case *ast.DropDatabaseStmt:
		v.IfExists = true
	case *ast.DropTableStmt:
		v.IfExists = true

		tables := v.Tables
		for _, t := range tables {
			if t.Schema.O == "" {
				t.Schema = schemaName
			}

			v.Tables = []*ast.TableName{t}
			bf.Reset()
			err = stmt.Restore(ctx)
			if err != nil {
				v.Tables = tables
				return nil, errors.Annotate(err, "restore ast node")
			}

			sqls = append(sqls, bf.String())
		}
		v.Tables = tables

		return sqls, nil
	case *ast.CreateTableStmt:
		v.IfNotExists = true
		if v.Table.Schema.O == "" {
			v.Table.Schema = schemaName
		}

		if v.ReferTable != nil && v.ReferTable.Schema.O == "" {
			v.ReferTable.Schema = schemaName
		}
	case *ast.TruncateTableStmt:
		if v.Table.Schema.O == "" {
			v.Table.Schema = schemaName
		}
	case *ast.DropIndexStmt:
		v.IfExists = true
		if v.Table.Schema.O == "" {
			v.Table.Schema = schemaName
		}
	case *ast.CreateIndexStmt:
		if v.Table.Schema.O == "" {
			v.Table.Schema = schemaName
		}
	case *ast.RenameTableStmt:
		t2ts := v.TableToTables
		for _, t2t := range t2ts {
			if t2t.OldTable.Schema.O == "" {
				t2t.OldTable.Schema = schemaName
			}
			if t2t.NewTable.Schema.O == "" {
				t2t.NewTable.Schema = schemaName
			}

			v.TableToTables = []*ast.TableToTable{t2t}

			bf.Reset()
			err = stmt.Restore(ctx)
			if err != nil {
				v.TableToTables = t2ts
				return nil, errors.Annotate(err, "restore ast node")
			}

			sqls = append(sqls, bf.String())
		}
		v.TableToTables = t2ts

		return sqls, nil
	case *ast.AlterTableStmt:
		specs := v.Specs
		table := v.Table

		if v.Table.Schema.O == "" {
			v.Table.Schema = schemaName
		}

		for _, spec := range specs {
			if spec.Tp == ast.AlterTableRenameTable {
				if spec.NewTable.Schema.O == "" {
					spec.NewTable.Schema = schemaName
				}
			}

			v.Specs = []*ast.AlterTableSpec{spec}

			bf.Reset()
			err = stmt.Restore(ctx)
			if err != nil {
				v.Specs = specs
				v.Table = table
				return nil, errors.Annotate(err, "restore ast node")
			}
			sqls = append(sqls, bf.String())

			if spec.Tp == ast.AlterTableRenameTable {
				v.Table = spec.NewTable
			}
		}
		v.Specs = specs
		v.Table = table

		return sqls, nil
	default:
		return nil, errors.Errorf("unknown type ddl %+v", stmt)
	}

	bf.Reset()
	err = stmt.Restore(ctx)
	if err != nil {
		return nil, errors.Annotate(err, "restore ast node")
	}
	sqls = append(sqls, bf.String())

	return sqls, nil
}

func genTableName(schema string, table string) *filter.Table {
	return &filter.Table{Schema: schema, Name: table}
}
