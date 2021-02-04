package binlog

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/actiontech/dtle/drivers/mysql/common"
	hclog "github.com/hashicorp/go-hclog"

	"github.com/pingcap/parser"
)

func Test_loadMapping(t *testing.T) {

	replicateDoDbWithRename := []*common.DataSource{
		{
			TableSchema:       "db1",
			TableSchemaRename: "db1-rename",
			Tables: []*common.Table{
				{
					TableName:         "tb1",
					TableRename:       "db1-tb1-rename",
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
				},
			},
		},
		{
			TableSchema:       "db2",
			TableSchemaRename: "db2-rename",
			Tables: []*common.Table{
				{
					TableName:         "tb1",
					TableRename:       "db2-tb1-rename",
					TableSchema:       "db2",
					TableSchemaRename: "db2-rename",
				},
			},
		},
	}

	replicateDoDbWithoutRename := []*common.DataSource{
		{
			TableSchema: "db1",
			Tables: []*common.Table{
				{
					TableName:   "tb1",
					TableSchema: "db1",
				},
			},
		},
		{
			TableSchema: "db2",
			Tables: []*common.Table{
				{
					TableName:   "tb1",
					TableSchema: "db2",
				},
			},
		},
	}

	type args struct {
		sql             string
		currentSchema   string
		newSchemaName   string
		newTableName    string
		replicationDoDB []*common.DataSource
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// test with rename config
		// drop table
		{name: "schema-rename-drop-table", args: args{
			sql:             "drop table `db1`.`tb1`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithRename,
		}, want: "DROP TABLE `db1-rename`.`db1-tb1-rename`"},
		{name: "schema-rename-drop-table", args: args{ //drop table without specify schema
			sql:             "drop table `tb1`,`db2`.tb1",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithRename,
		}, want: "DROP TABLE `db1-rename`.`db1-tb1-rename`, `db2-rename`.`db2-tb1-rename`"},
		{name: "schema-rename-drop-table", args: args{ //drop several tables including table that need no renaming
			sql:             "drop table `db1`.`tb1`,`db2`.tb1,`db3`.`tb1`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithRename,
		}, want: "DROP TABLE `db1-rename`.`db1-tb1-rename`, `db2-rename`.`db2-tb1-rename`, `db3`.`tb1`"},
		// create/drop database
		{name: "schema-rename-create-database", args: args{
			sql:             "create database `db1`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithRename,
		}, want: "CREATE DATABASE `db1-rename`"},
		{name: "schema-rename-drop-database", args: args{
			sql:             "drop database `db1`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithRename,
		}, want: "DROP DATABASE `db1-rename`"},
		// alter database
		{name: "schema-rename-alter-database", args: args{
			sql:             "alter database `db1` character set utf8 collate utf8_general_ci",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithRename,
		}, want: "ALTER DATABASE `db1-rename` CHARACTER SET = utf8 COLLATE = utf8_general_ci"},
		{name: "schema-rename-alter-database", args: args{ // alter default database
			sql:             "alter database character set utf8 collate utf8_general_ci",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithRename,
		}, want: "ALTER DATABASE CHARACTER SET = utf8 COLLATE = utf8_general_ci"},
		// create index
		{name: "schema-rename-create-index", args: args{
			sql:             "create index idx on `db1`.`tb1` (name(10))",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithRename,
		}, want: "CREATE INDEX `idx` ON `db1-rename`.`db1-tb1-rename` (`name`(10))"},
		// drop index
		{name: "schema-rename-drop-index", args: args{
			sql:             "drop index idx on `db1`.`tb1`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithRename,
		}, want: "DROP INDEX `idx` ON `db1-rename`.`db1-tb1-rename`"},
		// create table
		{name: "schema-rename-create-table", args: args{
			sql:             "create table `db1`.`tb1`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithRename,
		}, want: "CREATE TABLE `db1-rename`.`db1-tb1-rename` "},
		// alter table
		{name: "schema-rename-alter-table", args: args{
			sql:             "alter table `db1`.`tb1` add column a int",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithRename,
		}, want: "ALTER TABLE `db1-rename`.`db1-tb1-rename` ADD COLUMN `a` INT"},
		{name: "schema-rename-alter-table", args: args{
			sql:             "alter table `db1`.`tb1` rename as `db2`.`tb1`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithRename,
		}, want: "ALTER TABLE `db1-rename`.`db1-tb1-rename` RENAME AS `db2-rename`.`db2-tb1-rename`"},
		{name: "schema-rename-alter-table", args: args{
			sql:             "alter table `db1`.`tb1` rename to `db2`.`tb1`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithRename,
		}, want: "ALTER TABLE `db1-rename`.`db1-tb1-rename` RENAME AS `db2-rename`.`db2-tb1-rename`"},
		// flush tables
		{name: "schema-rename-flush-tables", args: args{
			sql:             "flush tables `tb1`,`db2`.tb1",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithRename,
		}, want: "FLUSH TABLES `db1-rename`.`db1-tb1-rename`, `db2-rename`.`db2-tb1-rename`"},
		// truncate table
		{name: "schema-rename-truncate-table", args: args{
			sql:             "truncate table `db1`.`tb1`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithRename,
		}, want: "TRUNCATE TABLE `db1-rename`.`db1-tb1-rename`"},
		// rename table
		{name: "schema-rename-rename-table", args: args{
			sql:             "rename table `db1`.`tb1` to `db1`.`tb2`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithRename,
		}, want: "RENAME TABLE `db1-rename`.`db1-tb1-rename` TO `db1-rename`.`tb2`"},
		{name: "schema-rename-rename-table", args: args{
			sql:             "rename table `tb1` to `tb2`,`db2`.`tb1` to `db2`.`tb2`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithRename,
		}, want: "RENAME TABLE `db1-rename`.`db1-tb1-rename` TO `db1-rename`.`tb2`, `db2-rename`.`db2-tb1-rename` TO `db2-rename`.`tb2`"},

		// test without rename config
		// drop table
		{name: "schema-map-drop-table", args: args{ //drop table without specify schema
			sql:             "drop table `tb1`,`db2`.tb1",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithoutRename,
		}, want: "DROP TABLE `db1`.`tb1`, `db2`.`tb1`"},
		// create/drop database
		{name: "schema-map-create-database", args: args{
			sql:             "create database `db1`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithoutRename,
		}, want: "CREATE DATABASE `db1`"},
		{name: "schema-map-drop-database", args: args{
			sql:             "drop database `db1`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithoutRename,
		}, want: "DROP DATABASE `db1`"},
		// alter database
		{name: "schema-map-alter-database", args: args{
			sql:           "alter database `db1` character set utf8 collate utf8_general_ci",
			currentSchema: "db1",
		}, want: "ALTER DATABASE `db1` CHARACTER SET = utf8 COLLATE = utf8_general_ci"},
		{name: "schema-map-alter-database", args: args{ // alter default database
			sql:             "alter database character set utf8 collate utf8_general_ci",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithoutRename,
		}, want: "ALTER DATABASE CHARACTER SET = utf8 COLLATE = utf8_general_ci"},
		// create index
		{name: "schema-map-create-index", args: args{
			sql:             "create index idx on `db1`.`tb1` (name(10))",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithoutRename,
		}, want: "CREATE INDEX `idx` ON `db1`.`tb1` (`name`(10))"},
		// drop index
		{name: "schema-map-drop-index", args: args{
			sql:             "drop index idx on `db1`.`tb1`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithoutRename,
		}, want: "DROP INDEX `idx` ON `db1`.`tb1`"},
		// create table
		{name: "schema-map-create-table", args: args{
			sql:             "create table `db1`.`tb1`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithoutRename,
		}, want: "CREATE TABLE `db1`.`tb1` "},
		// alter table
		{name: "schema-map-alter-table", args: args{
			sql:             "alter table `db1`.`tb1` add column a int",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithoutRename,
		}, want: "ALTER TABLE `db1`.`tb1` ADD COLUMN `a` INT"},
		{name: "schema-map-alter-table", args: args{
			sql:             "alter table `db1`.`tb1` rename as `db2`.`tb1`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithoutRename,
		}, want: "ALTER TABLE `db1`.`tb1` RENAME AS `db2`.`tb1`"},
		{name: "schema-map-alter-table", args: args{
			sql:             "alter table `db1`.`tb1` rename to `db2`.`tb1`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithoutRename,
		}, want: "ALTER TABLE `db1`.`tb1` RENAME AS `db2`.`tb1`"},
		// flush tables
		{name: "schema-map-flush-tables", args: args{
			sql:             "flush tables `tb1`,`db2`.tb1",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithoutRename,
		}, want: "FLUSH TABLES `db1`.`tb1`, `db2`.`tb1`"},
		// truncate table
		{name: "schema-map-truncate-table", args: args{
			sql:             "truncate table `db1`.`tb1`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithoutRename,
		}, want: "TRUNCATE TABLE `db1`.`tb1`"},
		// rename table
		{name: "schema-map-rename-table", args: args{
			sql:             "rename table `db1`.`tb1` to `db1`.`tb2`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithoutRename,
		}, want: "RENAME TABLE `db1`.`tb1` TO `db1`.`tb2`"},
		{name: "schema-map-rename-table", args: args{
			sql:             "rename table `tb1` to `tb2`,`db2`.`tb1` to `db2`.`tb2`",
			currentSchema:   "db1",
			replicationDoDB: replicateDoDbWithoutRename,
		}, want: "RENAME TABLE `db1`.`tb1` TO `db1`.`tb2`, `db2`.`tb1` TO `db2`.`tb2`"},
	}

	binlogReader := &BinlogReader{
		logger: hclog.New(&hclog.LoggerOptions{
			Level:      hclog.Debug,
			JSONFormat: true,
		}),
		mysqlContext: &common.MySQLDriverConfig{},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.New().ParseOneStmt(tt.args.sql, "", "")
			if err != nil {
				t.Error(err)
				return
			}

			binlogReader.mysqlContext.ReplicateDoDb = tt.args.replicationDoDB
			schemaRenameMap, schemaNameToTablesRenameMap := binlogReader.generateRenameMaps()
			if got, err := binlogReader.loadMapping(tt.args.sql, tt.args.currentSchema, schemaRenameMap, schemaNameToTablesRenameMap, stmt); nil != err {
				t.Errorf("loadMapping() failed: %v", err)
			} else if got != tt.want {
				t.Errorf("loadMapping() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_resolveDDLSQL(t *testing.T) {
	skipFunc1 := func(schema string, tableName string) bool {
		return schema == "skip" || tableName == "skip"
	}
	type args struct {
		currentSchema string
		sql           string
		skipFunc      func(schema string, tableName string) bool
	}
	tests := []struct {
		name       string
		args       args
		wantResult parseDDLResult
		wantErr    bool
	}{
		{
			name:       "drop-table-1",
			args:       args{
				currentSchema: "",
				sql:           "drop table a.b, skip.c, d",
				skipFunc:      skipFunc1,
			},
			wantResult: parseDDLResult{
				sql:         "DROP TABLE `a`.`b`, `d`",
			},
			wantErr:    false,
		}, {
			name:       "drop-table-2",
			args:       args{
				currentSchema: "",
				sql:           "drop table if exists skip.b, skip.c",
				skipFunc:      skipFunc1,
			},
			wantResult: parseDDLResult{
				sql:         "drop table if exists dtle-dummy-never-exists.dtle-dummy-never-exists",
			},
			wantErr:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotResult, err := resolveDDLSQL(tt.args.currentSchema, tt.args.sql, tt.args.skipFunc)
			if (err != nil) != tt.wantErr {
				t.Errorf("resolveDDLSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if gotResult.sql != tt.wantResult.sql {
				t.Errorf("resolveDDLSQL() gotResult = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}

func Test_generateRenameMaps(t *testing.T) {
	replicateDoD := []*common.DataSource{
		{
			TableSchema:       "db1",
			TableSchemaRename: "db1-rename",
			Tables: []*common.Table{
				{
					TableName:         "tb1",
					TableRename:       "db1-tb1-rename",
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
				},
				{
					TableName:         "tb2",
					TableRename:       "db1-tb2-rename",
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
				},
				{
					TableName:         "tb3",
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
				},
			},
		},
		{
			TableSchema: "db2",
			Tables: []*common.Table{
				{
					TableName:   "tb1",
					TableRename: "db2-tb1-rename",
					TableSchema: "db2",
				},
			},
		},
		{
			TableSchema:       "db3",
			TableSchemaRename: "db3-rename",
			Tables: []*common.Table{
				{
					TableName:   "tb1",
					TableSchema: "db3",
				},
			},
		},
	}

	wantSchemaRenameMap := map[string]string{
		"db1": "db1-rename",
		"db3": "db3-rename",
	}

	wantSchemaToTablesRenameMap := map[string]map[string]string{
		"db1": {
			"tb1": "db1-tb1-rename",
			"tb2": "db1-tb2-rename",
		},
		"db2": {
			"tb1": "db2-tb1-rename",
		},
	}

	binlogReader := &BinlogReader{
		logger:       nil,
		mysqlContext: &common.MySQLDriverConfig{},
	}
	binlogReader.mysqlContext.ReplicateDoDb = replicateDoD

	schemaRenameMap, schemaToTablesRenameMap := binlogReader.generateRenameMaps()

	if !assert.Equal(t, wantSchemaRenameMap, schemaRenameMap, "unexpected schemaRenameMap") {
		t.Errorf("unexpected schemaRenameMap: %v", schemaRenameMap)
	}

	if !assert.Equal(t, wantSchemaToTablesRenameMap, schemaToTablesRenameMap, "unexpected schemaToTablesRenameMap") {
		t.Errorf("unexpected schemaToTablesRenameMap: %v", schemaToTablesRenameMap)
	}

}
