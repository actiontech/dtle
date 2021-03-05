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

			binlogReader.currentReplicateDoDb = tt.args.replicationDoDB
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
	currentReplicateDoDb := []*common.DataSource{
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
	binlogReader.currentReplicateDoDb = currentReplicateDoDb

	schemaRenameMap, schemaToTablesRenameMap := binlogReader.generateRenameMaps()

	if !assert.Equal(t, wantSchemaRenameMap, schemaRenameMap, "unexpected schemaRenameMap") {
		t.Errorf("unexpected schemaRenameMap: %v", schemaRenameMap)
	}

	if !assert.Equal(t, wantSchemaToTablesRenameMap, schemaToTablesRenameMap, "unexpected schemaToTablesRenameMap") {
		t.Errorf("unexpected schemaToTablesRenameMap: %v", schemaToTablesRenameMap)
	}

}

func Test_matchTable(t *testing.T) {
	tableConfigs := []*common.Table{
		{
			TableName: "tb1",
		},
		{
			TableRegex: "(\\w*)tb_rex",
		},
	}

	rawReplicateDoDb := []*common.DataSource{
		{
			TableSchema: "db1",
			Tables:      tableConfigs,
		},
		{
			TableSchema: "db2",
		},
		{
			TableSchemaRegex: "(\\w*)db_rex1",
		},
	}

	type args struct {
		schemaName string
		tableName  string
	}
	tests := []struct {
		name       string
		args       args
		wantResult bool
	}{
		{
			name: "match_schema",
			args: args{
				schemaName: "db1",
			},
			wantResult: true,
		},
		{
			name: "match_schema",
			args: args{
				schemaName: "db2",
				tableName:  "",
			},
			wantResult: true,
		},
		{
			name: "match_schema_rex",
			args: args{
				schemaName: "testdb_rex1",
				tableName:  "",
			},
			wantResult: true,
		},
		{
			name: "match_table",
			args: args{
				schemaName: "db1",
				tableName:  "tb1",
			},
			wantResult: true,
		},
		{
			name: "match_table_rex",
			args: args{
				schemaName: "db1",
				tableName:  "testtb_rex",
			},
			wantResult: true,
		},
		{
			name: "match_table",
			args: args{
				schemaName: "db2",
				tableName:  "testtb",
			},
			wantResult: true,
		},
		{
			name: "skip_schema",
			args: args{
				schemaName: "db_not_match",
			},
			wantResult: false,
		},
		{
			name: "skip_table",
			args: args{
				schemaName: "db1",
				tableName:  "tb2",
			},
			wantResult: false,
		},
	}

	binlogReader := &BinlogReader{
		mysqlContext: &common.MySQLDriverConfig{},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if res := binlogReader.matchTable(rawReplicateDoDb, tt.args.schemaName, tt.args.tableName); res != tt.wantResult {
				t.Errorf("matchTable() gotResult = %v, want %v", res, tt.wantResult)
			}
		})
	}
}

func Test_skipQueryDDL(t *testing.T) {
	rawReplicateDoDb := []*common.DataSource{
		{
			TableSchema: "db1",
			Tables: []*common.Table{
				{
					TableName: "tb1",
				},
				{
					TableName: "tb2",
				},
			},
		},
		{
			TableSchema: "db2",
		},
		{
			TableSchema: "db3",
			Tables: []*common.Table{
				{
					TableName: "tb1",
				},
			},
		},
		{
			TableSchema: "db4",
			Tables: []*common.Table{
				{
					TableName: "tb1",
				},
			},
		},
	}

	rawReplicateIgnoreDb := []*common.DataSource{
		{
			TableSchema: "db1",
			Tables: []*common.Table{
				{
					TableName: "tb1",
				},
			},
		},
		{
			TableSchema: "db2",
			Tables: []*common.Table{
				{
					TableName: "tb-skip",
				},
			},
		},
		{
			TableSchema: "db3",
		},
		{
			TableSchema: "db4",
			Tables: []*common.Table{
				{
					TableName: "tb1",
				},
			},
		},
	}

	type args struct {
		schemaName string
		tableName  string
	}
	tests := []struct {
		name       string
		args       args
		wantResult bool
	}{
		{
			name: "replicateDoDb-tables/replicateIgnoreDb-table/match-schema",
			args: args{
				schemaName: "db1",
			},
			wantResult: false,
		},
		{
			name: "replicateDoDb-tables/replicateIgnoreDb-table/skip-table",
			args: args{
				schemaName: "db1",
				tableName:  "tb1",
			},
			wantResult: true,
		},
		{
			name: "replicateDoDb-tables/replicateIgnoreDb-table/match-table",
			args: args{
				schemaName: "db1",
				tableName:  "tb2",
			},
			wantResult: false,
		},
		{
			name: "replicateDoDb-schema/replicateIgnoreDb-table/match-schema",
			args: args{
				schemaName: "db2",
			},
			wantResult: false,
		},
		{
			name: "replicateDoDb-schema/replicateIgnoreDb-table/skip-table",
			args: args{
				schemaName: "db2",
				tableName:  "tb-skip",
			},
			wantResult: true,
		},
		{
			name: "replicateDoDb-table/replicateIgnoreDb-schema/skip-schema",
			args: args{
				schemaName: "db3",
			},
			wantResult: true,
		},
		{
			name: "replicateDoDb-table/replicateIgnoreDb-schema/skip-table",
			args: args{
				schemaName: "db3",
				tableName:  "tb1",
			},
			wantResult: true,
		},
		{
			name: "replicateDoDb-table/replicateIgnoreDb-table/skip-table",
			args: args{
				schemaName: "db4",
				tableName:  "tb1",
			},
			wantResult: true,
		},
		{
			name: "replicateDoDb-table/replicateIgnoreDb-table/match-schema",
			args: args{
				schemaName: "db4",
			},
			wantResult: false,
		},
	}

	binlogReader := &BinlogReader{
		mysqlContext: &common.MySQLDriverConfig{},
	}
	binlogReader.mysqlContext.ReplicateIgnoreDb = rawReplicateIgnoreDb
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binlogReader.mysqlContext.ReplicateDoDb = rawReplicateDoDb
			res := binlogReader.skipQueryDDL(tt.args.schemaName, tt.args.tableName)
			if res != tt.wantResult {
				t.Errorf("skipQueryDDL() gotResult = %v, want %v", res, tt.wantResult)
			}

			binlogReader.mysqlContext.ReplicateDoDb = []*common.DataSource{}
			res = binlogReader.skipQueryDDL(tt.args.schemaName, tt.args.tableName)
			if res != tt.wantResult {
				t.Errorf("skipQueryDDL() with empty replicateDoDb gotResult = %v, want %v", res, tt.wantResult)
			}
		})
	}
}

func Test_updateCurrentReplicateDoDb(t *testing.T) {
	tableConfigs := []*common.Table{
		{TableName: "tb1", TableRename: "tb1-rename"},
		{TableName: "tb2", TableRename: ""},
		{TableRegex: "(\\w*)tb-rex1", TableRename: "tb${1}-rex1-rename"},
		{TableRegex: "(\\w*)tb-rex2", TableRename: "tb${1}-rex2-rename"},
		{TableRegex: "(\\w*)tb-rex3", TableRename: ""},
	}

	rawReplicateDoDb := []*common.DataSource{
		{TableSchema: "db1", TableSchemaRename: "db1-rename", Tables: tableConfigs},
		{TableSchema: "db2", TableSchemaRename: "db2-rename", Tables: []*common.Table{}},
		{TableSchema: "db3", TableSchemaRename: "", Tables: tableConfigs},
		{TableSchemaRegex: "(\\w*)db-rex1", TableSchemaRename: "db${1}-rex1-rename", Tables: tableConfigs},
		{TableSchemaRegex: "(\\w*)db-rex2", TableSchemaRename: "db${1}-rex2-rename", Tables: []*common.Table{}},
		{TableSchemaRegex: "(\\w*)db-rex3", TableSchemaRename: "", Tables: tableConfigs},
	}

	type args struct {
		schema    string
		tableName string
	}

	tests := []struct {
		name                 string
		rawReplicateDoDb     []*common.DataSource
		currentReplicateDoDb []*common.DataSource
		args                 args
		want                 []*common.DataSource
	}{
		{
			name:                 "empty-rawReplicateDoDb/empty-currentReplicateDoDb/input-new-table",
			rawReplicateDoDb:     []*common.DataSource{},
			currentReplicateDoDb: []*common.DataSource{},
			args:                 args{schema: "db1", tableName: "tb1"},
			want: []*common.DataSource{
				{
					TableSchema: "db1",
					Tables: []*common.Table{
						{TableName: "tb1", TableSchema: "db1", Where: "true"},
					},
				},
			},
		},
		{
			name:             "empty-rawReplicateDoDb/exists-one-table/input-new-table",
			rawReplicateDoDb: []*common.DataSource{},
			currentReplicateDoDb: []*common.DataSource{
				{
					TableSchema: "db1",
					Tables: []*common.Table{
						{TableName: "tb1", TableSchema: "db1", Where: "true"},
					},
				}},
			args: args{schema: "db1", tableName: "tb2"},
			want: []*common.DataSource{
				{
					TableSchema: "db1",
					Tables: []*common.Table{
						{TableName: "tb1", TableSchema: "db1", Where: "true"},
						{TableName: "tb2", TableSchema: "db1", Where: "true"},
					},
				},
			},
		},
		{
			name:             "empty-rawReplicateDoDb/exists-one-table/input-existed-table",
			rawReplicateDoDb: []*common.DataSource{},
			currentReplicateDoDb: []*common.DataSource{
				{
					TableSchema: "db1",
					Tables: []*common.Table{
						{TableName: "tb1", TableSchema: "db1", Where: "true"},
					},
				}},
			args: args{schema: "db1", tableName: "tb1"},
			want: []*common.DataSource{
				{
					TableSchema: "db1",
					Tables: []*common.Table{
						{TableName: "tb1", TableSchema: "db1", Where: "true"},
					},
				},
			},
		},
		{
			name:             "empty-rawReplicateDoDb/exists-one-schema/input-existed-schema",
			rawReplicateDoDb: []*common.DataSource{},
			currentReplicateDoDb: []*common.DataSource{
				{
					TableSchema: "db1",
				},
			},
			args: args{schema: "db1", tableName: ""},
			want: []*common.DataSource{
				{
					TableSchema: "db1",
				},
			},
		},
		{
			name:             "empty-rawReplicateDoDb/exists-one-schema/input-new-table-to-existed-schema",
			rawReplicateDoDb: []*common.DataSource{},
			currentReplicateDoDb: []*common.DataSource{{
				TableSchema: "db1",
			}},
			args: args{schema: "db1", tableName: "tb1"},
			want: []*common.DataSource{
				{
					TableSchema: "db1",
					Tables: []*common.Table{
						{TableName: "tb1", TableSchema: "db1", Where: "true"},
					},
				},
			},
		},
		{
			name:                 "empty-currentReplicateDoDb/input-table",
			rawReplicateDoDb:     rawReplicateDoDb,
			currentReplicateDoDb: []*common.DataSource{},
			args:                 args{schema: "db1", tableName: "tb1"},
			want: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
					Tables: []*common.Table{
						{TableName: "tb1", TableRename: "tb1-rename", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
					},
				},
			},
		},
		{
			name:                 "empty-currentReplicateDoDb/input-table-match-regex",
			rawReplicateDoDb:     rawReplicateDoDb,
			currentReplicateDoDb: []*common.DataSource{},
			args:                 args{schema: "db1", tableName: "testtb-rex1"},
			want: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
					Tables: []*common.Table{
						{TableName: "testtb-rex1", TableRegex: "(\\w*)tb-rex1", TableRename: "tbtest-rex1-rename", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
					},
				},
			},
		},
		{
			name:                 "empty-currentReplicateDoDb/input-table-match-regex-without-rename",
			rawReplicateDoDb:     rawReplicateDoDb,
			currentReplicateDoDb: []*common.DataSource{},
			args:                 args{schema: "testdb-rex3", tableName: "testtb-rex3"},
			want: []*common.DataSource{
				{
					TableSchema:       "testdb-rex3",
					TableSchemaRegex:  "(\\w*)db-rex3",
					TableSchemaRename: "",
					Tables: []*common.Table{
						{TableName: "testtb-rex3", TableRegex: "(\\w*)tb-rex3", TableRename: "", TableSchema: "testdb-rex3", TableSchemaRename: "", Where: "true"},
					},
				},
			},
		},
		{
			name:                 "empty-currentReplicateDoDb/input-schema",
			rawReplicateDoDb:     rawReplicateDoDb,
			currentReplicateDoDb: []*common.DataSource{},
			args:                 args{schema: "db1", tableName: ""},
			want: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
				},
			},
		},
		{
			name:                 "empty-currentReplicateDoDb/input-not-match-table",
			rawReplicateDoDb:     rawReplicateDoDb,
			currentReplicateDoDb: []*common.DataSource{},
			args:                 args{schema: "db1", tableName: "tb-not-match"},
			want: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
				},
			},
		},
		{
			name:                 "empty-currentReplicateDoDb/input-not-match-schema",
			rawReplicateDoDb:     rawReplicateDoDb,
			currentReplicateDoDb: []*common.DataSource{},
			args:                 args{schema: "db-not-match", tableName: "tb1"},
			want:                 []*common.DataSource{},
		},
		{
			name:             "input-existed-table",
			rawReplicateDoDb: rawReplicateDoDb,
			currentReplicateDoDb: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
					Tables: []*common.Table{
						{TableName: "tb1", TableRename: "tb1-rename", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
					},
				},
			},
			args: args{schema: "db1", tableName: "tb1"},
			want: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
					Tables: []*common.Table{
						{TableName: "tb1", TableRename: "tb1-rename", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
					},
				},
			},
		},
		{
			name:             "input-existed-table-match-regex",
			rawReplicateDoDb: rawReplicateDoDb,
			currentReplicateDoDb: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
					Tables: []*common.Table{
						{TableName: "testtb-rex1", TableRegex: "(\\w*)tb-rex1", TableRename: "tbtest-rex1-rename", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
					},
				},
			},
			args: args{schema: "db1", tableName: "testtb-rex1"},
			want: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
					Tables: []*common.Table{
						{TableName: "testtb-rex1", TableRegex: "(\\w*)tb-rex1", TableRename: "tbtest-rex1-rename", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
					},
				},
			},
		},
		{
			name:             "input-new-table",
			rawReplicateDoDb: rawReplicateDoDb,
			currentReplicateDoDb: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
					Tables: []*common.Table{
						{TableName: "tb1", TableRename: "tb1-rename", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
					},
				},
			},
			args: args{schema: "db1", tableName: "tb2"},
			want: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
					Tables: []*common.Table{
						{TableName: "tb1", TableRename: "tb1-rename", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
						{TableName: "tb2", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
					},
				},
			},
		},
		{
			// a table has existed and then add new table that match the same regex
			name:             "input-new-table-match-regex",
			rawReplicateDoDb: rawReplicateDoDb,
			currentReplicateDoDb: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
					Tables: []*common.Table{
						{TableName: "testtb-rex1", TableRegex: "(\\w*)tb-rex1", TableRename: "tbtest-rex1-rename", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
					},
				},
			},
			args: args{schema: "db1", tableName: "test2tb-rex1"},
			want: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
					Tables: []*common.Table{
						{TableName: "testtb-rex1", TableRegex: "(\\w*)tb-rex1", TableRename: "tbtest-rex1-rename", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
						{TableName: "test2tb-rex1", TableRegex: "(\\w*)tb-rex1", TableRename: "tbtest2-rex1-rename", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
					},
				},
			},
		},
		{
			// a table has existed and then add new table that match the different regex
			name:             "input-new-table-match-regex",
			rawReplicateDoDb: rawReplicateDoDb,
			currentReplicateDoDb: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
					Tables: []*common.Table{
						{TableName: "testtb-rex1", TableRegex: "(\\w*)tb-rex1", TableRename: "tbtest-rex1-rename", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
					},
				},
			},
			args: args{schema: "db1", tableName: "testtb-rex2"},
			want: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
					Tables: []*common.Table{
						{TableName: "testtb-rex1", TableRegex: "(\\w*)tb-rex1", TableRename: "tbtest-rex1-rename", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
						{TableName: "testtb-rex2", TableRegex: "(\\w*)tb-rex2", TableRename: "tbtest-rex2-rename", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
					},
				},
			},
		},
		{
			name:             "input-not-match-table",
			rawReplicateDoDb: rawReplicateDoDb,
			currentReplicateDoDb: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
					Tables: []*common.Table{
						{TableName: "tb1", TableRename: "tb1-rename", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
					},
				},
			},
			args: args{schema: "db1", tableName: "tb-not-match"},
			want: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
					Tables: []*common.Table{
						{TableName: "tb1", TableRename: "tb1-rename", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
					},
				},
			},
		},
		{
			name:             "input-existed-schema",
			rawReplicateDoDb: rawReplicateDoDb,
			currentReplicateDoDb: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
				},
			},
			args: args{schema: "db1", tableName: ""},
			want: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
				},
			},
		},
		{
			// regex
			name:             "input-existed-schema-match-regex",
			rawReplicateDoDb: rawReplicateDoDb,
			currentReplicateDoDb: []*common.DataSource{
				{
					TableSchema:       "testdb-rex1",
					TableSchemaRegex:  "(\\w*)db-rex1",
					TableSchemaRename: "dbtest-rex1-rename",
				},
			},
			args: args{schema: "testdb-rex1", tableName: ""},
			want: []*common.DataSource{
				{
					TableSchema:       "testdb-rex1",
					TableSchemaRegex:  "(\\w*)db-rex1",
					TableSchemaRename: "dbtest-rex1-rename",
				},
			},
		},
		{
			name:             "input-new-schema",
			rawReplicateDoDb: rawReplicateDoDb,
			currentReplicateDoDb: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
				},
			},
			args: args{schema: "db2"},
			want: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
				},
				{
					TableSchema:       "db2",
					TableSchemaRename: "db2-rename",
				},
			},
		},
		{
			// a schema has existed and then add new schema that match the same regex
			name:             "input-new-schema-match-regex",
			rawReplicateDoDb: rawReplicateDoDb,
			currentReplicateDoDb: []*common.DataSource{
				{
					TableSchema:       "test1db-rex1",
					TableSchemaRegex:  "(\\w*)db-rex1",
					TableSchemaRename: "dbtest1-rex1-rename",
				},
			},
			args: args{schema: "test2db-rex1"},
			want: []*common.DataSource{
				{
					TableSchema:       "test1db-rex1",
					TableSchemaRegex:  "(\\w*)db-rex1",
					TableSchemaRename: "dbtest1-rex1-rename",
				},
				{
					TableSchema:       "test2db-rex1",
					TableSchemaRegex:  "(\\w*)db-rex1",
					TableSchemaRename: "dbtest2-rex1-rename",
				},
			},
		},
		{
			// a schema has existed and then add new schema that match the different regex
			name:             "input-new-schema-match-regex",
			rawReplicateDoDb: rawReplicateDoDb,
			currentReplicateDoDb: []*common.DataSource{
				{
					TableSchema:       "test1db-rex1",
					TableSchemaRegex:  "(\\w*)db-rex1",
					TableSchemaRename: "dbtest1-rex1-rename",
				},
			},
			args: args{schema: "test1db-rex2"},
			want: []*common.DataSource{
				{
					TableSchema:       "test1db-rex1",
					TableSchemaRegex:  "(\\w*)db-rex1",
					TableSchemaRename: "dbtest1-rex1-rename",
				},
				{
					TableSchema:       "test1db-rex2",
					TableSchemaRegex:  "(\\w*)db-rex2",
					TableSchemaRename: "dbtest1-rex2-rename",
				},
			},
		},
		{
			name:             "input-not-match-table",
			rawReplicateDoDb: rawReplicateDoDb,
			currentReplicateDoDb: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
					Tables: []*common.Table{
						{TableName: "tb1", TableRename: "tb1-rename", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
					},
				},
			},
			args: args{schema: "db1", tableName: "tb-not-match"},
			want: []*common.DataSource{
				{
					TableSchema:       "db1",
					TableSchemaRename: "db1-rename",
					Tables: []*common.Table{
						{TableName: "tb1", TableRename: "tb1-rename", TableSchema: "db1", TableSchemaRename: "db1-rename", Where: "true"},
					},
				},
			},
		},
	}

	binlogReader := &BinlogReader{
		logger: hclog.New(&hclog.LoggerOptions{
			Level:      hclog.Debug,
			JSONFormat: true,
		}),
		mysqlContext: &common.MySQLDriverConfig{},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			binlogReader.mysqlContext.ReplicateDoDb = test.rawReplicateDoDb
			binlogReader.currentReplicateDoDb = test.currentReplicateDoDb
			if err := binlogReader.updateCurrentReplicateDoDb(test.args.schema, test.args.tableName); nil != err {
				t.Error(err)
				return
			}

			if !assert.Equal(t, test.want, binlogReader.currentReplicateDoDb, "unexpected currentReplicateDoDb") {
				printObject := func(db *common.DataSource, prefix string) {
					t.Errorf("%v: &{TableSchema: %v TableSchemaRegex: %v TableSchemaRename: %v}\n", prefix, db.TableSchema, db.TableSchemaRegex, db.TableSchemaRename)
					for _, tb := range db.Tables {
						t.Errorf("table: %+v\n", tb)
					}
				}

				for _, db := range binlogReader.currentReplicateDoDb {
					printObject(db, "got current db")
				}

				for _, db := range test.want {
					printObject(db, "want db")
				}
			}
		})

	}

}
