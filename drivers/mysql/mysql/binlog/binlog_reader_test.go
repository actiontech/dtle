package binlog

import (
	"github.com/pingcap/parser"
	"testing"
)

func Test_loadMapping(t *testing.T) {
	type args struct {
		sql           string
		beforeName    string
		afterName     string
		mappingType   string
		currentSchema string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "schema-rename-drop-table", args: args{
			sql:           "drop table `a`.`c`",
			beforeName:    "a",
			afterName:     "b",
			mappingType:   "schemaRename",
			currentSchema: "",
		}, want: "DROP TABLE `b`.`c`"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.New().ParseOneStmt(tt.args.sql, "", "")
			if err != nil {
				t.Error(err)
			}

			if got := loadMapping(tt.args.sql, tt.args.beforeName, tt.args.afterName, tt.args.mappingType, tt.args.currentSchema, stmt); got != tt.want {
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
