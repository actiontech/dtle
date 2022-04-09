/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package base

import (
	gosql "database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	sqle "github.com/actiontech/dtle/driver/mysql/sqle/inspector"
	"github.com/pingcap/tidb/parser"

	"github.com/actiontech/dtle/driver/common"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	test "github.com/outbrain/golib/tests"
)

func TestStringContainsAll(t *testing.T) {
	s := `insert,delete,update`

	test.S(t).ExpectFalse(StringContainsAll(s))
	test.S(t).ExpectFalse(StringContainsAll(s, ""))
	test.S(t).ExpectFalse(StringContainsAll(s, "drop"))
	test.S(t).ExpectTrue(StringContainsAll(s, "insert"))
	test.S(t).ExpectFalse(StringContainsAll(s, "insert", "drop"))
	test.S(t).ExpectTrue(StringContainsAll(s, "insert", ""))
	test.S(t).ExpectTrue(StringContainsAll(s, "insert", "update", "delete"))
}

func TestPrettifyDurationOutput(t *testing.T) {
	type args struct {
		d time.Duration
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"ZeroNumber", args{0}, "0s"},
		{"negativeNum", args{-10}, "0s"},
		{"lessOneSecond", args{1011212}, "0s"},
		{"oneSecond", args{time.Second}, "1s"},
		{"oneMin", args{time.Minute}, "1m0s"},
		{"oneHour", args{time.Hour}, "1h0m0s"},
		{"1h17m6s", args{time.Hour*1 + time.Minute*17 + time.Second*6}, "1h17m6s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PrettifyDurationOutput(tt.args.d); got != tt.want {
				t.Errorf("PrettifyDurationOutput() = %v, want %v", got, tt.want)
			}
		})
	}
}

//var userAndPwd = "root:pass"
//var testAddr = "127.0.0.1:33061"

func TestGetSelfBinlogCoordinates(t *testing.T) {
	type args struct {
		db *gosql.DB
	}
	//db, err := sql.CreateDB(fmt.Sprintf("%s@(%s)/?timeout=5s&tls=false&autocommit=true&charset=utf8mb4,utf8,latin1&multiStatements=true", userAndPwd, testAddr))
	//if err != nil {
	//	return
	//}
	tests := []struct {
		name                      string
		args                      args
		wantSelfBinlogCoordinates *common.MySQLCoordinates
		wantErr                   bool
	}{
		//{name: "T1",
		//	args: args{db},
		//	wantSelfBinlogCoordinates: &BinlogCoordinatesX{
		//		LogFile: "bin.000003",
		//		LogPos:  1110,
		//		GtidSet: "09bceaee-bd29-11eb-8837-0242ac120002:1-9",
		//	},
		//	wantErr: false},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSelfBinlogCoordinates, err := GetSelfBinlogCoordinates(tt.args.db)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetSelfBinlogCoordinates() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotSelfBinlogCoordinates, tt.wantSelfBinlogCoordinates) {
				t.Errorf("GetSelfBinlogCoordinates() = %v, want %v", gotSelfBinlogCoordinates, tt.wantSelfBinlogCoordinates)
			}
		})
	}
}

//docker run --name mysql-src -e MYSQL_ROOT_PASSWORD=pass -p 33061:3306 --network=dtle-net -d mysql:5.7 --gtid-mode=ON --enforce-gtid-consistency=1 --log-bin=bin --server-id=1
//mysql -h 127.0.0.1 -P 33061 -uroot -ppass -e "CREATE DATABASE demo; CREATE TABLE demo.demo_tbl(a int primary key,c_bit_1 bit(1) DEFAULT 1)ENGINE=InnoDB DEFAULT CHARSET=utf8;"
//mysql -h 127.0.0.1 -P 33061 -uroot -ppass -e "insert into demo.demo_tbl values(0,1)"
//mysql -h 127.0.0.1 -P 33061 -uroot -ppass -e "insert into demo.demo_tbl values(1,0)”

func TestGetTableColumns(t *testing.T) {
	type args struct {
		db           *gosql.DB
		databaseName string
		tableName    string
	}
	//db, err := sql.CreateDB(fmt.Sprintf("%s@(%s)/?timeout=5s&tls=false&autocommit=true&charset=utf8mb4,utf8,latin1&multiStatements=true", userAndPwd, testAddr))
	//if err != nil {
	//	return
	//}
	tests := []struct {
		name    string
		args    args
		want    *common.ColumnList
		wantErr bool
	}{
		//{name: "T", args: args{
		//	db:           db,
		//	databaseName: "demo",
		//	tableName:    "demo_tbl"},
		//	want: &common.ColumnList{
		//		Columns: []mysqlconfig.Column{
		//			mysqlconfig.Column{
		//				RawName:            "a",
		//				EscapedName:        "`a`",
		//				IsUnsigned:         false,
		//				Charset:            "",
		//				Type:               0,
		//				Default:            "",
		//				ColumnType:         "int(11)",
		//				Key:                "PRI",
		//				TimezoneConversion: nil,
		//				Nullable:           false,
		//				Precision:          0,
		//				Scale:              0},
		//			mysqlconfig.Column{
		//				RawName:            "c_bit_1",
		//				EscapedName:        "`c_bit_1`",
		//				IsUnsigned:         false,
		//				Charset:            "",
		//				Type:               0,
		//				Default:            "b'1'",
		//				ColumnType:         "bit(1)",
		//				Key:                "",
		//				TimezoneConversion: nil,
		//				Nullable:           true,
		//				Precision:          0,
		//				Scale:              0},
		//		},
		//		Ordinals: mysqlconfig.ColumnsMap{
		//			"a":       0,
		//			"c_bit_1": 1,
		//		}},
		//	wantErr: false},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetTableColumns(tt.args.db, tt.args.databaseName, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetTableColumns() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetTableColumns() = %v, want %v", got, tt.want)
			}
		})
	}
}

//func TestGetTableColumns2(t *testing.T) {
//	db, err := sql.CreateDB(fmt.Sprintf("%s@(%s)/?timeout=5s&tls=false&autocommit=true&charset=utf8mb4,utf8,latin1&multiStatements=true", "root:password", "10.186.62.40:3307"))
//	if err != nil {
//		t.Error(err)
//	}
//	cl, err := GetTableColumns(db, "a", "a939")
//	if err != nil {
//		t.Error(err)
//	}
//	for _, col := range cl.Columns {
//		fmt.Printf("%v default '%v'\n", col.RawName, col.Default)
//	}
//}

func TestApplyColumnTypes(t *testing.T) {
	type args struct {
		db           *gosql.Tx
		database     string
		tablename    string
		columnsLists []*common.ColumnList
	}
	tests := []struct {
		name    string
		args    args
		want    []*common.ColumnList
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ApplyColumnTypes(tt.args.db, tt.args.database, tt.args.tablename, tt.args.columnsLists...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ApplyColumnTypes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(tt.args.columnsLists, tt.want) {
				t.Errorf("ApplyColumnTypes() = %v, want %v", tt.args.columnsLists, tt.want)
			}
		})
	}
}

func Test_stringInterval(t *testing.T) {
	type args struct {
		intervals gomysql.IntervalSlice
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "T1", args: args{
			intervals: gomysql.IntervalSlice{},
		}, want: ""},
		{name: "T1", args: args{
			intervals: gomysql.IntervalSlice{gomysql.Interval{1, 11}, gomysql.Interval{9, 89}},
		}, want: "1-10:9-88"},
		{name: "T1", args: args{
			intervals: gomysql.IntervalSlice{gomysql.Interval{1, 31}, gomysql.Interval{33, 89}},
		}, want: "1-30:33-88"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StringInterval(tt.args.intervals); got != tt.want {
				t.Errorf("stringInterval() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetTableColumnsSqle(t *testing.T) {
	// Not a real test. Just printing intermediate result.
	sqleCtx := sqle.NewContext(nil)
	sqleCtx.AddSchema("a"); sqleCtx.LoadSchemas(nil); sqleCtx.LoadTables("a", nil)
	p := parser.New()
	sqls := []string{
		"create table a.text_columns(id int(11) not null primary key,c_text longtext)",
		"create table a.binary_columns(id int(11) not null primary key, c_binary varbinary(255))",
	}
	for i := range sqls {
		stmt, err := p.ParseOneStmt(sqls[i], "", "")
		if err != nil {
			t.Error(err)
		}
		sqleCtx.UpdateContext(stmt, "mysql")
	}

	type args struct {
		sqleContext *sqle.Context
		schema      string
		table       string
	}
	tests := []struct {
		name    string
		args    args
		want    *common.ColumnList
		wantErr bool
	}{
		{
			name:    "column-text",
			args:    args{
				sqleContext: sqleCtx,
				schema:      "a",
				table:       "text_columns",
			},
			want:    nil,
			wantErr: false,
		}, {
			name:    "column-binary",
			args:    args{
				sqleContext: sqleCtx,
				schema:      "a",
				table:       "binary_columns",
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := GetTableColumnsSqle(tt.args.sqleContext, tt.args.schema, tt.args.table)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetTableColumnsSqle() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			fmt.Printf("table %v\n", tt.args.table)
			for i := range got.Columns {
				println(got.Columns[i].Type)
			}
		})
	}
}

func TestRenameCreateTable(t *testing.T) {
	type args struct {
		createTable string
		newSchema   string
		newTable    string
		columnMap   []string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name:    "1",
			args:    args{
				createTable: "create table s.t (id int primary key, val int)",
				newSchema:   "s1",
				newTable:    "t1",
				columnMap:   []string{"val", "id"},
			},
			want:    "CREATE TABLE `s1`.`t1` (`val` INT,`id` INT PRIMARY KEY)",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RenameCreateTable(tt.args.createTable, tt.args.newSchema, tt.args.newTable, tt.args.columnMap)
			if (err != nil) != tt.wantErr {
				t.Errorf("RenameCreateTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("RenameCreateTable() got = %v, want %v", got, tt.want)
			}
		})
	}
}
