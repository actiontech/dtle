package base

import (
	gosql "database/sql"
	"reflect"
	"testing"
	"time"
	umconf "udup/internal/config/mysql"

	test "github.com/outbrain/golib/tests"
	gomysql "github.com/siddontang/go-mysql/mysql"
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
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PrettifyDurationOutput(tt.args.d); got != tt.want {
				t.Errorf("PrettifyDurationOutput() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFileExists(t *testing.T) {
	type args struct {
		fileName string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FileExists(tt.args.fileName); got != tt.want {
				t.Errorf("FileExists() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewNoReplicationLagResult(t *testing.T) {
	tests := []struct {
		name string
		want *ReplicationLagResult
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewNoReplicationLagResult(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewNoReplicationLagResult() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReplicationLagResult_HasLag(t *testing.T) {
	type fields struct {
		Key umconf.InstanceKey
		Lag time.Duration
		Err error
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &ReplicationLagResult{
				Key: tt.fields.Key,
				Lag: tt.fields.Lag,
				Err: tt.fields.Err,
			}
			if got := r.HasLag(); got != tt.want {
				t.Errorf("ReplicationLagResult.HasLag() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetReplicationLag(t *testing.T) {
	type args struct {
		connectionConfig *umconf.ConnectionConfig
	}
	tests := []struct {
		name               string
		args               args
		wantReplicationLag time.Duration
		wantErr            bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotReplicationLag, err := GetReplicationLag(tt.args.connectionConfig)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetReplicationLag() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotReplicationLag != tt.wantReplicationLag {
				t.Errorf("GetReplicationLag() = %v, want %v", gotReplicationLag, tt.wantReplicationLag)
			}
		})
	}
}

func TestGetReplicationBinlogCoordinates(t *testing.T) {
	type args struct {
		db *gosql.DB
	}
	tests := []struct {
		name                         string
		args                         args
		wantReadBinlogCoordinates    *BinlogCoordinates
		wantExecuteBinlogCoordinates *BinlogCoordinates
		wantErr                      bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotReadBinlogCoordinates, gotExecuteBinlogCoordinates, err := GetReplicationBinlogCoordinates(tt.args.db)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetReplicationBinlogCoordinates() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotReadBinlogCoordinates, tt.wantReadBinlogCoordinates) {
				t.Errorf("GetReplicationBinlogCoordinates() gotReadBinlogCoordinates = %v, want %v", gotReadBinlogCoordinates, tt.wantReadBinlogCoordinates)
			}
			if !reflect.DeepEqual(gotExecuteBinlogCoordinates, tt.wantExecuteBinlogCoordinates) {
				t.Errorf("GetReplicationBinlogCoordinates() gotExecuteBinlogCoordinates = %v, want %v", gotExecuteBinlogCoordinates, tt.wantExecuteBinlogCoordinates)
			}
		})
	}
}

func TestGetSelfBinlogCoordinates(t *testing.T) {
	type args struct {
		db *gosql.DB
	}
	tests := []struct {
		name                      string
		args                      args
		wantSelfBinlogCoordinates *BinlogCoordinates
		wantErr                   bool
	}{
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

func TestGetTableColumns(t *testing.T) {
	type args struct {
		db           *gosql.DB
		databaseName string
		tableName    string
	}
	tests := []struct {
		name    string
		args    args
		want    *umconf.ColumnList
		wantErr bool
	}{
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

func TestGetTableColumnsWithTx(t *testing.T) {
	type args struct {
		db           *gosql.Tx
		databaseName string
		tableName    string
	}
	tests := []struct {
		name    string
		args    args
		want    *umconf.ColumnList
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetTableColumnsWithTx(tt.args.db, tt.args.databaseName, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetTableColumnsWithTx() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetTableColumnsWithTx() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplyColumnTypes(t *testing.T) {
	type args struct {
		db           *gosql.Tx
		database     string
		tablename    string
		columnsLists []*umconf.ColumnList
	}
	tests := []struct {
		name    string
		args    args
		want    []*umconf.ColumnList
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ApplyColumnTypes(tt.args.db, tt.args.database, tt.args.tablename, tt.args.columnsLists...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ApplyColumnTypes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ApplyColumnTypes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestShowCreateTable(t *testing.T) {
	type args struct {
		db                *gosql.DB
		databaseName      string
		tableName         string
		dropTableIfExists bool
	}
	tests := []struct {
		name                     string
		args                     args
		wantCreateTableStatement string
		wantErr                  bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCreateTableStatement, err := ShowCreateTable(tt.args.db, tt.args.databaseName, tt.args.tableName, tt.args.dropTableIfExists)
			if (err != nil) != tt.wantErr {
				t.Errorf("ShowCreateTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotCreateTableStatement != tt.wantCreateTableStatement {
				t.Errorf("ShowCreateTable() = %v, want %v", gotCreateTableStatement, tt.wantCreateTableStatement)
			}
		})
	}
}

func TestContrastGtidSet(t *testing.T) {
	type args struct {
		contrastGtid string
		currentGtid  string
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
	// TODO: Add test cases.
		{"T1", args{"0ebdc0a2-7439-11e7-b49b-0242ac110004:1-206","0ebdc0a2-7439-11e7-b49b-0242ac110004:1-206"},true, false},
		{"T2", args{"0ebdc0a2-7439-11e7-b49b-0242ac110004:1-206,134c2318-7439-11e7-b57f-0242ac110003:1-3120","0ebdc0a2-7439-11e7-b49b-0242ac110004:1-206"},true, false},
		{"T3", args{"0ebdc0a2-7439-11e7-b49b-0242ac110004:1-206,134c2318-7439-11e7-b57f-0242ac110003:1-3120","0ebdc0a2-7439-11e7-b49b-0242ac110004:1-206,134c2318-7439-11e7-b57f-0242ac110003:1-3120:454591-459264:459266:459270"},false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ContrastGtidSet(tt.args.contrastGtid, tt.args.currentGtid)
			if (err != nil) != tt.wantErr {
				t.Errorf("ContrastGtidSet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ContrastGtidSet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseInterval(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name    string
		args    args
		wantI   gomysql.Interval
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotI, err := parseInterval(tt.args.str)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseInterval() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotI, tt.wantI) {
				t.Errorf("parseInterval() = %v, want %v", gotI, tt.wantI)
			}
		})
	}
}
