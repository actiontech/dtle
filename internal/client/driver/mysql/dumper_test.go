package mysql

import (
	"database/sql"
	"reflect"
	"testing"
	"udup/internal/config"
	log "udup/internal/logger"
)

func TestNewDumper(t *testing.T) {
	type args struct {
		db        *sql.Tx
		dbName    string
		tableName string
		logger    *log.Entry
	}
	tests := []struct {
		name string
		args args
		want *dumper
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewDumper(tt.args.db, tt.args.dbName, tt.args.tableName, tt.args.logger); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewDumper() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_dumper_getRowsCount(t *testing.T) {
	tests := []struct {
		name    string
		d       *dumper
		want    uint64
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.d.getRowsCount()
			if (err != nil) != tt.wantErr {
				t.Errorf("dumper.getRowsCount() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("dumper.getRowsCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_dumpEntry_incrementCounter(t *testing.T) {
	tests := []struct {
		name string
		e    *dumpEntry
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.e.incrementCounter()
		})
	}
}

func Test_dumper_getDumpEntries(t *testing.T) {
	tests := []struct {
		name    string
		d       *dumper
		want    []*dumpEntry
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.d.getDumpEntries()
			if (err != nil) != tt.wantErr {
				t.Errorf("dumper.getDumpEntries() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("dumper.getDumpEntries() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_dumper_getChunkData(t *testing.T) {
	type args struct {
		e *dumpEntry
	}
	tests := []struct {
		name    string
		d       *dumper
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.d.getChunkData(tt.args.e); (err != nil) != tt.wantErr {
				t.Errorf("dumper.getChunkData() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_dumpEntry_escape(t *testing.T) {
	type args struct {
		colValue string
	}
	tests := []struct {
		name string
		e    *dumpEntry
		args args
		want string
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.escape(tt.args.colValue); got != tt.want {
				t.Errorf("dumpEntry.escape() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_dumper_worker(t *testing.T) {
	tests := []struct {
		name string
		d    *dumper
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.d.worker()
		})
	}
}

func Test_dumper_Dump(t *testing.T) {
	type args struct {
		w int
	}
	tests := []struct {
		name    string
		d       *dumper
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.d.Dump(tt.args.w); (err != nil) != tt.wantErr {
				t.Errorf("dumper.Dump() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_showDatabases(t *testing.T) {
	type args struct {
		db *sql.DB
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := showDatabases(tt.args.db)
			if (err != nil) != tt.wantErr {
				t.Errorf("showDatabases() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("showDatabases() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_showTables(t *testing.T) {
	type args struct {
		db     *sql.DB
		dbName string
	}
	tests := []struct {
		name       string
		args       args
		wantTables []*config.Table
		wantErr    bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTables, err := showTables(tt.args.db, tt.args.dbName)
			if (err != nil) != tt.wantErr {
				t.Errorf("showTables() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotTables, tt.wantTables) {
				t.Errorf("showTables() = %v, want %v", gotTables, tt.wantTables)
			}
		})
	}
}

func Test_dumper_Close(t *testing.T) {
	tests := []struct {
		name    string
		d       *dumper
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.d.Close(); (err != nil) != tt.wantErr {
				t.Errorf("dumper.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
