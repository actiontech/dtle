package mysql

import (
	"reflect"
	"testing"
	uconf "udup/internal/config"
	umconf "udup/internal/config/mysql"
	log "udup/internal/logger"
)

func TestNewInspector(t *testing.T) {
	type args struct {
		ctx    *uconf.MySQLDriverConfig
		logger *log.Entry
	}
	tests := []struct {
		name string
		args args
		want *Inspector
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewInspector(tt.args.ctx, tt.args.logger); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewInspector() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInspector_InitDBConnections(t *testing.T) {
	tests := []struct {
		name    string
		i       *Inspector
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.i.InitDBConnections(); (err != nil) != tt.wantErr {
				t.Errorf("Inspector.InitDBConnections() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInspector_ValidateOriginalTable(t *testing.T) {
	type args struct {
		databaseName string
		tableName    string
	}
	tests := []struct {
		name    string
		i       *Inspector
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var table uconf.Table
			if err := tt.i.ValidateOriginalTable(tt.args.databaseName, tt.args.tableName, &table); (err != nil) != tt.wantErr {
				t.Errorf("Inspector.ValidateOriginalTable() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInspector_InspectTableColumnsAndUniqueKeys(t *testing.T) {
	type args struct {
		databaseName string
		tableName    string
	}
	tests := []struct {
		name           string
		i              *Inspector
		args           args
		wantColumns    *umconf.ColumnList
		wantUniqueKeys [](*umconf.UniqueKey)
		wantErr        bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotColumns, gotUniqueKeys, err := tt.i.InspectTableColumnsAndUniqueKeys(tt.args.databaseName, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("Inspector.InspectTableColumnsAndUniqueKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotColumns, tt.wantColumns) {
				t.Errorf("Inspector.InspectTableColumnsAndUniqueKeys() gotColumns = %v, want %v", gotColumns, tt.wantColumns)
			}
			if !reflect.DeepEqual(gotUniqueKeys, tt.wantUniqueKeys) {
				t.Errorf("Inspector.InspectTableColumnsAndUniqueKeys() gotUniqueKeys = %v, want %v", gotUniqueKeys, tt.wantUniqueKeys)
			}
		})
	}
}

func TestInspector_validateConnection(t *testing.T) {
	tests := []struct {
		name    string
		i       *Inspector
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.i.validateConnection(); (err != nil) != tt.wantErr {
				t.Errorf("Inspector.validateConnection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInspector_validateGrants(t *testing.T) {
	tests := []struct {
		name    string
		i       *Inspector
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.i.validateGrants(); (err != nil) != tt.wantErr {
				t.Errorf("Inspector.validateGrants() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInspector_validateGTIDMode(t *testing.T) {
	tests := []struct {
		name    string
		i       *Inspector
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.i.validateGTIDMode(); (err != nil) != tt.wantErr {
				t.Errorf("Inspector.validateGTIDMode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInspector_validateBinlogs(t *testing.T) {
	tests := []struct {
		name    string
		i       *Inspector
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.i.validateBinlogs(); (err != nil) != tt.wantErr {
				t.Errorf("Inspector.validateBinlogs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInspector_validateLogSlaveUpdates(t *testing.T) {
	tests := []struct {
		name    string
		i       *Inspector
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.i.validateLogSlaveUpdates(); (err != nil) != tt.wantErr {
				t.Errorf("Inspector.validateLogSlaveUpdates() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInspector_validateTable(t *testing.T) {
	type args struct {
		databaseName string
		tableName    string
	}
	tests := []struct {
		name    string
		i       *Inspector
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.i.validateTable(tt.args.databaseName, tt.args.tableName); (err != nil) != tt.wantErr {
				t.Errorf("Inspector.validateTable() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInspector_validateTableForeignKeys(t *testing.T) {
	type args struct {
		databaseName          string
		tableName             string
		allowChildForeignKeys bool
	}
	tests := []struct {
		name    string
		i       *Inspector
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.i.validateTableForeignKeys(tt.args.databaseName, tt.args.tableName, tt.args.allowChildForeignKeys); (err != nil) != tt.wantErr {
				t.Errorf("Inspector.validateTableForeignKeys() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInspector_validateTableTriggers(t *testing.T) {
	type args struct {
		databaseName string
		tableName    string
	}
	tests := []struct {
		name    string
		i       *Inspector
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.i.validateTableTriggers(tt.args.databaseName, tt.args.tableName); (err != nil) != tt.wantErr {
				t.Errorf("Inspector.validateTableTriggers() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInspector_applyColumnTypes(t *testing.T) {
	type args struct {
		databaseName string
		tableName    string
		columnsLists []*umconf.ColumnList
	}
	tests := []struct {
		name    string
		i       *Inspector
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.i.applyColumnTypes(tt.args.databaseName, tt.args.tableName, tt.args.columnsLists...); (err != nil) != tt.wantErr {
				t.Errorf("Inspector.applyColumnTypes() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInspector_getCandidateUniqueKeys(t *testing.T) {
	type args struct {
		databaseName string
		tableName    string
	}
	tests := []struct {
		name           string
		i              *Inspector
		args           args
		wantUniqueKeys [](*umconf.UniqueKey)
		wantErr        bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotUniqueKeys, err := tt.i.getCandidateUniqueKeys(tt.args.databaseName, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("Inspector.getCandidateUniqueKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotUniqueKeys, tt.wantUniqueKeys) {
				t.Errorf("Inspector.getCandidateUniqueKeys() = %v, want %v", gotUniqueKeys, tt.wantUniqueKeys)
			}
		})
	}
}

func Test_getSharedUniqueKeys(t *testing.T) {
	type args struct {
		originalUniqueKeys [](*umconf.UniqueKey)
		ghostUniqueKeys    [](*umconf.UniqueKey)
	}
	tests := []struct {
		name           string
		args           args
		wantUniqueKeys [](*umconf.UniqueKey)
		wantErr        bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotUniqueKeys, err := getSharedUniqueKeys(tt.args.originalUniqueKeys, tt.args.ghostUniqueKeys)
			if (err != nil) != tt.wantErr {
				t.Errorf("getSharedUniqueKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotUniqueKeys, tt.wantUniqueKeys) {
				t.Errorf("getSharedUniqueKeys() = %v, want %v", gotUniqueKeys, tt.wantUniqueKeys)
			}
		})
	}
}

func TestInspector_showCreateTable(t *testing.T) {
	type args struct {
		databaseName string
		tableName    string
	}
	tests := []struct {
		name                     string
		i                        *Inspector
		args                     args
		wantCreateTableStatement string
		wantErr                  bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCreateTableStatement, err := tt.i.showCreateTable(tt.args.databaseName, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("Inspector.showCreateTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotCreateTableStatement != tt.wantCreateTableStatement {
				t.Errorf("Inspector.showCreateTable() = %v, want %v", gotCreateTableStatement, tt.wantCreateTableStatement)
			}
		})
	}
}
