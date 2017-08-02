package mysql

import (
	gosql "database/sql"
	"reflect"
	"testing"
	"udup/internal/client/driver/mysql/binlog"
	"udup/internal/client/driver/mysql/sql"
	"udup/internal/config"
	umconf "udup/internal/config/mysql"
	log "udup/internal/logger"
	"udup/internal/models"
)

func TestNewApplier(t *testing.T) {
	type args struct {
		subject string
		tp      string
		cfg     *config.MySQLDriverConfig
		logger  *log.Logger
	}
	tests := []struct {
		name string
		args args
		want *Applier
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewApplier(tt.args.subject, tt.args.tp, tt.args.cfg, tt.args.logger); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewApplier() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplier_sleepWhileTrue(t *testing.T) {
	type args struct {
		operation func() (bool, error)
	}
	tests := []struct {
		name    string
		a       *Applier
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.sleepWhileTrue(tt.args.operation); (err != nil) != tt.wantErr {
				t.Errorf("Applier.sleepWhileTrue() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_retryOperation(t *testing.T) {
	type args struct {
		operation    func() error
		notFatalHint []bool
	}
	tests := []struct {
		name    string
		a       *Applier
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.retryOperation(tt.args.operation, tt.args.notFatalHint...); (err != nil) != tt.wantErr {
				t.Errorf("Applier.retryOperation() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_consumeRowCopyComplete(t *testing.T) {
	tests := []struct {
		name string
		a    *Applier
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.a.consumeRowCopyComplete()
		})
	}
}

func TestApplier_validateStatement(t *testing.T) {
	type args struct {
		doTb *config.Table
	}
	tests := []struct {
		name    string
		a       *Applier
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.validateStatement(tt.args.doTb); (err != nil) != tt.wantErr {
				t.Errorf("Applier.validateStatement() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_Run(t *testing.T) {
	tests := []struct {
		name string
		a    *Applier
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.a.Run()
		})
	}
}

func TestApplier_readCurrentBinlogCoordinates(t *testing.T) {
	tests := []struct {
		name    string
		a       *Applier
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.readCurrentBinlogCoordinates(); (err != nil) != tt.wantErr {
				t.Errorf("Applier.readCurrentBinlogCoordinates() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_cutOver(t *testing.T) {
	tests := []struct {
		name    string
		a       *Applier
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.cutOver(); (err != nil) != tt.wantErr {
				t.Errorf("Applier.cutOver() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_waitForEventsUpToLock(t *testing.T) {
	tests := []struct {
		name    string
		a       *Applier
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.waitForEventsUpToLock(); (err != nil) != tt.wantErr {
				t.Errorf("Applier.waitForEventsUpToLock() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_cutOverTwoStep(t *testing.T) {
	tests := []struct {
		name    string
		a       *Applier
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.cutOverTwoStep(); (err != nil) != tt.wantErr {
				t.Errorf("Applier.cutOverTwoStep() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_atomicCutOver(t *testing.T) {
	tests := []struct {
		name    string
		a       *Applier
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.atomicCutOver(); (err != nil) != tt.wantErr {
				t.Errorf("Applier.atomicCutOver() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_printMigrationStatusHint(t *testing.T) {
	type args struct {
		databaseName string
		tableName    string
	}
	tests := []struct {
		name string
		a    *Applier
		args args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.a.printMigrationStatusHint(tt.args.databaseName, tt.args.tableName)
		})
	}
}

func TestApplier_onApplyTxStruct(t *testing.T) {
	type args struct {
		dbApplier *sql.DB
		binlogTx  *binlog.BinlogTx
	}
	tests := []struct {
		name    string
		a       *Applier
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.onApplyTxStruct(tt.args.dbApplier, tt.args.binlogTx); (err != nil) != tt.wantErr {
				t.Errorf("Applier.onApplyTxStruct() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_executeWriteFuncs(t *testing.T) {
	tests := []struct {
		name string
		a    *Applier
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.a.executeWriteFuncs()
		})
	}
}

func TestApplier_initNatSubClient(t *testing.T) {
	tests := []struct {
		name    string
		a       *Applier
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.initNatSubClient(); (err != nil) != tt.wantErr {
				t.Errorf("Applier.initNatSubClient() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDecode(t *testing.T) {
	type args struct {
		data []byte
		vPtr interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Decode(tt.args.data, tt.args.vPtr); (err != nil) != tt.wantErr {
				t.Errorf("Decode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_initiateStreaming(t *testing.T) {
	tests := []struct {
		name    string
		a       *Applier
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.initiateStreaming(); (err != nil) != tt.wantErr {
				t.Errorf("Applier.initiateStreaming() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_initDBConnections(t *testing.T) {
	tests := []struct {
		name    string
		a       *Applier
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.initDBConnections(); (err != nil) != tt.wantErr {
				t.Errorf("Applier.initDBConnections() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_validateServerUUID(t *testing.T) {
	tests := []struct {
		name string
		a    *Applier
		want string
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.validateServerUUID(); got != tt.want {
				t.Errorf("Applier.validateServerUUID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplier_validateConnection(t *testing.T) {
	type args struct {
		db *gosql.DB
	}
	tests := []struct {
		name    string
		a       *Applier
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.validateConnection(tt.args.db); (err != nil) != tt.wantErr {
				t.Errorf("Applier.validateConnection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_validateTableForeignKeys(t *testing.T) {
	tests := []struct {
		name    string
		a       *Applier
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.validateTableForeignKeys(); (err != nil) != tt.wantErr {
				t.Errorf("Applier.validateTableForeignKeys() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_validateAndReadTimeZone(t *testing.T) {
	tests := []struct {
		name    string
		a       *Applier
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.validateAndReadTimeZone(); (err != nil) != tt.wantErr {
				t.Errorf("Applier.validateAndReadTimeZone() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_readTableColumns(t *testing.T) {
	tests := []struct {
		name    string
		a       *Applier
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.readTableColumns(); (err != nil) != tt.wantErr {
				t.Errorf("Applier.readTableColumns() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_LockOriginalTable(t *testing.T) {
	tests := []struct {
		name    string
		a       *Applier
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.LockOriginalTable(); (err != nil) != tt.wantErr {
				t.Errorf("Applier.LockOriginalTable() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_UnlockTables(t *testing.T) {
	tests := []struct {
		name    string
		a       *Applier
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.UnlockTables(); (err != nil) != tt.wantErr {
				t.Errorf("Applier.UnlockTables() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_GetSessionLockName(t *testing.T) {
	type args struct {
		sessionId int64
	}
	tests := []struct {
		name string
		a    *Applier
		args args
		want string
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.GetSessionLockName(tt.args.sessionId); got != tt.want {
				t.Errorf("Applier.GetSessionLockName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplier_ExpectUsedLock(t *testing.T) {
	type args struct {
		sessionId int64
	}
	tests := []struct {
		name    string
		a       *Applier
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.ExpectUsedLock(tt.args.sessionId); (err != nil) != tt.wantErr {
				t.Errorf("Applier.ExpectUsedLock() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_ExpectProcess(t *testing.T) {
	type args struct {
		sessionId int64
		stateHint string
		infoHint  string
	}
	tests := []struct {
		name    string
		a       *Applier
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.ExpectProcess(tt.args.sessionId, tt.args.stateHint, tt.args.infoHint); (err != nil) != tt.wantErr {
				t.Errorf("Applier.ExpectProcess() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_ShowStatusVariable(t *testing.T) {
	type args struct {
		variableName string
	}
	tests := []struct {
		name       string
		a          *Applier
		args       args
		wantResult int64
		wantErr    bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotResult, err := tt.a.ShowStatusVariable(tt.args.variableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("Applier.ShowStatusVariable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotResult != tt.wantResult {
				t.Errorf("Applier.ShowStatusVariable() = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}

func TestApplier_getCandidateUniqueKeys(t *testing.T) {
	type args struct {
		databaseName string
		tableName    string
	}
	tests := []struct {
		name           string
		a              *Applier
		args           args
		wantUniqueKeys [](*umconf.UniqueKey)
		wantErr        bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotUniqueKeys, err := tt.a.getCandidateUniqueKeys(tt.args.databaseName, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("Applier.getCandidateUniqueKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotUniqueKeys, tt.wantUniqueKeys) {
				t.Errorf("Applier.getCandidateUniqueKeys() = %v, want %v", gotUniqueKeys, tt.wantUniqueKeys)
			}
		})
	}
}

func TestApplier_InspectTableColumnsAndUniqueKeys(t *testing.T) {
	type args struct {
		databaseName string
		tableName    string
	}
	tests := []struct {
		name           string
		a              *Applier
		args           args
		wantColumns    *umconf.ColumnList
		wantUniqueKeys [](*umconf.UniqueKey)
		wantErr        bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotColumns, gotUniqueKeys, err := tt.a.InspectTableColumnsAndUniqueKeys(tt.args.databaseName, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("Applier.InspectTableColumnsAndUniqueKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotColumns, tt.wantColumns) {
				t.Errorf("Applier.InspectTableColumnsAndUniqueKeys() gotColumns = %v, want %v", gotColumns, tt.wantColumns)
			}
			if !reflect.DeepEqual(gotUniqueKeys, tt.wantUniqueKeys) {
				t.Errorf("Applier.InspectTableColumnsAndUniqueKeys() gotUniqueKeys = %v, want %v", gotUniqueKeys, tt.wantUniqueKeys)
			}
		})
	}
}

func TestApplier_getSharedColumns(t *testing.T) {
	type args struct {
		originalColumns *umconf.ColumnList
		ghostColumns    *umconf.ColumnList
		columnRenameMap map[string]string
	}
	tests := []struct {
		name  string
		a     *Applier
		args  args
		want  *umconf.ColumnList
		want1 *umconf.ColumnList
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := tt.a.getSharedColumns(tt.args.originalColumns, tt.args.ghostColumns, tt.args.columnRenameMap)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Applier.getSharedColumns() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("Applier.getSharedColumns() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestApplier_buildDMLEventQuery(t *testing.T) {
	type args struct {
		dmlEvent binlog.DataEvent
	}
	tests := []struct {
		name          string
		a             *Applier
		args          args
		wantQuery     string
		wantArgs      []interface{}
		wantRowsDelta int64
		wantErr       bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotQuery, gotArgs, gotRowsDelta, err := tt.a.buildDMLEventQuery(tt.args.dmlEvent)
			if (err != nil) != tt.wantErr {
				t.Errorf("Applier.buildDMLEventQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotQuery != tt.wantQuery {
				t.Errorf("Applier.buildDMLEventQuery() gotQuery = %v, want %v", gotQuery, tt.wantQuery)
			}
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("Applier.buildDMLEventQuery() gotArgs = %v, want %v", gotArgs, tt.wantArgs)
			}
			if gotRowsDelta != tt.wantRowsDelta {
				t.Errorf("Applier.buildDMLEventQuery() gotRowsDelta = %v, want %v", gotRowsDelta, tt.wantRowsDelta)
			}
		})
	}
}

func TestApplier_ApplyBinlogEvent(t *testing.T) {
	type args struct {
		db     *gosql.DB
		events [](binlog.DataEvent)
	}
	tests := []struct {
		name    string
		a       *Applier
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.ApplyBinlogEvent(tt.args.db, tt.args.events); (err != nil) != tt.wantErr {
				t.Errorf("Applier.ApplyBinlogEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_ApplyEventQueries(t *testing.T) {
	type args struct {
		db    *gosql.DB
		entry *dumpEntry
	}
	tests := []struct {
		name    string
		a       *Applier
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.ApplyEventQueries(tt.args.db, tt.args.entry); (err != nil) != tt.wantErr {
				t.Errorf("Applier.ApplyEventQueries() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_Stats(t *testing.T) {
	tests := []struct {
		name    string
		a       *Applier
		want    *models.TaskStatistics
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.a.Stats()
			if (err != nil) != tt.wantErr {
				t.Errorf("Applier.Stats() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Applier.Stats() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplier_ID(t *testing.T) {
	tests := []struct {
		name string
		a    *Applier
		want string
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.ID(); got != tt.want {
				t.Errorf("Applier.ID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplier_onError(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		a    *Applier
		args args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.a.onError(tt.args.err)
		})
	}
}

func TestApplier_WaitCh(t *testing.T) {
	tests := []struct {
		name string
		a    *Applier
		want chan *models.WaitResult
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.WaitCh(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Applier.WaitCh() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplier_Shutdown(t *testing.T) {
	tests := []struct {
		name    string
		a       *Applier
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.a.Shutdown(); (err != nil) != tt.wantErr {
				t.Errorf("Applier.Shutdown() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
