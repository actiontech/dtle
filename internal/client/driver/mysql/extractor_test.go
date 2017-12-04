package mysql

import (
	"reflect"
	"testing"
	"udup/internal/client/driver/mysql/base"
	"udup/internal/config"
	log "udup/internal/logger"
	"udup/internal/models"
)

func TestNewExtractor(t *testing.T) {
	type args struct {
		subject    string
		tp         string
		maxPayload int
		cfg        *config.MySQLDriverConfig
		logger     *log.Logger
	}
	tests := []struct {
		name string
		args args
		want *Extractor
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewExtractor(tt.args.subject, tt.args.tp, tt.args.maxPayload, tt.args.cfg, tt.args.logger); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewExtractor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractor_sleepWhileTrue(t *testing.T) {
	type args struct {
		operation func() (bool, error)
	}
	tests := []struct {
		name    string
		e       *Extractor
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.sleepWhileTrue(tt.args.operation); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.sleepWhileTrue() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_retryOperation(t *testing.T) {
	type args struct {
		operation    func() error
		notFatalHint []bool
	}
	tests := []struct {
		name    string
		e       *Extractor
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.retryOperation(tt.args.operation, tt.args.notFatalHint...); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.retryOperation() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_consumeRowCopyComplete(t *testing.T) {
	tests := []struct {
		name string
		e    *Extractor
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.e.consumeRowCopyComplete()
		})
	}
}

func TestExtractor_canStopStreaming(t *testing.T) {
	tests := []struct {
		name string
		e    *Extractor
		want bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.canStopStreaming(); got != tt.want {
				t.Errorf("Extractor.canStopStreaming() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractor_Run(t *testing.T) {
	tests := []struct {
		name string
		e    *Extractor
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.e.Run()
		})
	}
}

func TestExtractor_cutOver(t *testing.T) {
	tests := []struct {
		name    string
		e       *Extractor
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.cutOver(); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.cutOver() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_waitForEventsUpToLock(t *testing.T) {
	tests := []struct {
		name    string
		e       *Extractor
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.waitForEventsUpToLock(); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.waitForEventsUpToLock() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_cutOverTwoStep(t *testing.T) {
	tests := []struct {
		name    string
		e       *Extractor
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.cutOverTwoStep(); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.cutOverTwoStep() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_atomicCutOver(t *testing.T) {
	type args struct {
		tableName string
	}
	tests := []struct {
		name    string
		e       *Extractor
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.atomicCutOver(tt.args.tableName); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.atomicCutOver() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_initiateInspector(t *testing.T) {
	tests := []struct {
		name    string
		e       *Extractor
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.initiateInspector(); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.initiateInspector() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_printMigrationStatusHint(t *testing.T) {
	type args struct {
		databaseName string
		tableName    string
	}
	tests := []struct {
		name string
		e    *Extractor
		args args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.e.printMigrationStatusHint(tt.args.databaseName, tt.args.tableName)
		})
	}
}

func TestExtractor_initNatsPubClient(t *testing.T) {
	tests := []struct {
		name    string
		e       *Extractor
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.initNatsPubClient(); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.initNatsPubClient() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_initiateStreaming(t *testing.T) {
	tests := []struct {
		name    string
		e       *Extractor
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.initiateStreaming(); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.initiateStreaming() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_initDBConnections(t *testing.T) {
	tests := []struct {
		name    string
		e       *Extractor
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.initDBConnections(); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.initDBConnections() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_initBinlogReader(t *testing.T) {
	type args struct {
		binlogCoordinates *base.BinlogCoordinates
	}
	tests := []struct {
		name    string
		e       *Extractor
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.initBinlogReader(tt.args.binlogCoordinates); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.initBinlogReader() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_validateConnection(t *testing.T) {
	tests := []struct {
		name    string
		e       *Extractor
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.validateConnection(); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.validateConnection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_SelectSqlMode(t *testing.T) {
	tests := []struct {
		name    string
		e       *Extractor
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.selectSqlMode(); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.SelectSqlMode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_GetCurrentBinlogCoordinates(t *testing.T) {
	tests := []struct {
		name string
		e    *Extractor
		want *base.BinlogCoordinates
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.GetCurrentBinlogCoordinates(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Extractor.GetCurrentBinlogCoordinates() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractor_GetReconnectBinlogCoordinates(t *testing.T) {
	tests := []struct {
		name string
		e    *Extractor
		want *base.BinlogCoordinates
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.GetReconnectBinlogCoordinates(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Extractor.GetReconnectBinlogCoordinates() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractor_readCurrentBinlogCoordinates(t *testing.T) {
	tests := []struct {
		name    string
		e       *Extractor
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.readCurrentBinlogCoordinates(); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.readCurrentBinlogCoordinates() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_readMySqlCharsetSystemVariables(t *testing.T) {
	tests := []struct {
		name    string
		e       *Extractor
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.readMySqlCharsetSystemVariables(); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.readMySqlCharsetSystemVariables() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_setStatementFor(t *testing.T) {
	tests := []struct {
		name string
		e    *Extractor
		want string
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.setStatementFor(); got != tt.want {
				t.Errorf("Extractor.setStatementFor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractor_validateServerUUID(t *testing.T) {
	tests := []struct {
		name string
		e    *Extractor
		want string
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.validateServerUUID(); got != tt.want {
				t.Errorf("Extractor.validateServerUUID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEncode(t *testing.T) {
	type args struct {
		v interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Encode(tt.args.v)
			if (err != nil) != tt.wantErr {
				t.Errorf("Encode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Encode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractor_StreamEvents(t *testing.T) {
	type args struct {
	}
	tests := []struct {
		name    string
		e       *Extractor
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.StreamEvents(); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.StreamEvents() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_requestMsg(t *testing.T) {
	type args struct {
		subject string
		gtid    string
		txMsg   []byte
	}
	tests := []struct {
		name    string
		e       *Extractor
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.requestMsg(tt.args.subject, tt.args.gtid, tt.args.txMsg); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.requestMsg() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_mysqlDump(t *testing.T) {
	tests := []struct {
		name    string
		e       *Extractor
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.mysqlDump(); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.mysqlDump() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_encodeDumpEntry(t *testing.T) {
	type args struct {
		entry *dumpEntry
	}
	tests := []struct {
		name    string
		e       *Extractor
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.encodeDumpEntry(tt.args.entry); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.encodeDumpEntry() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_currentTimeMillis(t *testing.T) {
	tests := []struct {
		name string
		want int64
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := currentTimeMillis(); got != tt.want {
				t.Errorf("currentTimeMillis() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractor_Stats(t *testing.T) {
	tests := []struct {
		name    string
		e       *Extractor
		want    *models.TaskStatistics
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.e.Stats()
			if (err != nil) != tt.wantErr {
				t.Errorf("Extractor.Stats() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Extractor.Stats() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractor_ID(t *testing.T) {
	tests := []struct {
		name string
		e    *Extractor
		want string
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.ID(); got != tt.want {
				t.Errorf("Extractor.ID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractor_onError(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		e    *Extractor
		args args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.e.onError(tt.args.err)
		})
	}
}

func TestExtractor_WaitCh(t *testing.T) {
	tests := []struct {
		name string
		e    *Extractor
		want chan *models.WaitResult
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.WaitCh(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Extractor.WaitCh() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractor_Shutdown(t *testing.T) {
	tests := []struct {
		name    string
		e       *Extractor
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.Shutdown(); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.Shutdown() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_inspectTables(t *testing.T) {
	tests := []struct {
		name    string
		e       *Extractor
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.inspectTables(); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.inspectTables() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_readTableColumns(t *testing.T) {
	tests := []struct {
		name    string
		e       *Extractor
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.readTableColumns(); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.readTableColumns() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_selectSqlMode(t *testing.T) {
	tests := []struct {
		name    string
		e       *Extractor
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.selectSqlMode(); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.selectSqlMode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_validateAndReadTimeZone(t *testing.T) {
	tests := []struct {
		name    string
		e       *Extractor
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.validateAndReadTimeZone(); (err != nil) != tt.wantErr {
				t.Errorf("Extractor.validateAndReadTimeZone() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractor_CountTableRows(t *testing.T) {
	type args struct {
		tableSchema string
		tableName   string
	}
	tests := []struct {
		name    string
		e       *Extractor
		args    args
		want    int64
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.e.CountTableRows(tt.args.tableSchema, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("Extractor.CountTableRows() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Extractor.CountTableRows() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractor_onDone(t *testing.T) {
	tests := []struct {
		name string
		e    *Extractor
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.e.onDone()
		})
	}
}
