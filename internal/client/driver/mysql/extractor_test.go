package mysql

import (
	"reflect"
	"testing"
	"udup/internal/client/driver/mysql/base"
	"udup/internal/config"
	log "udup/internal/logger"
	"udup/internal/models"
)

func TestGtidSetDiff(t *testing.T) {
	// TODO
	g, err := GtidSetDiff(
		"113fa2ce-c8e6-11e7-b894-67ad30e6f107:1-100:200:300-400,f2a4aa16-c8e6-11e7-9ff0-e19f7778f563:100-200:300-400,8888aa16-c8e6-11e7-9ff0-e19f7778f563:1-1000",
		"113fa2ce-c8e6-11e7-b894-67ad30e6f107:330,f2a4aa16-c8e6-11e7-9ff0-e19f7778f563:301",
	)
}

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
		binlogCoordinates *base.BinlogCoordinateTx
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
		want *base.BinlogCoordinateTx
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
		want *base.BinlogCoordinateTx
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
