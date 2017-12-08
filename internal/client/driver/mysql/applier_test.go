package mysql

import (
	gosql "database/sql"
	"reflect"
	"sync"
	"testing"
	"udup/internal/client/driver/mysql/binlog"
	"udup/internal/client/driver/mysql/sql"
	"udup/internal/config"
	log "udup/internal/logger"
	"udup/internal/models"

	gonats "github.com/nats-io/go-nats"
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

func TestApplier_onApplyTxStructWithSetGtid(t *testing.T) {
	type fields struct {
		logger                     *log.Entry
		subject                    string
		tp                         string
		mysqlContext               *config.MySQLDriverConfig
		dbs                        []*sql.DB
		singletonDB                *gosql.DB
		parser                     *sql.Parser
		totalRowCount              int
		applyRowCount              int
		rowCopyComplete            chan bool
		rowCopyCompleteFlag        int64
		copyRowsQueue              chan *dumpEntry
		applyDataEntryQueue        chan *binlog.BinlogEntry
		applyBinlogTxQueue         chan *binlog.BinlogTx
		applyBinlogGroupTxQueue    chan []*binlog.BinlogTx
		lastAppliedBinlogTx        *binlog.BinlogTx
		natsConn                   *gonats.Conn
		waitCh                     chan *models.WaitResult
		wg                         sync.WaitGroup
		shutdown                   bool
		shutdownCh                 chan struct{}
		shutdownLock               sync.Mutex
	}
	type args struct {
		dbApplier *sql.DB
		binlogTx  *binlog.BinlogTx
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Applier{
				logger:                     tt.fields.logger,
				subject:                    tt.fields.subject,
				tp:                         tt.fields.tp,
				mysqlContext:               tt.fields.mysqlContext,
				dbs:                        tt.fields.dbs,
				singletonDB:                tt.fields.singletonDB,
				parser:                     tt.fields.parser,
				totalRowCount:              tt.fields.totalRowCount,
				applyRowCount:              tt.fields.applyRowCount,
				rowCopyComplete:            tt.fields.rowCopyComplete,
				rowCopyCompleteFlag:        tt.fields.rowCopyCompleteFlag,
				copyRowsQueue:              tt.fields.copyRowsQueue,
				applyDataEntryQueue:        tt.fields.applyDataEntryQueue,
				applyBinlogTxQueue:         tt.fields.applyBinlogTxQueue,
				applyBinlogGroupTxQueue:    tt.fields.applyBinlogGroupTxQueue,
				lastAppliedBinlogTx:        tt.fields.lastAppliedBinlogTx,
				natsConn:                   tt.fields.natsConn,
				waitCh:                     tt.fields.waitCh,
				wg:                         tt.fields.wg,
				shutdown:                   tt.fields.shutdown,
				shutdownCh:                 tt.fields.shutdownCh,
				shutdownLock:               tt.fields.shutdownLock,
			}
			if err := a.onApplyTxStructWithSetGtid(tt.args.dbApplier, tt.args.binlogTx); (err != nil) != tt.wantErr {
				t.Errorf("Applier.onApplyTxStructWithSetGtid() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_validateGrants(t *testing.T) {
	type fields struct {
		logger                     *log.Entry
		subject                    string
		tp                         string
		mysqlContext               *config.MySQLDriverConfig
		dbs                        []*sql.DB
		singletonDB                *gosql.DB
		parser                     *sql.Parser
		totalRowCount              int
		applyRowCount              int
		rowCopyComplete            chan bool
		rowCopyCompleteFlag        int64
		copyRowsQueue              chan *dumpEntry
		applyDataEntryQueue        chan *binlog.BinlogEntry
		applyBinlogTxQueue         chan *binlog.BinlogTx
		applyBinlogGroupTxQueue    chan []*binlog.BinlogTx
		lastAppliedBinlogTx        *binlog.BinlogTx
		natsConn                   *gonats.Conn
		waitCh                     chan *models.WaitResult
		wg                         sync.WaitGroup
		shutdown                   bool
		shutdownCh                 chan struct{}
		shutdownLock               sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Applier{
				logger:                     tt.fields.logger,
				subject:                    tt.fields.subject,
				tp:                         tt.fields.tp,
				mysqlContext:               tt.fields.mysqlContext,
				dbs:                        tt.fields.dbs,
				singletonDB:                tt.fields.singletonDB,
				parser:                     tt.fields.parser,
				totalRowCount:              tt.fields.totalRowCount,
				applyRowCount:              tt.fields.applyRowCount,
				rowCopyComplete:            tt.fields.rowCopyComplete,
				rowCopyCompleteFlag:        tt.fields.rowCopyCompleteFlag,
				copyRowsQueue:              tt.fields.copyRowsQueue,
				applyDataEntryQueue:        tt.fields.applyDataEntryQueue,
				applyBinlogTxQueue:         tt.fields.applyBinlogTxQueue,
				applyBinlogGroupTxQueue:    tt.fields.applyBinlogGroupTxQueue,
				lastAppliedBinlogTx:        tt.fields.lastAppliedBinlogTx,
				natsConn:                   tt.fields.natsConn,
				waitCh:                     tt.fields.waitCh,
				wg:                         tt.fields.wg,
				shutdown:                   tt.fields.shutdown,
				shutdownCh:                 tt.fields.shutdownCh,
				shutdownLock:               tt.fields.shutdownLock,
			}
			if err := a.validateGrants(); (err != nil) != tt.wantErr {
				t.Errorf("Applier.validateGrants() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_createTableGtidExecuted(t *testing.T) {
	type fields struct {
		logger                     *log.Entry
		subject                    string
		tp                         string
		mysqlContext               *config.MySQLDriverConfig
		dbs                        []*sql.DB
		singletonDB                *gosql.DB
		parser                     *sql.Parser
		totalRowCount              int
		applyRowCount              int
		rowCopyComplete            chan bool
		rowCopyCompleteFlag        int64
		copyRowsQueue              chan *dumpEntry
		applyDataEntryQueue        chan *binlog.BinlogEntry
		applyBinlogTxQueue         chan *binlog.BinlogTx
		applyBinlogGroupTxQueue    chan []*binlog.BinlogTx
		lastAppliedBinlogTx        *binlog.BinlogTx
		natsConn                   *gonats.Conn
		waitCh                     chan *models.WaitResult
		wg                         sync.WaitGroup
		shutdown                   bool
		shutdownCh                 chan struct{}
		shutdownLock               sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Applier{
				logger:                     tt.fields.logger,
				subject:                    tt.fields.subject,
				tp:                         tt.fields.tp,
				mysqlContext:               tt.fields.mysqlContext,
				dbs:                        tt.fields.dbs,
				singletonDB:                tt.fields.singletonDB,
				parser:                     tt.fields.parser,
				totalRowCount:              tt.fields.totalRowCount,
				applyRowCount:              tt.fields.applyRowCount,
				rowCopyComplete:            tt.fields.rowCopyComplete,
				rowCopyCompleteFlag:        tt.fields.rowCopyCompleteFlag,
				copyRowsQueue:              tt.fields.copyRowsQueue,
				applyDataEntryQueue:        tt.fields.applyDataEntryQueue,
				applyBinlogTxQueue:         tt.fields.applyBinlogTxQueue,
				applyBinlogGroupTxQueue:    tt.fields.applyBinlogGroupTxQueue,
				lastAppliedBinlogTx:        tt.fields.lastAppliedBinlogTx,
				natsConn:                   tt.fields.natsConn,
				waitCh:                     tt.fields.waitCh,
				wg:                         tt.fields.wg,
				shutdown:                   tt.fields.shutdown,
				shutdownCh:                 tt.fields.shutdownCh,
				shutdownLock:               tt.fields.shutdownLock,
			}
			if err := a.createTableGtidExecuted(); (err != nil) != tt.wantErr {
				t.Errorf("Applier.createTableGtidExecuted() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_onDone(t *testing.T) {
	type fields struct {
		logger                     *log.Entry
		subject                    string
		tp                         string
		mysqlContext               *config.MySQLDriverConfig
		dbs                        []*sql.DB
		singletonDB                *gosql.DB
		parser                     *sql.Parser
		totalRowCount              int
		applyRowCount              int
		rowCopyComplete            chan bool
		rowCopyCompleteFlag        int64
		copyRowsQueue              chan *dumpEntry
		applyDataEntryQueue        chan *binlog.BinlogEntry
		applyBinlogTxQueue         chan *binlog.BinlogTx
		applyBinlogGroupTxQueue    chan []*binlog.BinlogTx
		lastAppliedBinlogTx        *binlog.BinlogTx
		natsConn                   *gonats.Conn
		waitCh                     chan *models.WaitResult
		wg                         sync.WaitGroup
		shutdown                   bool
		shutdownCh                 chan struct{}
		shutdownLock               sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Applier{
				logger:                     tt.fields.logger,
				subject:                    tt.fields.subject,
				tp:                         tt.fields.tp,
				mysqlContext:               tt.fields.mysqlContext,
				dbs:                        tt.fields.dbs,
				singletonDB:                tt.fields.singletonDB,
				parser:                     tt.fields.parser,
				totalRowCount:              tt.fields.totalRowCount,
				applyRowCount:              tt.fields.applyRowCount,
				rowCopyComplete:            tt.fields.rowCopyComplete,
				rowCopyCompleteFlag:        tt.fields.rowCopyCompleteFlag,
				copyRowsQueue:              tt.fields.copyRowsQueue,
				applyDataEntryQueue:        tt.fields.applyDataEntryQueue,
				applyBinlogTxQueue:         tt.fields.applyBinlogTxQueue,
				applyBinlogGroupTxQueue:    tt.fields.applyBinlogGroupTxQueue,
				lastAppliedBinlogTx:        tt.fields.lastAppliedBinlogTx,
				natsConn:                   tt.fields.natsConn,
				waitCh:                     tt.fields.waitCh,
				wg:                         tt.fields.wg,
				shutdown:                   tt.fields.shutdown,
				shutdownCh:                 tt.fields.shutdownCh,
				shutdownLock:               tt.fields.shutdownLock,
			}
			a.onDone()
		})
	}
}

func TestApplier_onApplyTxStructWithSuper(t *testing.T) {
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
			if err := tt.a.onApplyTxStructWithSuper(tt.args.dbApplier, tt.args.binlogTx); (err != nil) != tt.wantErr {
				t.Errorf("Applier.onApplyTxStructWithSuper() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplier_Test008ProvidesFlowControlToThrottleOverSending(t *testing.T) {
	tests := []struct {
		name string
		a    *Applier
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.a.Test008ProvidesFlowControlToThrottleOverSending()
		})
	}
}
