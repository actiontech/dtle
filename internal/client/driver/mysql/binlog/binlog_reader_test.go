package binlog

import (
	"bytes"
	gosql "database/sql"
	"reflect"
	"regexp"
	"sync"
	"testing"
	"udup/internal/client/driver/mysql/base"
	"udup/internal/config"
	"udup/internal/config/mysql"
	log "udup/internal/logger"

	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

func TestParseMysqlGTIDSet(t *testing.T) {
	type args struct {
		gtidset string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"t1", args{"de278ad0-2106-11e4-9f8e-6edd0ca20947:1:2:3:4:6:5"}, false},
		{"t2", args{"de278ad0-2106-11e4-9f8e-6edd0ca20947:1-5,de278ad0-2106-11e4-9f8e-6edd0ca20947:6-7:10-20"}, false},
		{"t3", args{"de278ad0-2106-11e4-9f8e-6edd0ca20947:6-7:1-4:10-20"}, false},
		{"t4", args{"de278ad0-2106-11e4-9f8e-6edd0ca20947:14972:14977:14983:14984:14989:14992"}, false},
		{"t5", args{"de278ad0-2106-11e4-9f8e-6edd0ca20947:38137-38142,de278ad0-2106-11e4-9f8e-6edd0ca20947:38137-38143"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotGtidset, err := gomysql.ParseMysqlGTIDSet(tt.args.gtidset)
			if err != nil {
				t.Errorf("ParseMysqlGTIDSet error = %v", err)
				return
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseMysqlGTIDSet error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			println(t.Name(), gotGtidset.String())
		})
	}
}

func TestNewMySQLReader(t *testing.T) {
	type args struct {
		cfg    *config.MySQLDriverConfig
		logger *log.Entry
	}
	tests := []struct {
		name             string
		args             args
		wantBinlogReader *BinlogReader
		wantErr          bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBinlogReader, err := NewMySQLReader(tt.args.cfg, tt.args.logger)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewMySQLReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotBinlogReader, tt.wantBinlogReader) {
				t.Errorf("NewMySQLReader() = %v, want %v", gotBinlogReader, tt.wantBinlogReader)
			}
		})
	}
}

func TestBinlogReader_ConnectBinlogStreamer(t *testing.T) {
	type fields struct {
		logger                   *log.Entry
		connectionConfig         *mysql.ConnectionConfig
		db                       *gosql.DB
		binlogSyncer             *replication.BinlogSyncer
		binlogStreamer           *replication.BinlogStreamer
		currentCoordinates       base.BinlogCoordinates
		currentCoordinatesMutex  *sync.Mutex
		LastAppliedRowsEventHint base.BinlogCoordinates
		MysqlContext             *config.MySQLDriverConfig
		currentTx                *BinlogTx
		currentBinlogEntry       *BinlogEntry
		txCount                  int
		currentFde               string
		currentQuery             *bytes.Buffer
		currentSqlB64            *bytes.Buffer
		appendB64SqlBs           []byte
		ReMap                    map[string]*regexp.Regexp
		wg                       sync.WaitGroup
		shutdown                 bool
		shutdownCh               chan struct{}
		shutdownLock             sync.Mutex
	}
	type args struct {
		coordinates base.BinlogCoordinates
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
			b := &BinlogReader{
				logger:                   tt.fields.logger,
				connectionConfig:         tt.fields.connectionConfig,
				db:                       tt.fields.db,
				binlogSyncer:             tt.fields.binlogSyncer,
				binlogStreamer:           tt.fields.binlogStreamer,
				currentCoordinates:       tt.fields.currentCoordinates,
				currentCoordinatesMutex:  tt.fields.currentCoordinatesMutex,
				LastAppliedRowsEventHint: tt.fields.LastAppliedRowsEventHint,
				MysqlContext:             tt.fields.MysqlContext,
				currentTx:                tt.fields.currentTx,
				currentBinlogEntry:       tt.fields.currentBinlogEntry,
				txCount:                  tt.fields.txCount,
				currentFde:               tt.fields.currentFde,
				currentQuery:             tt.fields.currentQuery,
				currentSqlB64:            tt.fields.currentSqlB64,
				appendB64SqlBs:           tt.fields.appendB64SqlBs,
				ReMap:                    tt.fields.ReMap,
				wg:                       tt.fields.wg,
				shutdown:                 tt.fields.shutdown,
				shutdownCh:               tt.fields.shutdownCh,
				shutdownLock:             tt.fields.shutdownLock,
			}
			if err := b.ConnectBinlogStreamer(tt.args.coordinates); (err != nil) != tt.wantErr {
				t.Errorf("BinlogReader.ConnectBinlogStreamer() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBinlogReader_GetCurrentBinlogCoordinates(t *testing.T) {
	type fields struct {
		logger                   *log.Entry
		connectionConfig         *mysql.ConnectionConfig
		db                       *gosql.DB
		binlogSyncer             *replication.BinlogSyncer
		binlogStreamer           *replication.BinlogStreamer
		currentCoordinates       base.BinlogCoordinates
		currentCoordinatesMutex  *sync.Mutex
		LastAppliedRowsEventHint base.BinlogCoordinates
		MysqlContext             *config.MySQLDriverConfig
		currentTx                *BinlogTx
		currentBinlogEntry       *BinlogEntry
		txCount                  int
		currentFde               string
		currentQuery             *bytes.Buffer
		currentSqlB64            *bytes.Buffer
		appendB64SqlBs           []byte
		ReMap                    map[string]*regexp.Regexp
		wg                       sync.WaitGroup
		shutdown                 bool
		shutdownCh               chan struct{}
		shutdownLock             sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   *base.BinlogCoordinates
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogReader{
				logger:                   tt.fields.logger,
				connectionConfig:         tt.fields.connectionConfig,
				db:                       tt.fields.db,
				binlogSyncer:             tt.fields.binlogSyncer,
				binlogStreamer:           tt.fields.binlogStreamer,
				currentCoordinates:       tt.fields.currentCoordinates,
				currentCoordinatesMutex:  tt.fields.currentCoordinatesMutex,
				LastAppliedRowsEventHint: tt.fields.LastAppliedRowsEventHint,
				MysqlContext:             tt.fields.MysqlContext,
				currentTx:                tt.fields.currentTx,
				currentBinlogEntry:       tt.fields.currentBinlogEntry,
				txCount:                  tt.fields.txCount,
				currentFde:               tt.fields.currentFde,
				currentQuery:             tt.fields.currentQuery,
				currentSqlB64:            tt.fields.currentSqlB64,
				appendB64SqlBs:           tt.fields.appendB64SqlBs,
				ReMap:                    tt.fields.ReMap,
				wg:                       tt.fields.wg,
				shutdown:                 tt.fields.shutdown,
				shutdownCh:               tt.fields.shutdownCh,
				shutdownLock:             tt.fields.shutdownLock,
			}
			if got := b.GetCurrentBinlogCoordinates(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BinlogReader.GetCurrentBinlogCoordinates() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogReader_handleRowsEvent(t *testing.T) {
	type fields struct {
		logger                   *log.Entry
		connectionConfig         *mysql.ConnectionConfig
		db                       *gosql.DB
		binlogSyncer             *replication.BinlogSyncer
		binlogStreamer           *replication.BinlogStreamer
		currentCoordinates       base.BinlogCoordinates
		currentCoordinatesMutex  *sync.Mutex
		LastAppliedRowsEventHint base.BinlogCoordinates
		MysqlContext             *config.MySQLDriverConfig
		currentTx                *BinlogTx
		currentBinlogEntry       *BinlogEntry
		txCount                  int
		currentFde               string
		currentQuery             *bytes.Buffer
		currentSqlB64            *bytes.Buffer
		appendB64SqlBs           []byte
		ReMap                    map[string]*regexp.Regexp
		wg                       sync.WaitGroup
		shutdown                 bool
		shutdownCh               chan struct{}
		shutdownLock             sync.Mutex
	}
	type args struct {
		ev             *replication.BinlogEvent
		entriesChannel chan<- *BinlogEntry
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
			b := &BinlogReader{
				logger:                   tt.fields.logger,
				connectionConfig:         tt.fields.connectionConfig,
				db:                       tt.fields.db,
				binlogSyncer:             tt.fields.binlogSyncer,
				binlogStreamer:           tt.fields.binlogStreamer,
				currentCoordinates:       tt.fields.currentCoordinates,
				currentCoordinatesMutex:  tt.fields.currentCoordinatesMutex,
				LastAppliedRowsEventHint: tt.fields.LastAppliedRowsEventHint,
				MysqlContext:             tt.fields.MysqlContext,
				currentTx:                tt.fields.currentTx,
				currentBinlogEntry:       tt.fields.currentBinlogEntry,
				txCount:                  tt.fields.txCount,
				currentFde:               tt.fields.currentFde,
				currentQuery:             tt.fields.currentQuery,
				currentSqlB64:            tt.fields.currentSqlB64,
				appendB64SqlBs:           tt.fields.appendB64SqlBs,
				ReMap:                    tt.fields.ReMap,
				wg:                       tt.fields.wg,
				shutdown:                 tt.fields.shutdown,
				shutdownCh:               tt.fields.shutdownCh,
				shutdownLock:             tt.fields.shutdownLock,
			}
			if err := b.handleRowsEvent(tt.args.ev, tt.args.entriesChannel); (err != nil) != tt.wantErr {
				t.Errorf("BinlogReader.handleRowsEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBinlogReader_DataStreamEvents(t *testing.T) {
	type fields struct {
		logger                   *log.Entry
		connectionConfig         *mysql.ConnectionConfig
		db                       *gosql.DB
		binlogSyncer             *replication.BinlogSyncer
		binlogStreamer           *replication.BinlogStreamer
		currentCoordinates       base.BinlogCoordinates
		currentCoordinatesMutex  *sync.Mutex
		LastAppliedRowsEventHint base.BinlogCoordinates
		MysqlContext             *config.MySQLDriverConfig
		currentTx                *BinlogTx
		currentBinlogEntry       *BinlogEntry
		txCount                  int
		currentFde               string
		currentQuery             *bytes.Buffer
		currentSqlB64            *bytes.Buffer
		appendB64SqlBs           []byte
		ReMap                    map[string]*regexp.Regexp
		wg                       sync.WaitGroup
		shutdown                 bool
		shutdownCh               chan struct{}
		shutdownLock             sync.Mutex
	}
	type args struct {
		entriesChannel chan<- *BinlogEntry
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
			b := &BinlogReader{
				logger:                   tt.fields.logger,
				connectionConfig:         tt.fields.connectionConfig,
				db:                       tt.fields.db,
				binlogSyncer:             tt.fields.binlogSyncer,
				binlogStreamer:           tt.fields.binlogStreamer,
				currentCoordinates:       tt.fields.currentCoordinates,
				currentCoordinatesMutex:  tt.fields.currentCoordinatesMutex,
				LastAppliedRowsEventHint: tt.fields.LastAppliedRowsEventHint,
				MysqlContext:             tt.fields.MysqlContext,
				currentTx:                tt.fields.currentTx,
				currentBinlogEntry:       tt.fields.currentBinlogEntry,
				txCount:                  tt.fields.txCount,
				currentFde:               tt.fields.currentFde,
				currentQuery:             tt.fields.currentQuery,
				currentSqlB64:            tt.fields.currentSqlB64,
				appendB64SqlBs:           tt.fields.appendB64SqlBs,
				ReMap:                    tt.fields.ReMap,
				wg:                       tt.fields.wg,
				shutdown:                 tt.fields.shutdown,
				shutdownCh:               tt.fields.shutdownCh,
				shutdownLock:             tt.fields.shutdownLock,
			}
			if err := b.DataStreamEvents(tt.args.entriesChannel); (err != nil) != tt.wantErr {
				t.Errorf("BinlogReader.DataStreamEvents() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBinlogReader_BinlogStreamEvents(t *testing.T) {
	type fields struct {
		logger                   *log.Entry
		connectionConfig         *mysql.ConnectionConfig
		db                       *gosql.DB
		binlogSyncer             *replication.BinlogSyncer
		binlogStreamer           *replication.BinlogStreamer
		currentCoordinates       base.BinlogCoordinates
		currentCoordinatesMutex  *sync.Mutex
		LastAppliedRowsEventHint base.BinlogCoordinates
		MysqlContext             *config.MySQLDriverConfig
		currentTx                *BinlogTx
		currentBinlogEntry       *BinlogEntry
		txCount                  int
		currentFde               string
		currentQuery             *bytes.Buffer
		currentSqlB64            *bytes.Buffer
		appendB64SqlBs           []byte
		ReMap                    map[string]*regexp.Regexp
		wg                       sync.WaitGroup
		shutdown                 bool
		shutdownCh               chan struct{}
		shutdownLock             sync.Mutex
	}
	type args struct {
		txChannel chan<- *BinlogTx
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
			b := &BinlogReader{
				logger:                   tt.fields.logger,
				connectionConfig:         tt.fields.connectionConfig,
				db:                       tt.fields.db,
				binlogSyncer:             tt.fields.binlogSyncer,
				binlogStreamer:           tt.fields.binlogStreamer,
				currentCoordinates:       tt.fields.currentCoordinates,
				currentCoordinatesMutex:  tt.fields.currentCoordinatesMutex,
				LastAppliedRowsEventHint: tt.fields.LastAppliedRowsEventHint,
				MysqlContext:             tt.fields.MysqlContext,
				currentTx:                tt.fields.currentTx,
				currentBinlogEntry:       tt.fields.currentBinlogEntry,
				txCount:                  tt.fields.txCount,
				currentFde:               tt.fields.currentFde,
				currentQuery:             tt.fields.currentQuery,
				currentSqlB64:            tt.fields.currentSqlB64,
				appendB64SqlBs:           tt.fields.appendB64SqlBs,
				ReMap:                    tt.fields.ReMap,
				wg:                       tt.fields.wg,
				shutdown:                 tt.fields.shutdown,
				shutdownCh:               tt.fields.shutdownCh,
				shutdownLock:             tt.fields.shutdownLock,
			}
			if err := b.BinlogStreamEvents(tt.args.txChannel); (err != nil) != tt.wantErr {
				t.Errorf("BinlogReader.BinlogStreamEvents() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBinlogReader_handleBinlogRowsEvent(t *testing.T) {
	type fields struct {
		logger                   *log.Entry
		connectionConfig         *mysql.ConnectionConfig
		db                       *gosql.DB
		binlogSyncer             *replication.BinlogSyncer
		binlogStreamer           *replication.BinlogStreamer
		currentCoordinates       base.BinlogCoordinates
		currentCoordinatesMutex  *sync.Mutex
		LastAppliedRowsEventHint base.BinlogCoordinates
		MysqlContext             *config.MySQLDriverConfig
		currentTx                *BinlogTx
		currentBinlogEntry       *BinlogEntry
		txCount                  int
		currentFde               string
		currentQuery             *bytes.Buffer
		currentSqlB64            *bytes.Buffer
		appendB64SqlBs           []byte
		ReMap                    map[string]*regexp.Regexp
		wg                       sync.WaitGroup
		shutdown                 bool
		shutdownCh               chan struct{}
		shutdownLock             sync.Mutex
	}
	type args struct {
		ev        *replication.BinlogEvent
		txChannel chan<- *BinlogTx
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
			b := &BinlogReader{
				logger:                   tt.fields.logger,
				connectionConfig:         tt.fields.connectionConfig,
				db:                       tt.fields.db,
				binlogSyncer:             tt.fields.binlogSyncer,
				binlogStreamer:           tt.fields.binlogStreamer,
				currentCoordinates:       tt.fields.currentCoordinates,
				currentCoordinatesMutex:  tt.fields.currentCoordinatesMutex,
				LastAppliedRowsEventHint: tt.fields.LastAppliedRowsEventHint,
				MysqlContext:             tt.fields.MysqlContext,
				currentTx:                tt.fields.currentTx,
				currentBinlogEntry:       tt.fields.currentBinlogEntry,
				txCount:                  tt.fields.txCount,
				currentFde:               tt.fields.currentFde,
				currentQuery:             tt.fields.currentQuery,
				currentSqlB64:            tt.fields.currentSqlB64,
				appendB64SqlBs:           tt.fields.appendB64SqlBs,
				ReMap:                    tt.fields.ReMap,
				wg:                       tt.fields.wg,
				shutdown:                 tt.fields.shutdown,
				shutdownCh:               tt.fields.shutdownCh,
				shutdownLock:             tt.fields.shutdownLock,
			}
			if err := b.handleBinlogRowsEvent(tt.args.ev, tt.args.txChannel); (err != nil) != tt.wantErr {
				t.Errorf("BinlogReader.handleBinlogRowsEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBinlogReader_appendQuery(t *testing.T) {
	type fields struct {
		logger                   *log.Entry
		connectionConfig         *mysql.ConnectionConfig
		db                       *gosql.DB
		binlogSyncer             *replication.BinlogSyncer
		binlogStreamer           *replication.BinlogStreamer
		currentCoordinates       base.BinlogCoordinates
		currentCoordinatesMutex  *sync.Mutex
		LastAppliedRowsEventHint base.BinlogCoordinates
		MysqlContext             *config.MySQLDriverConfig
		currentTx                *BinlogTx
		currentBinlogEntry       *BinlogEntry
		txCount                  int
		currentFde               string
		currentQuery             *bytes.Buffer
		currentSqlB64            *bytes.Buffer
		appendB64SqlBs           []byte
		ReMap                    map[string]*regexp.Regexp
		wg                       sync.WaitGroup
		shutdown                 bool
		shutdownCh               chan struct{}
		shutdownLock             sync.Mutex
	}
	type args struct {
		query string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogReader{
				logger:                   tt.fields.logger,
				connectionConfig:         tt.fields.connectionConfig,
				db:                       tt.fields.db,
				binlogSyncer:             tt.fields.binlogSyncer,
				binlogStreamer:           tt.fields.binlogStreamer,
				currentCoordinates:       tt.fields.currentCoordinates,
				currentCoordinatesMutex:  tt.fields.currentCoordinatesMutex,
				LastAppliedRowsEventHint: tt.fields.LastAppliedRowsEventHint,
				MysqlContext:             tt.fields.MysqlContext,
				currentTx:                tt.fields.currentTx,
				currentBinlogEntry:       tt.fields.currentBinlogEntry,
				txCount:                  tt.fields.txCount,
				currentFde:               tt.fields.currentFde,
				currentQuery:             tt.fields.currentQuery,
				currentSqlB64:            tt.fields.currentSqlB64,
				appendB64SqlBs:           tt.fields.appendB64SqlBs,
				ReMap:                    tt.fields.ReMap,
				wg:                       tt.fields.wg,
				shutdown:                 tt.fields.shutdown,
				shutdownCh:               tt.fields.shutdownCh,
				shutdownLock:             tt.fields.shutdownLock,
			}
			b.appendQuery(tt.args.query)
		})
	}
}

func Test_newTxWithoutGTIDError(t *testing.T) {
	type args struct {
		event *BinlogEvent
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
			if err := newTxWithoutGTIDError(tt.args.event); (err != nil) != tt.wantErr {
				t.Errorf("newTxWithoutGTIDError() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBinlogReader_clearB64Sql(t *testing.T) {
	type fields struct {
		logger                   *log.Entry
		connectionConfig         *mysql.ConnectionConfig
		db                       *gosql.DB
		binlogSyncer             *replication.BinlogSyncer
		binlogStreamer           *replication.BinlogStreamer
		currentCoordinates       base.BinlogCoordinates
		currentCoordinatesMutex  *sync.Mutex
		LastAppliedRowsEventHint base.BinlogCoordinates
		MysqlContext             *config.MySQLDriverConfig
		currentTx                *BinlogTx
		currentBinlogEntry       *BinlogEntry
		txCount                  int
		currentFde               string
		currentQuery             *bytes.Buffer
		currentSqlB64            *bytes.Buffer
		appendB64SqlBs           []byte
		ReMap                    map[string]*regexp.Regexp
		wg                       sync.WaitGroup
		shutdown                 bool
		shutdownCh               chan struct{}
		shutdownLock             sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogReader{
				logger:                   tt.fields.logger,
				connectionConfig:         tt.fields.connectionConfig,
				db:                       tt.fields.db,
				binlogSyncer:             tt.fields.binlogSyncer,
				binlogStreamer:           tt.fields.binlogStreamer,
				currentCoordinates:       tt.fields.currentCoordinates,
				currentCoordinatesMutex:  tt.fields.currentCoordinatesMutex,
				LastAppliedRowsEventHint: tt.fields.LastAppliedRowsEventHint,
				MysqlContext:             tt.fields.MysqlContext,
				currentTx:                tt.fields.currentTx,
				currentBinlogEntry:       tt.fields.currentBinlogEntry,
				txCount:                  tt.fields.txCount,
				currentFde:               tt.fields.currentFde,
				currentQuery:             tt.fields.currentQuery,
				currentSqlB64:            tt.fields.currentSqlB64,
				appendB64SqlBs:           tt.fields.appendB64SqlBs,
				ReMap:                    tt.fields.ReMap,
				wg:                       tt.fields.wg,
				shutdown:                 tt.fields.shutdown,
				shutdownCh:               tt.fields.shutdownCh,
				shutdownLock:             tt.fields.shutdownLock,
			}
			b.clearB64Sql()
		})
	}
}

func TestBinlogReader_appendB64Sql(t *testing.T) {
	type fields struct {
		logger                   *log.Entry
		connectionConfig         *mysql.ConnectionConfig
		db                       *gosql.DB
		binlogSyncer             *replication.BinlogSyncer
		binlogStreamer           *replication.BinlogStreamer
		currentCoordinates       base.BinlogCoordinates
		currentCoordinatesMutex  *sync.Mutex
		LastAppliedRowsEventHint base.BinlogCoordinates
		MysqlContext             *config.MySQLDriverConfig
		currentTx                *BinlogTx
		currentBinlogEntry       *BinlogEntry
		txCount                  int
		currentFde               string
		currentQuery             *bytes.Buffer
		currentSqlB64            *bytes.Buffer
		appendB64SqlBs           []byte
		ReMap                    map[string]*regexp.Regexp
		wg                       sync.WaitGroup
		shutdown                 bool
		shutdownCh               chan struct{}
		shutdownLock             sync.Mutex
	}
	type args struct {
		event *BinlogEvent
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogReader{
				logger:                   tt.fields.logger,
				connectionConfig:         tt.fields.connectionConfig,
				db:                       tt.fields.db,
				binlogSyncer:             tt.fields.binlogSyncer,
				binlogStreamer:           tt.fields.binlogStreamer,
				currentCoordinates:       tt.fields.currentCoordinates,
				currentCoordinatesMutex:  tt.fields.currentCoordinatesMutex,
				LastAppliedRowsEventHint: tt.fields.LastAppliedRowsEventHint,
				MysqlContext:             tt.fields.MysqlContext,
				currentTx:                tt.fields.currentTx,
				currentBinlogEntry:       tt.fields.currentBinlogEntry,
				txCount:                  tt.fields.txCount,
				currentFde:               tt.fields.currentFde,
				currentQuery:             tt.fields.currentQuery,
				currentSqlB64:            tt.fields.currentSqlB64,
				appendB64SqlBs:           tt.fields.appendB64SqlBs,
				ReMap:                    tt.fields.ReMap,
				wg:                       tt.fields.wg,
				shutdown:                 tt.fields.shutdown,
				shutdownCh:               tt.fields.shutdownCh,
				shutdownLock:             tt.fields.shutdownLock,
			}
			b.appendB64Sql(tt.args.event)
		})
	}
}

func TestBinlogReader_onCommit(t *testing.T) {
	type fields struct {
		logger                   *log.Entry
		connectionConfig         *mysql.ConnectionConfig
		db                       *gosql.DB
		binlogSyncer             *replication.BinlogSyncer
		binlogStreamer           *replication.BinlogStreamer
		currentCoordinates       base.BinlogCoordinates
		currentCoordinatesMutex  *sync.Mutex
		LastAppliedRowsEventHint base.BinlogCoordinates
		MysqlContext             *config.MySQLDriverConfig
		currentTx                *BinlogTx
		currentBinlogEntry       *BinlogEntry
		txCount                  int
		currentFde               string
		currentQuery             *bytes.Buffer
		currentSqlB64            *bytes.Buffer
		appendB64SqlBs           []byte
		ReMap                    map[string]*regexp.Regexp
		wg                       sync.WaitGroup
		shutdown                 bool
		shutdownCh               chan struct{}
		shutdownLock             sync.Mutex
	}
	type args struct {
		lastEvent *BinlogEvent
		txChannel chan<- *BinlogTx
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogReader{
				logger:                   tt.fields.logger,
				connectionConfig:         tt.fields.connectionConfig,
				db:                       tt.fields.db,
				binlogSyncer:             tt.fields.binlogSyncer,
				binlogStreamer:           tt.fields.binlogStreamer,
				currentCoordinates:       tt.fields.currentCoordinates,
				currentCoordinatesMutex:  tt.fields.currentCoordinatesMutex,
				LastAppliedRowsEventHint: tt.fields.LastAppliedRowsEventHint,
				MysqlContext:             tt.fields.MysqlContext,
				currentTx:                tt.fields.currentTx,
				currentBinlogEntry:       tt.fields.currentBinlogEntry,
				txCount:                  tt.fields.txCount,
				currentFde:               tt.fields.currentFde,
				currentQuery:             tt.fields.currentQuery,
				currentSqlB64:            tt.fields.currentSqlB64,
				appendB64SqlBs:           tt.fields.appendB64SqlBs,
				ReMap:                    tt.fields.ReMap,
				wg:                       tt.fields.wg,
				shutdown:                 tt.fields.shutdown,
				shutdownCh:               tt.fields.shutdownCh,
				shutdownLock:             tt.fields.shutdownLock,
			}
			b.onCommit(tt.args.lastEvent, tt.args.txChannel)
		})
	}
}

func TestBinlogReader_InspectTableColumnsAndUniqueKeys(t *testing.T) {
	type fields struct {
		logger                   *log.Entry
		connectionConfig         *mysql.ConnectionConfig
		db                       *gosql.DB
		binlogSyncer             *replication.BinlogSyncer
		binlogStreamer           *replication.BinlogStreamer
		currentCoordinates       base.BinlogCoordinates
		currentCoordinatesMutex  *sync.Mutex
		LastAppliedRowsEventHint base.BinlogCoordinates
		MysqlContext             *config.MySQLDriverConfig
		currentTx                *BinlogTx
		currentBinlogEntry       *BinlogEntry
		txCount                  int
		currentFde               string
		currentQuery             *bytes.Buffer
		currentSqlB64            *bytes.Buffer
		appendB64SqlBs           []byte
		ReMap                    map[string]*regexp.Regexp
		wg                       sync.WaitGroup
		shutdown                 bool
		shutdownCh               chan struct{}
		shutdownLock             sync.Mutex
	}
	type args struct {
		databaseName string
		tableName    string
	}
	tests := []struct {
		name           string
		fields         fields
		args           args
		wantColumns    *mysql.ColumnList
		wantUniqueKeys [](*mysql.UniqueKey)
		wantErr        bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogReader{
				logger:                   tt.fields.logger,
				connectionConfig:         tt.fields.connectionConfig,
				db:                       tt.fields.db,
				binlogSyncer:             tt.fields.binlogSyncer,
				binlogStreamer:           tt.fields.binlogStreamer,
				currentCoordinates:       tt.fields.currentCoordinates,
				currentCoordinatesMutex:  tt.fields.currentCoordinatesMutex,
				LastAppliedRowsEventHint: tt.fields.LastAppliedRowsEventHint,
				MysqlContext:             tt.fields.MysqlContext,
				currentTx:                tt.fields.currentTx,
				currentBinlogEntry:       tt.fields.currentBinlogEntry,
				txCount:                  tt.fields.txCount,
				currentFde:               tt.fields.currentFde,
				currentQuery:             tt.fields.currentQuery,
				currentSqlB64:            tt.fields.currentSqlB64,
				appendB64SqlBs:           tt.fields.appendB64SqlBs,
				ReMap:                    tt.fields.ReMap,
				wg:                       tt.fields.wg,
				shutdown:                 tt.fields.shutdown,
				shutdownCh:               tt.fields.shutdownCh,
				shutdownLock:             tt.fields.shutdownLock,
			}
			gotColumns, gotUniqueKeys, err := b.InspectTableColumnsAndUniqueKeys(tt.args.databaseName, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("BinlogReader.InspectTableColumnsAndUniqueKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotColumns, tt.wantColumns) {
				t.Errorf("BinlogReader.InspectTableColumnsAndUniqueKeys() gotColumns = %v, want %v", gotColumns, tt.wantColumns)
			}
			if !reflect.DeepEqual(gotUniqueKeys, tt.wantUniqueKeys) {
				t.Errorf("BinlogReader.InspectTableColumnsAndUniqueKeys() gotUniqueKeys = %v, want %v", gotUniqueKeys, tt.wantUniqueKeys)
			}
		})
	}
}

func TestBinlogReader_getCandidateUniqueKeys(t *testing.T) {
	type fields struct {
		logger                   *log.Entry
		connectionConfig         *mysql.ConnectionConfig
		db                       *gosql.DB
		binlogSyncer             *replication.BinlogSyncer
		binlogStreamer           *replication.BinlogStreamer
		currentCoordinates       base.BinlogCoordinates
		currentCoordinatesMutex  *sync.Mutex
		LastAppliedRowsEventHint base.BinlogCoordinates
		MysqlContext             *config.MySQLDriverConfig
		currentTx                *BinlogTx
		currentBinlogEntry       *BinlogEntry
		txCount                  int
		currentFde               string
		currentQuery             *bytes.Buffer
		currentSqlB64            *bytes.Buffer
		appendB64SqlBs           []byte
		ReMap                    map[string]*regexp.Regexp
		wg                       sync.WaitGroup
		shutdown                 bool
		shutdownCh               chan struct{}
		shutdownLock             sync.Mutex
	}
	type args struct {
		databaseName string
		tableName    string
	}
	tests := []struct {
		name           string
		fields         fields
		args           args
		wantUniqueKeys [](*mysql.UniqueKey)
		wantErr        bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogReader{
				logger:                   tt.fields.logger,
				connectionConfig:         tt.fields.connectionConfig,
				db:                       tt.fields.db,
				binlogSyncer:             tt.fields.binlogSyncer,
				binlogStreamer:           tt.fields.binlogStreamer,
				currentCoordinates:       tt.fields.currentCoordinates,
				currentCoordinatesMutex:  tt.fields.currentCoordinatesMutex,
				LastAppliedRowsEventHint: tt.fields.LastAppliedRowsEventHint,
				MysqlContext:             tt.fields.MysqlContext,
				currentTx:                tt.fields.currentTx,
				currentBinlogEntry:       tt.fields.currentBinlogEntry,
				txCount:                  tt.fields.txCount,
				currentFde:               tt.fields.currentFde,
				currentQuery:             tt.fields.currentQuery,
				currentSqlB64:            tt.fields.currentSqlB64,
				appendB64SqlBs:           tt.fields.appendB64SqlBs,
				ReMap:                    tt.fields.ReMap,
				wg:                       tt.fields.wg,
				shutdown:                 tt.fields.shutdown,
				shutdownCh:               tt.fields.shutdownCh,
				shutdownLock:             tt.fields.shutdownLock,
			}
			gotUniqueKeys, err := b.getCandidateUniqueKeys(tt.args.databaseName, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("BinlogReader.getCandidateUniqueKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotUniqueKeys, tt.wantUniqueKeys) {
				t.Errorf("BinlogReader.getCandidateUniqueKeys() = %v, want %v", gotUniqueKeys, tt.wantUniqueKeys)
			}
		})
	}
}

func TestGenDDLSQL(t *testing.T) {
	type args struct {
		sql    string
		schema string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenDDLSQL(tt.args.sql, tt.args.schema)
			if (err != nil) != tt.wantErr {
				t.Errorf("GenDDLSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GenDDLSQL() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_resolveDDLSQL(t *testing.T) {
	type args struct {
		sql string
	}
	tests := []struct {
		name     string
		args     args
		wantSqls []string
		wantOk   bool
		wantErr  bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSqls, gotOk, err := resolveDDLSQL(tt.args.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("resolveDDLSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotSqls, tt.wantSqls) {
				t.Errorf("resolveDDLSQL() gotSqls = %v, want %v", gotSqls, tt.wantSqls)
			}
			if gotOk != tt.wantOk {
				t.Errorf("resolveDDLSQL() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func Test_parserDDLTableName(t *testing.T) {
	type args struct {
		sql string
	}
	tests := []struct {
		name    string
		args    args
		want    config.Table
		wantErr bool
	}{
		// TODO: Add test cases.
		{"t1", args{"drop table if exists `regexp`"}, config.Table{}, false},
		{"t2", args{"drop table if exists UFregexp,regexp_test001,regexp_test002,regexp_test003,`regexp`;"}, config.Table{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parserDDLTableName(tt.args.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("parserDDLTableName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parserDDLTableName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogReader_skipQueryDDL(t *testing.T) {
	type fields struct {
		logger                   *log.Entry
		connectionConfig         *mysql.ConnectionConfig
		db                       *gosql.DB
		binlogSyncer             *replication.BinlogSyncer
		binlogStreamer           *replication.BinlogStreamer
		currentCoordinates       base.BinlogCoordinates
		currentCoordinatesMutex  *sync.Mutex
		LastAppliedRowsEventHint base.BinlogCoordinates
		MysqlContext             *config.MySQLDriverConfig
		currentTx                *BinlogTx
		currentBinlogEntry       *BinlogEntry
		txCount                  int
		currentFde               string
		currentQuery             *bytes.Buffer
		currentSqlB64            *bytes.Buffer
		appendB64SqlBs           []byte
		ReMap                    map[string]*regexp.Regexp
		wg                       sync.WaitGroup
		shutdown                 bool
		shutdownCh               chan struct{}
		shutdownLock             sync.Mutex
	}
	type args struct {
		sql    string
		schema string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogReader{
				logger:                   tt.fields.logger,
				connectionConfig:         tt.fields.connectionConfig,
				db:                       tt.fields.db,
				binlogSyncer:             tt.fields.binlogSyncer,
				binlogStreamer:           tt.fields.binlogStreamer,
				currentCoordinates:       tt.fields.currentCoordinates,
				currentCoordinatesMutex:  tt.fields.currentCoordinatesMutex,
				LastAppliedRowsEventHint: tt.fields.LastAppliedRowsEventHint,
				MysqlContext:             tt.fields.MysqlContext,
				currentTx:                tt.fields.currentTx,
				currentBinlogEntry:       tt.fields.currentBinlogEntry,
				txCount:                  tt.fields.txCount,
				currentFde:               tt.fields.currentFde,
				currentQuery:             tt.fields.currentQuery,
				currentSqlB64:            tt.fields.currentSqlB64,
				appendB64SqlBs:           tt.fields.appendB64SqlBs,
				ReMap:                    tt.fields.ReMap,
				wg:                       tt.fields.wg,
				shutdown:                 tt.fields.shutdown,
				shutdownCh:               tt.fields.shutdownCh,
				shutdownLock:             tt.fields.shutdownLock,
			}
			if got := b.skipQueryDDL(tt.args.sql, tt.args.schema); got != tt.want {
				t.Errorf("BinlogReader.skipQueryDDL() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_skipQueryEvent(t *testing.T) {
	type args struct {
		sql string
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
			if got := skipQueryEvent(tt.args.sql); got != tt.want {
				t.Errorf("skipQueryEvent() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogReader_skipRowEvent(t *testing.T) {
	type fields struct {
		logger                   *log.Entry
		connectionConfig         *mysql.ConnectionConfig
		db                       *gosql.DB
		binlogSyncer             *replication.BinlogSyncer
		binlogStreamer           *replication.BinlogStreamer
		currentCoordinates       base.BinlogCoordinates
		currentCoordinatesMutex  *sync.Mutex
		LastAppliedRowsEventHint base.BinlogCoordinates
		MysqlContext             *config.MySQLDriverConfig
		currentTx                *BinlogTx
		currentBinlogEntry       *BinlogEntry
		txCount                  int
		currentFde               string
		currentQuery             *bytes.Buffer
		currentSqlB64            *bytes.Buffer
		appendB64SqlBs           []byte
		ReMap                    map[string]*regexp.Regexp
		wg                       sync.WaitGroup
		shutdown                 bool
		shutdownCh               chan struct{}
		shutdownLock             sync.Mutex
	}
	type args struct {
		schema string
		table  string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogReader{
				logger:                   tt.fields.logger,
				connectionConfig:         tt.fields.connectionConfig,
				db:                       tt.fields.db,
				binlogSyncer:             tt.fields.binlogSyncer,
				binlogStreamer:           tt.fields.binlogStreamer,
				currentCoordinates:       tt.fields.currentCoordinates,
				currentCoordinatesMutex:  tt.fields.currentCoordinatesMutex,
				LastAppliedRowsEventHint: tt.fields.LastAppliedRowsEventHint,
				MysqlContext:             tt.fields.MysqlContext,
				currentTx:                tt.fields.currentTx,
				currentBinlogEntry:       tt.fields.currentBinlogEntry,
				txCount:                  tt.fields.txCount,
				currentFde:               tt.fields.currentFde,
				currentQuery:             tt.fields.currentQuery,
				currentSqlB64:            tt.fields.currentSqlB64,
				appendB64SqlBs:           tt.fields.appendB64SqlBs,
				ReMap:                    tt.fields.ReMap,
				wg:                       tt.fields.wg,
				shutdown:                 tt.fields.shutdown,
				shutdownCh:               tt.fields.shutdownCh,
				shutdownLock:             tt.fields.shutdownLock,
			}
			if got := b.skipRowEvent(tt.args.schema, tt.args.table); got != tt.want {
				t.Errorf("BinlogReader.skipRowEvent() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogReader_matchString(t *testing.T) {
	type fields struct {
		logger                   *log.Entry
		connectionConfig         *mysql.ConnectionConfig
		db                       *gosql.DB
		binlogSyncer             *replication.BinlogSyncer
		binlogStreamer           *replication.BinlogStreamer
		currentCoordinates       base.BinlogCoordinates
		currentCoordinatesMutex  *sync.Mutex
		LastAppliedRowsEventHint base.BinlogCoordinates
		MysqlContext             *config.MySQLDriverConfig
		currentTx                *BinlogTx
		currentBinlogEntry       *BinlogEntry
		txCount                  int
		currentFde               string
		currentQuery             *bytes.Buffer
		currentSqlB64            *bytes.Buffer
		appendB64SqlBs           []byte
		ReMap                    map[string]*regexp.Regexp
		wg                       sync.WaitGroup
		shutdown                 bool
		shutdownCh               chan struct{}
		shutdownLock             sync.Mutex
	}
	type args struct {
		pattern string
		t       string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogReader{
				logger:                   tt.fields.logger,
				connectionConfig:         tt.fields.connectionConfig,
				db:                       tt.fields.db,
				binlogSyncer:             tt.fields.binlogSyncer,
				binlogStreamer:           tt.fields.binlogStreamer,
				currentCoordinates:       tt.fields.currentCoordinates,
				currentCoordinatesMutex:  tt.fields.currentCoordinatesMutex,
				LastAppliedRowsEventHint: tt.fields.LastAppliedRowsEventHint,
				MysqlContext:             tt.fields.MysqlContext,
				currentTx:                tt.fields.currentTx,
				currentBinlogEntry:       tt.fields.currentBinlogEntry,
				txCount:                  tt.fields.txCount,
				currentFde:               tt.fields.currentFde,
				currentQuery:             tt.fields.currentQuery,
				currentSqlB64:            tt.fields.currentSqlB64,
				appendB64SqlBs:           tt.fields.appendB64SqlBs,
				ReMap:                    tt.fields.ReMap,
				wg:                       tt.fields.wg,
				shutdown:                 tt.fields.shutdown,
				shutdownCh:               tt.fields.shutdownCh,
				shutdownLock:             tt.fields.shutdownLock,
			}
			if got := b.matchString(tt.args.pattern, tt.args.t); got != tt.want {
				t.Errorf("BinlogReader.matchString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogReader_matchDB(t *testing.T) {
	type fields struct {
		logger                   *log.Entry
		connectionConfig         *mysql.ConnectionConfig
		db                       *gosql.DB
		binlogSyncer             *replication.BinlogSyncer
		binlogStreamer           *replication.BinlogStreamer
		currentCoordinates       base.BinlogCoordinates
		currentCoordinatesMutex  *sync.Mutex
		LastAppliedRowsEventHint base.BinlogCoordinates
		MysqlContext             *config.MySQLDriverConfig
		currentTx                *BinlogTx
		currentBinlogEntry       *BinlogEntry
		txCount                  int
		currentFde               string
		currentQuery             *bytes.Buffer
		currentSqlB64            *bytes.Buffer
		appendB64SqlBs           []byte
		ReMap                    map[string]*regexp.Regexp
		wg                       sync.WaitGroup
		shutdown                 bool
		shutdownCh               chan struct{}
		shutdownLock             sync.Mutex
	}
	type args struct {
		patternDBS []*config.DataSource
		a          string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogReader{
				logger:                   tt.fields.logger,
				connectionConfig:         tt.fields.connectionConfig,
				db:                       tt.fields.db,
				binlogSyncer:             tt.fields.binlogSyncer,
				binlogStreamer:           tt.fields.binlogStreamer,
				currentCoordinates:       tt.fields.currentCoordinates,
				currentCoordinatesMutex:  tt.fields.currentCoordinatesMutex,
				LastAppliedRowsEventHint: tt.fields.LastAppliedRowsEventHint,
				MysqlContext:             tt.fields.MysqlContext,
				currentTx:                tt.fields.currentTx,
				currentBinlogEntry:       tt.fields.currentBinlogEntry,
				txCount:                  tt.fields.txCount,
				currentFde:               tt.fields.currentFde,
				currentQuery:             tt.fields.currentQuery,
				currentSqlB64:            tt.fields.currentSqlB64,
				appendB64SqlBs:           tt.fields.appendB64SqlBs,
				ReMap:                    tt.fields.ReMap,
				wg:                       tt.fields.wg,
				shutdown:                 tt.fields.shutdown,
				shutdownCh:               tt.fields.shutdownCh,
				shutdownLock:             tt.fields.shutdownLock,
			}
			if got := b.matchDB(tt.args.patternDBS, tt.args.a); got != tt.want {
				t.Errorf("BinlogReader.matchDB() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogReader_Close(t *testing.T) {
	type fields struct {
		logger                   *log.Entry
		connectionConfig         *mysql.ConnectionConfig
		db                       *gosql.DB
		binlogSyncer             *replication.BinlogSyncer
		binlogStreamer           *replication.BinlogStreamer
		currentCoordinates       base.BinlogCoordinates
		currentCoordinatesMutex  *sync.Mutex
		LastAppliedRowsEventHint base.BinlogCoordinates
		MysqlContext             *config.MySQLDriverConfig
		currentTx                *BinlogTx
		currentBinlogEntry       *BinlogEntry
		txCount                  int
		currentFde               string
		currentQuery             *bytes.Buffer
		currentSqlB64            *bytes.Buffer
		appendB64SqlBs           []byte
		ReMap                    map[string]*regexp.Regexp
		wg                       sync.WaitGroup
		shutdown                 bool
		shutdownCh               chan struct{}
		shutdownLock             sync.Mutex
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
			b := &BinlogReader{
				logger:                   tt.fields.logger,
				connectionConfig:         tt.fields.connectionConfig,
				db:                       tt.fields.db,
				binlogSyncer:             tt.fields.binlogSyncer,
				binlogStreamer:           tt.fields.binlogStreamer,
				currentCoordinates:       tt.fields.currentCoordinates,
				currentCoordinatesMutex:  tt.fields.currentCoordinatesMutex,
				LastAppliedRowsEventHint: tt.fields.LastAppliedRowsEventHint,
				MysqlContext:             tt.fields.MysqlContext,
				currentTx:                tt.fields.currentTx,
				currentBinlogEntry:       tt.fields.currentBinlogEntry,
				txCount:                  tt.fields.txCount,
				currentFde:               tt.fields.currentFde,
				currentQuery:             tt.fields.currentQuery,
				currentSqlB64:            tt.fields.currentSqlB64,
				appendB64SqlBs:           tt.fields.appendB64SqlBs,
				ReMap:                    tt.fields.ReMap,
				wg:                       tt.fields.wg,
				shutdown:                 tt.fields.shutdown,
				shutdownCh:               tt.fields.shutdownCh,
				shutdownLock:             tt.fields.shutdownLock,
			}
			if err := b.Close(); (err != nil) != tt.wantErr {
				t.Errorf("BinlogReader.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_genTableName(t *testing.T) {
	type args struct {
		schema string
		table  []*config.Table
	}
	tests := []struct {
		name string
		args args
		want config.DataSource
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := genTableName(tt.args.schema, tt.args.table); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("genTableName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogReader_matchTable(t *testing.T) {
	type fields struct {
		logger                   *log.Entry
		connectionConfig         *mysql.ConnectionConfig
		db                       *gosql.DB
		binlogSyncer             *replication.BinlogSyncer
		binlogStreamer           *replication.BinlogStreamer
		currentCoordinates       base.BinlogCoordinates
		currentCoordinatesMutex  *sync.Mutex
		LastAppliedRowsEventHint base.BinlogCoordinates
		MysqlContext             *config.MySQLDriverConfig
		currentTx                *BinlogTx
		currentBinlogEntry       *BinlogEntry
		txCount                  int
		currentFde               string
		currentQuery             *bytes.Buffer
		currentSqlB64            *bytes.Buffer
		appendB64SqlBs           []byte
		ReMap                    map[string]*regexp.Regexp
		wg                       sync.WaitGroup
		shutdown                 bool
		shutdownCh               chan struct{}
		shutdownLock             sync.Mutex
	}
	type args struct {
		patternTBS []*config.DataSource
		t          config.DataSource
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogReader{
				logger:                   tt.fields.logger,
				connectionConfig:         tt.fields.connectionConfig,
				db:                       tt.fields.db,
				binlogSyncer:             tt.fields.binlogSyncer,
				binlogStreamer:           tt.fields.binlogStreamer,
				currentCoordinates:       tt.fields.currentCoordinates,
				currentCoordinatesMutex:  tt.fields.currentCoordinatesMutex,
				LastAppliedRowsEventHint: tt.fields.LastAppliedRowsEventHint,
				MysqlContext:             tt.fields.MysqlContext,
				currentTx:                tt.fields.currentTx,
				currentBinlogEntry:       tt.fields.currentBinlogEntry,
				txCount:                  tt.fields.txCount,
				currentFde:               tt.fields.currentFde,
				currentQuery:             tt.fields.currentQuery,
				currentSqlB64:            tt.fields.currentSqlB64,
				appendB64SqlBs:           tt.fields.appendB64SqlBs,
				ReMap:                    tt.fields.ReMap,
				wg:                       tt.fields.wg,
				shutdown:                 tt.fields.shutdown,
				shutdownCh:               tt.fields.shutdownCh,
				shutdownLock:             tt.fields.shutdownLock,
			}
			if got := b.matchTable(tt.args.patternTBS, tt.args.t); got != tt.want {
				t.Errorf("BinlogReader.matchTable() = %v, want %v", got, tt.want)
			}
		})
	}
}
