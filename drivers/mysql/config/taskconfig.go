package config

import (
	"github.com/actiontech/dtle/drivers/mysql/mysql/mysqlconfig"
	"sync/atomic"
	"time"
)

const (
	channelBufferSize = 600
	defaultNumRetries = 5
	defaultChunkSize = 2000
	defaultNumWorkers = 1
	defaultMsgBytes = 20 * 1024
	DefaultClusterID     = "dtle-nats"
)

type DtleTaskConfig struct {
	//Ref:http://dev.mysql.com/doc/refman/5.7/en/replication-options-slave.html#option_mysqld_replicate-do-table
	ReplicateDoDb        []*mysqlconfig.DataSource `codec:"ReplicateDoDb"`
	ReplicateIgnoreDb    []*mysqlconfig.DataSource `codec:"ReplicateIgnoreDb"`
	DropTableIfExists    bool                      `codec:"DropTableIfExists"`
	ExpandSyntaxSupport  bool                      `codec:"ExpandSyntaxSupport"`
	ReplChanBufferSize   int64                     `codec:"ReplChanBufferSize"`
	MsgBytesLimit        int                       `codec:"MsgBytesLimit"`
	TrafficAgainstLimits int                       `codec:"TrafficAgainstLimits"`
	MaxRetries           int64                     `codec:"MaxRetries"`
	ChunkSize            int64                     `codec:"ChunkSize"`
	SqlFilter            []string                  `codec:"SqlFilter"`
	GroupMaxSize         int                       `codec:"GroupMaxSize"`
	GroupTimeout         int                       `codec:"GroupTimeout"`
	Gtid                 string              `codec:"Gtid"`
	BinlogFile           string              `codec:"BinlogFile"`
	BinlogPos            int64                     `codec:"BinlogPos"`
	GtidStart            string                    `codec:"GtidStart"`
	AutoGtid             bool                      `codec:"AutoGtid"`
	BinlogRelay          bool                      `codec:"BinlogRelay"`

	ParallelWorkers int `codec:"ParallelWorkers"`

	SkipCreateDbTable   bool                          `codec:"SkipCreateDbTable"`
	SkipPrivilegeCheck  bool                          `codec:"SkipPrivilegeCheck"`
	SkipIncrementalCopy bool                          `codec:"SkipIncrementalCopy"`
	ConnectionConfig    *mysqlconfig.ConnectionConfig `codec:"ConnectionConfig"`
	KafkaConfig         *KafkaConfig                  `codec:"KafkaConfig"`
}

func (d *DtleTaskConfig) SetDefaultForEmpty() {
	if d.MaxRetries <= 0 {
		d.MaxRetries = defaultNumRetries
	}
	if d.ChunkSize <= 0 {
		d.ChunkSize = defaultChunkSize
	}
	if d.ReplChanBufferSize <= 0 {
		d.ReplChanBufferSize = channelBufferSize
	}
	if d.ParallelWorkers <= 0 {
		d.ParallelWorkers = defaultNumWorkers
	}
	if d.MsgBytesLimit <= 0 {
		d.MsgBytesLimit = defaultMsgBytes
	}
	if d.GroupMaxSize == 0 {
		d.GroupMaxSize = 1
	}
	if d.GroupTimeout == 0 {
		d.GroupTimeout = 100
	}

	if d.ConnectionConfig == nil {
		d.ConnectionConfig = &mysqlconfig.ConnectionConfig{}
	}
	if d.ConnectionConfig.Charset == "" {
		d.ConnectionConfig.Charset = "utf8mb4"
	}
}

type MySQLDriverConfig struct {
	DtleTaskConfig

	RowsEstimate     int64
	DeltaEstimate    int64
	BinlogRowImage   string
	RowCopyStartTime time.Time
	RowCopyEndTime   time.Time
	TotalRowsCopied  int64

	Stage string
}

// ElapsedRowCopyTime returns time since starting to copy chunks of rows
func (m *MySQLDriverConfig) MarkRowCopyEndTime() {
	m.RowCopyEndTime = time.Now()
}

// MarkRowCopyStartTime
func (m *MySQLDriverConfig) MarkRowCopyStartTime() {
	m.RowCopyStartTime = time.Now()
}

// ElapsedRowCopyTime returns time since starting to copy chunks of rows
func (m *MySQLDriverConfig) ElapsedRowCopyTime() time.Duration {
	if m.RowCopyStartTime.IsZero() {
		// Row copy hasn't started yet
		return 0
	}

	if m.RowCopyEndTime.IsZero() {
		return time.Since(m.RowCopyStartTime)
	}
	return m.RowCopyEndTime.Sub(m.RowCopyStartTime)
}

// GetTotalRowsCopied returns the accurate number of rows being copied (affected)
// This is not exactly the same as the rows being iterated via chunks, but potentially close enough
func (m *MySQLDriverConfig) GetTotalRowsCopied() int64 {
	return atomic.LoadInt64(&m.TotalRowsCopied)
}

type KafkaConfig struct {
	Brokers   []string
	Topic     string
	Converter string
	NatsAddr  string
	Gtid      string // TODO remove?
}

