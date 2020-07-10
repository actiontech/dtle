package config

import (
	"github.com/actiontech/dtle/drivers/mysql/mysql/config"
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

// TODO This is repetitive to MySQLDriverConfig. Consider merge in to one struct.
type DtleTaskConfig struct {
	ReplicateDoDb        []*mysql.DataSource `codec:"ReplicateDoDb"`
	ReplicateIgnoreDb    []*mysql.DataSource `codec:"ReplicateIgnoreDb"`
	DropTableIfExists    bool                `codec:"DropTableIfExists"`
	ExpandSyntaxSupport  bool                `codec:"ExpandSyntaxSupport"`
	ReplChanBufferSize   int64               `codec:"ReplChanBufferSize"`
	MsgBytesLimit        int                 `codec:"MsgBytesLimit"`
	TrafficAgainstLimits int                 `codec:"TrafficAgainstLimits"`
	MaxRetries           int64               `codec:"MaxRetries"`
	ChunkSize            int64               `codec:"ChunkSize"`
	SqlFilter            []string            `codec:"SqlFilter"`
	GroupMaxSize         int                 `codec:"GroupMaxSize"`
	GroupTimeout         int                 `codec:"GroupTimeout"`
	Gtid                 string              `codec:"Gtid"`
	BinlogFile           string              `codec:"BinlogFile"`
	BinlogPos            int64               `codec:"BinlogPos"`
	GtidStart            string              `codec:"GtidStart"`
	AutoGtid             bool                `codec:"AutoGtid"`
	BinlogRelay          bool                `codec:"BinlogRelay"`

	ParallelWorkers int `codec:"ParallelWorkers"`

	SkipCreateDbTable   bool                    `codec:"SkipCreateDbTable"`
	SkipPrivilegeCheck  bool                    `codec:"SkipPrivilegeCheck"`
	SkipIncrementalCopy bool                    `codec:"SkipIncrementalCopy"`
	ConnectionConfig    *mysql.ConnectionConfig `codec:"ConnectionConfig"`
	KafkaConfig         *KafkaConfig            `codec:"KafkaConfig"`
}

type MySQLDriverConfig struct {
	//Ref:http://dev.mysql.com/doc/refman/5.7/en/replication-options-slave.html#option_mysqld_replicate-do-table
	ReplicateDoDb        []*mysql.DataSource
	ReplicateIgnoreDb    []*mysql.DataSource
	DropTableIfExists    bool
	ExpandSyntaxSupport  bool
	ReplChanBufferSize   int64
	MsgBytesLimit        int
	TrafficAgainstLimits int
	MaxRetries           int64
	ChunkSize            int64
	SqlFilter            []string
	GroupMaxSize         int
	GroupTimeout         int
	Gtid                 string
	BinlogFile           string
	BinlogPos            int64
	GtidStart            string
	AutoGtid             bool
	BinlogRelay          bool

	ParallelWorkers int

	SkipCreateDbTable   bool
	SkipPrivilegeCheck  bool
	SkipIncrementalCopy bool

	ConnectionConfig *mysql.ConnectionConfig

	RowsEstimate     int64
	DeltaEstimate    int64
	BinlogRowImage   string
	RowCopyStartTime time.Time
	RowCopyEndTime   time.Time
	TotalDeltaCopied int64
	TotalRowsCopied  int64
	TotalRowsReplay  int64

	Stage string
}

func (a *MySQLDriverConfig) SetDefault() *MySQLDriverConfig {
	result := *a

	if result.MaxRetries <= 0 {
		result.MaxRetries = defaultNumRetries
	}
	if result.ChunkSize <= 0 {
		result.ChunkSize = defaultChunkSize
	}
	if result.ReplChanBufferSize <= 0 {
		result.ReplChanBufferSize = channelBufferSize
	}
	if result.ParallelWorkers <= 0 {
		result.ParallelWorkers = defaultNumWorkers
	}
	if result.MsgBytesLimit <= 0 {
		result.MsgBytesLimit = defaultMsgBytes
	}
	if result.GroupMaxSize == 0 {
		result.GroupMaxSize = 1
	}
	if result.GroupTimeout == 0 {
		result.GroupTimeout = 100
	}

	if result.ConnectionConfig == nil {
		result.ConnectionConfig = &mysql.ConnectionConfig{}
	}
	if "" == result.ConnectionConfig.Charset {
		result.ConnectionConfig.Charset = "utf8mb4"
	}
	return &result
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

func (m *MySQLDriverConfig) GetTotalRowsReplay() int64 {
	return atomic.LoadInt64(&m.TotalRowsReplay)
}

func (m *MySQLDriverConfig) GetTotalDeltaCopied() int64 {
	return atomic.LoadInt64(&m.TotalDeltaCopied)
}

type KafkaConfig struct {
	Brokers   []string
	Topic     string
	Converter string
	NatsAddr  string
	Gtid      string // TODO remove?
}

