package common

import (
	"time"

	"github.com/actiontech/dtle/drivers/mysql/mysql/mysqlconfig"
)

const (
	channelBufferSize = 32
	defaultChunkSize  = 2000
	defaultNumWorkers = 1
	DefaultClusterID  = "dtle-nats"
)

type DtleTaskConfig struct {
	//Ref:http://dev.mysql.com/doc/refman/5.7/en/replication-options-slave.html#option_mysqld_replicate-do-table
	ReplicateDoDb       []*DataSource `codec:"ReplicateDoDb"`
	ReplicateIgnoreDb   []*DataSource `codec:"ReplicateIgnoreDb"`
	DropTableIfExists   bool          `codec:"DropTableIfExists"`
	ExpandSyntaxSupport bool          `codec:"ExpandSyntaxSupport"`
	ReplChanBufferSize  int64         `codec:"ReplChanBufferSize"`
	TrafficAgainstLimits int      `codec:"TrafficAgainstLimits"`
	ChunkSize            int64    `codec:"ChunkSize"`
	SqlFilter            []string `codec:"SqlFilter"`
	GroupMaxSize         int      `codec:"GroupMaxSize"`
	GroupTimeout         int      `codec:"GroupTimeout"`
	Gtid                 string   `codec:"Gtid"`
	BinlogFile           string   `codec:"BinlogFile"`
	BinlogPos            int64    `codec:"BinlogPos"`
	GtidStart            string   `codec:"GtidStart"`
	AutoGtid             bool     `codec:"AutoGtid"`
	BinlogRelay          bool     `codec:"BinlogRelay"`

	ParallelWorkers int `codec:"ParallelWorkers"`

	SkipCreateDbTable   bool                          `codec:"SkipCreateDbTable"`
	SkipPrivilegeCheck  bool                          `codec:"SkipPrivilegeCheck"`
	SkipIncrementalCopy bool                          `codec:"SkipIncrementalCopy"`
	ConnectionConfig    *mysqlconfig.ConnectionConfig `codec:"ConnectionConfig"`
	KafkaConfig         *KafkaConfig                  `codec:"KafkaConfig"`
}

func (d *DtleTaskConfig) SetDefaultForEmpty() {
	if d.ChunkSize <= 0 {
		d.ChunkSize = defaultChunkSize
	}
	if d.ReplChanBufferSize <= 0 {
		d.ReplChanBufferSize = channelBufferSize
	}
	if d.ParallelWorkers <= 0 {
		d.ParallelWorkers = defaultNumWorkers
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

	if d.KafkaConfig != nil {
		if d.KafkaConfig.MessageGroupMaxSize == 0 {
			d.KafkaConfig.MessageGroupMaxSize = 1
		}
		if d.KafkaConfig.MessageGroupTimeout == 0 {
			d.KafkaConfig.MessageGroupTimeout = 100
		}
	}
}

type MySQLDriverConfig struct {
	DtleTaskConfig

	RowsEstimate     int64
	DeltaEstimate    int64
	BinlogRowImage   string
	RowCopyStartTime time.Time
	RowCopyEndTime   time.Time

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

type KafkaConfig struct {
	Brokers             []string
	Topic               string
	Converter           string
	TimeZone            string
	MessageGroupMaxSize uint64
	MessageGroupTimeout uint64

	TopicWithSchemaTable bool
}
