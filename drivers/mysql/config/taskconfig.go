package config

import (
	"github.com/actiontech/dtle/drivers/mysql/kafka"
	"github.com/actiontech/dtle/drivers/mysql/mysql/config"
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
	KafkaConfig         *kafka.KafkaConfig      `codec:"KafkaConfig"`
}

