package models

import (
	"time"

	"github.com/actiontech/dtle/drivers/mysql/common"
)

type JobListReqV2 struct {
	FilterJobType   string `query:"filter_job_type"`
	FilterJobId     string `query:"filter_job_id"`
	FilterJobStatus string `query:"filter_job_status"`
	OrderBy         string `query:"order_by"`
}

type JobListRespV2 struct {
	Jobs []common.JobListItemV2 `json:"jobs"`
	BaseResp
}

type MysqlToMysqlJobDetailReqV2 struct {
	JobId string `query:"job_id" validate:"required"`
}

type JobBaseInfo struct {
	JobId             string           `json:"job_id"`
	SubscriptionTopic string           `json:"subscription_topic"`
	JobStatus         string           `json:"job_status"`
	JobCreateTime     string           `json:"job_create_time"`
	JobSteps          []common.JobStep `json:"job_steps"`
	Delay             int64            `json:"delay"`
}

type DtleNodeInfo struct {
	NodeAddr   string `json:"node_addr"`
	NodeId     string `json:"node_id"`
	DataSource string `json:"data_source"`
	Source     string `json:"source"`
}

type DataBase struct {
	Addr     string `json:"addr"`
	User     string `json:"user"`
	PassWord string `json:"pass_word"`
}

type ConnectionInfo struct {
	SrcDataBase MysqlConnectionConfig `json:"src_data_base"`
	DstDataBase MysqlConnectionConfig `json:"dst_data_base"`
	DstKafka    KafkaDestTaskConfig   `json:"dst_kafka"`
}

type Configuration struct {
	BinlogRelay        bool `json:"binlog_relay"`
	FailOver           bool `json:"fail_over"`
	RetryTimes         int  `json:"retry_times"`
	ParallelWorkers    int  `json:"parallel_workers"`
	ReplChanBufferSize int  `json:"repl_chan_buffer_size"`
	GroupMaxSize       int  `json:"group_max_size"`
	ChunkSize          int  `json:"chunk_size"`
	GroupTimeout       int  `json:"group_timeout"`
	DropTableIfExists  bool `json:"drop_table_if_exists"`
	SkipCreateDbTable  bool `json:"skip_create_db_table"`
}

type BasicTaskProfile struct {
	JobBaseInfo       JobBaseInfo              `json:"job_base_info"`
	DtleNodeInfos     []DtleNodeInfo           `json:"dtle_node_infos"`
	ConnectionInfo    ConnectionInfo           `json:"connection_info"`
	Configuration     Configuration            `json:"configuration"`
	ReplicateDoDb     []*MysqlDataSourceConfig `json:"replicate_do_db"`
	ReplicateIgnoreDb []*MysqlDataSourceConfig `json:"replicate_ignore_db"`
}

type TaskLog struct {
	TaskEvents   []TaskEvent `json:"task_events"`
	NodeId       string      `json:"node_id"`
	AllocationId string      `json:"allocation_id"`
	Address      string      `json:"address"`
	Target       string      `json:"target"`
}

type MysqlToMysqlJobDetailRespV2 struct {
	BasicTaskProfile BasicTaskProfile `json:"basic_task_profile"`
	TaskLogs         []TaskLog        `json:"task_logs"`
	BaseResp
}

type MysqlDestTaskDetail struct {
	Allocations []AllocationDetail  `json:"allocations"`
	TaskConfig  MysqlDestTaskConfig `json:"task_config"`
}

type MysqlSrcTaskDetail struct {
	Allocations []AllocationDetail `json:"allocations"`
	TaskConfig  MysqlSrcTaskConfig `json:"task_config"`
}

type AllocationDetail struct {
	NodeId       string     `json:"node_id"`
	AllocationId string     `json:"allocation_id"`
	TaskStatus   TaskStatus `json:"task_status"`
}

type TaskStatus struct {
	TaskEvents []TaskEvent `json:"task_events"`
	StartedAt  time.Time   `json:"started_at"`
	FinishedAt time.Time   `json:"finished_at"`
	Status     string      `json:"status"`
}

type TaskEvent struct {
	EventType  string `json:"event_type"`
	SetupError string `json:"setup_error"`
	Message    string `json:"message"`
	Time       string `json:"time"`
}

type MysqlSrcTaskConfig struct {
	TaskName              string                   `json:"task_name" validate:"required"`
	NodeId                string                   `json:"node_id,omitempty"`
	Gtid                  string                   `json:"gtid"`
	GroupMaxSize          int                      `json:"group_max_size"`
	ChunkSize             int64                    `json:"chunk_size"`
	DropTableIfExists     bool                     `json:"drop_table_if_exists"`
	SkipCreateDbTable     bool                     `json:"skip_create_db_table"`
	ReplChanBufferSize    int64                    `json:"repl_chan_buffer_size"`
	ReplicateDoDb         []*MysqlDataSourceConfig `json:"replicate_do_db"`
	ReplicateIgnoreDb     []*MysqlDataSourceConfig `json:"replicate_ignore_db"`
	MysqlConnectionConfig *MysqlConnectionConfig   `json:"mysql_connection_config" validate:"required"`
	BinlogRelay           bool                     `json:"binlog_relay"`
	GroupTimeout          int                      `json:"group_timeout"`
	WaitOnJob             string                   `json:"wait_on_job"`
	AutoGtid              bool                     `json:"auto_gtid"`
}

type MysqlDestTaskConfig struct {
	TaskName              string                 `json:"task_name" validate:"required"`
	NodeId                string                 `json:"node_id,omitempty"`
	ParallelWorkers       int                    `json:"parallel_workers"`
	MysqlConnectionConfig *MysqlConnectionConfig `json:"mysql_connection_config" validate:"required"`
}

type MysqlDataSourceConfig struct {
	TableSchema       string              `json:"table_schema"`
	TableSchemaRegex  string              `json:"table_schema_regex"`
	TableSchemaRename string              `json:"table_schema_rename"`
	Tables            []*MysqlTableConfig `json:"tables"`
}

type MysqlTableConfig struct {
	TableName     string   `json:"table_name"`
	TableRegex    string   `json:"table_regex"`
	TableRename   string   `json:"table_rename"`
	ColumnMapFrom []string `json:"column_map_from"`
	Where         string   `json:"where"`
}

type MysqlConnectionConfig struct {
	MysqlHost     string `json:"mysql_host" validate:"required"`
	MysqlPort     uint32 `json:"mysql_port" validate:"required"`
	MysqlUser     string `json:"mysql_user" validate:"required"`
	MysqlPassword string `json:"mysql_password" validate:"required"`
}

type CreateOrUpdateMysqlToMysqlJobParamV2 struct {
	JobId                    string               `json:"job_id" validate:"required"`
	TaskStepName             string               `json:"task_step_name"`
	Reverse                  bool                 `json:"reverse"`
	Failover                 *bool                `json:"failover" example:"true"`
	IsMysqlPasswordEncrypted bool                 `json:"is_mysql_password_encrypted"`
	SrcTask                  *MysqlSrcTaskConfig  `json:"src_task" validate:"required"`
	DestTask                 *MysqlDestTaskConfig `json:"dest_task" validate:"required"`
	Retry                    int                  `json:"retry"`
}

type CreateOrUpdateMysqlToMysqlJobRespV2 struct {
	CreateOrUpdateMysqlToMysqlJobParamV2 `json:"job"`
	EvalCreateIndex                      uint64 `json:"eval_create_index"`
	JobModifyIndex                       uint64 `json:"job_modify_index"`
	BaseResp
}

type KafkaDestTaskConfig struct {
	TaskName            string   `json:"task_name" validate:"required"`
	NodeId              string   `json:"node_id,omitempty"`
	BrokerAddrs         []string `json:"kafka_broker_addrs" validate:"required" example:"127.0.0.1:9092"`
	Topic               string   `json:"kafka_topic" validate:"required"`
	MessageGroupMaxSize uint64   `json:"message_group_max_size"`
	MessageGroupTimeout uint64   `json:"message_group_timeout"`
}

type CreateOrUpdateMysqlToKafkaJobParamV2 struct {
	JobId        string `json:"job_id" validate:"required"`
	TaskStepName string `json:"task_step_name"`
	// failover default:true
	Failover                 *bool                `json:"failover" example:"true"`
	IsMysqlPasswordEncrypted bool                 `json:"is_mysql_password_encrypted"`
	SrcTask                  *MysqlSrcTaskConfig  `json:"src_task" validate:"required"`
	DestTask                 *KafkaDestTaskConfig `json:"dest_task" validate:"required"`
	Retry                    int                  `json:"retry"`
}

type CreateOrUpdateMysqlToKafkaJobRespV2 struct {
	CreateOrUpdateMysqlToKafkaJobParamV2 `json:"job"`
	EvalCreateIndex                      uint64 `json:"eval_create_index"`
	JobModifyIndex                       uint64 `json:"job_modify_index"`
	BaseResp
}

type MysqlToKafkaJobDetailReqV2 struct {
	JobId string `query:"job_id" validate:"required"`
}

type MysqlToKafkaJobDetailRespV2 struct {
	BasicTaskProfile BasicTaskProfile `json:"basic_task_profile"`
	TaskLogs         []TaskLog        `json:"task_logs"`
	BaseResp
}

type KafkaDestTaskDetail struct {
	Allocations []AllocationDetail  `json:"allocations"`
	TaskConfig  KafkaDestTaskConfig `json:"task_config"`
}

type PauseJobReqV2 struct {
	JobId string `form:"job_id" validate:"required"`
}

type PauseJobRespV2 struct {
	BaseResp
}

type ResumeJobReqV2 struct {
	JobId string `form:"job_id" validate:"required"`
}

type ResumeJobRespV2 struct {
	BaseResp
}

type DeleteJobReqV2 struct {
	JobId string `form:"job_id" validate:"required"`
}

type DeleteJobRespV2 struct {
	BaseResp
}

type GetJobGtidReqV2 struct {
	JobId string `query:"job_id" validate:"required"`
}

type JobGtidResp struct {
	Gtid string `json:"gtid"`
	BaseResp
}

type ReverseStartReqV2 struct {
	JobId string `form:"job_id" validate:"required"`
}

type ReverseStartRespV2 struct {
	BaseResp
}

type ReverseJobReq struct {
	JobId         string         `json:"job_id" validate:"required"`
	ReverseConfig *ReverseConfig `json:"reverse_config"`
}

type ReverseConfig struct {
	SrcUser                  string `json:"src_user"`
	SrcPwd                   string `json:"src_pwd"`
	DestUser                 string `json:"dest_user"`
	DstPwd                   string `json:"dst_pwd"`
	IsMysqlPasswordEncrypted bool   `json:"is_mysql_password_encrypted"`
}
type ReverseJobResp struct {
	BaseResp
}
