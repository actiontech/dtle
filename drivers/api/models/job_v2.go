package models

import (
	"time"
)

type JobListReqV2 struct {
	FilterJobType string `query:"filter_job_type"`
}

type JobListItemV2 struct {
	JobId                string `json:"job_id"`
	JobName              string `json:"job_name"`
	JobStatus            string `json:"job_status"`
	JobStatusDescription string `json:"job_status_description"`
}

type JobListRespV2 struct {
	Jobs []JobListItemV2 `json:"jobs"`
	BaseResp
}

type MysqlToMysqlJobDetailReqV2 struct {
	JobId string `query:"job_id" validate:"required"`
}

type MysqlToMysqlJobDetailRespV2 struct {
	JobId          string              `json:"job_id"`
	JobName        string              `json:"job_name"`
	Failover       bool                `json:"failover"`
	SrcTaskDetail  MysqlSrcTaskDetail  `json:"src_task_detail"`
	DestTaskDetail MysqlDestTaskDetail `json:"dest_task_detail"`
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
}

type MysqlDestTaskConfig struct {
	TaskName              string                 `json:"task_name" validate:"required"`
	NodeId                string                 `json:"node_id,omitempty"`
	ParallelWorkers       int                    `json:"parallel_workers"`
	MysqlConnectionConfig *MysqlConnectionConfig `json:"mysql_connection_config" validate:"required"`
}

type MysqlDataSourceConfig struct {
	TableSchema       string              `json:"table_schema"`
	TableSchemaRename *string             `json:"table_schema_rename"`
	Tables            []*MysqlTableConfig `json:"tables"`
}

type MysqlTableConfig struct {
	TableName     string   `json:"table_name"`
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
	JobName string `json:"job_name" validate:"required"`
	JobId   string `json:"job_id"`
	// failover default:true
	Failover                 *bool                `json:"failover" example:"true"`
	IsMysqlPasswordEncrypted bool                 `json:"is_mysql_password_encrypted"`
	SrcTask                  *MysqlSrcTaskConfig  `json:"src_task" validate:"required"`
	DestTask                 *MysqlDestTaskConfig `json:"dest_task" validate:"required"`
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
	JobName string `json:"job_name" validate:"required"`
	JobId   string `json:"job_id"`
	// failover default:true
	Failover                 *bool                `json:"failover" example:"true"`
	IsMysqlPasswordEncrypted bool                 `json:"is_mysql_password_encrypted"`
	SrcTask                  *MysqlSrcTaskConfig  `json:"src_task" validate:"required"`
	DestTask                 *KafkaDestTaskConfig `json:"dest_task" validate:"required"`
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
	JobId          string              `json:"job_id"`
	JobName        string              `json:"job_name"`
	Failover       bool                `json:"failover"`
	SrcTaskDetail  MysqlSrcTaskDetail  `json:"src_task_detail"`
	DestTaskDetail KafkaDestTaskDetail `json:"dest_task_detail"`
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
	JobId   string `form:"job_id" validate:"required"`
}

type DeleteJobRespV2 struct {
	BaseResp
}
