package models

import (
	"time"
)

type JobListReqV2 struct {
	FilterJobType   string `query:"filter_job_type"`
	FilterJobName   string `json:"filter_job_name"`
	FilterJobStatus string `json:"filter_job_status"`
	OrderBy         string `json:"order_by"`
}

type JobStep struct {
	StepName      string  `json:"step_name"`
	StepStatus    string  `json:"step_status"`
	StepSchedule  float64 `json:"step_schedule"`
	JobCreateTime string  `json:"job_create_time"`
}

type JobListItemV2 struct {
	JobId                string    `json:"job_id"`
	JobName              string    `json:"job_name"`
	JobStatus            string    `json:"job_status"`
	JobStatusDescription string    `json:"job_status_description"`
	JobCreateTime        string    `json:"job_create_time"`
	SrcAddrList          []string  `json:"src_addr_list"`
	DstAddrList          []string  `json:"dst_addr_list"`
	User                 string    `json:"user"`
	JobSteps             []JobStep `json:"job_steps"`
}

type JobListRespV2 struct {
	Jobs []JobListItemV2 `json:"jobs"`
	BaseResp
}

type MysqlToMysqlJobDetailReqV2 struct {
	JobId string `query:"job_id" validate:"required"`
}

type JobBaseInfo struct {
	JobId                string    `json:"job_id"`
	JobName              string    `json:"job_name"`
	SubscriptionTopic    string    `json:"subscription_topic"`
	JobStatus            string    `json:"job_status"`
	JobStatusDescription string    `json:"job_status_description"`
	JobCreateTime        string    `json:"job_create_time"`
	JobSteps             []JobStep `json:"job_steps"`
	Delay                int64     `json:"delay"`
}

type DtleNodeInfo struct {
	NodeAddr   string   `json:"node_addr"`
	NodeId     string   `json:"node_id"`
	DataSource []string `json:"data_source"`
}

type DataBase struct {
	Addr     string `json:"addr"`
	User     string `json:"user"`
	PassWord string `json:"pass_word"`
}

type ConnectionInfo struct {
	SrcDataBaseList []MysqlConnectionConfig
	DstDataBaseList []MysqlConnectionConfig
}

type Configuration struct {
	RelayMechanism         bool `json:"relay_mechanism"`
	FailOver               bool `json:"fail_over"`
	RetryTime              int  `json:"retry_time"`
	ConcurrentPlayback     int  `json:"concurrent_playback"`
	MessageQueueBufferSize int  `json:"message_queue_buffer_size"`
	SourcePacketSize       int  `json:"source_packet_size"`
	ChunkSize              int  `json:"chunk_size"`
}

type BasicTaskProfile struct {
	JobBaseInfo     JobBaseInfo        `json:"job_base_info"`
	DtleNodeInfos   []DtleNodeInfo     `json:"dtle_node_infos"`
	ConnectionInfo  ConnectionInfo     `json:"connection_info"`
	Configuration   Configuration      `json:"configuration"`
	OperationObject []MysqlTableConfig `json:"operation_object"`
}

type TaskLog struct {
	StartTime    time.Time `json:"start_time"`
	SpecificTask string    `json:"specific_task"`
}

type MysqlToMysqlJobDetailRespV2 struct {
	JobId          string              `json:"job_id"`           // Compatible with old version v2 interface
	JobName        string              `json:"job_name"`         // Compatible with old version v2 interface
	Failover       bool                `json:"failover"`         // Compatible with old version v2 interface
	SrcTaskDetail  MysqlSrcTaskDetail  `json:"src_task_detail"`  // Compatible with old version v2 interface
	DestTaskDetail MysqlDestTaskDetail `json:"dest_task_detail"` // Compatible with old version v2 interface

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
	JobName      string `json:"job_name" validate:"required"`
	JobId        string `json:"job_id"`
	TaskStepName string `json:"task_step_name"`
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
	JobName      string `json:"job_name" validate:"required"`
	JobId        string `json:"job_id"`
	TaskStepName string `json:"task_step_name"`
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
	JobId          string              `json:"job_id"`           // Compatible with old version v2 interface
	JobName        string              `json:"job_name"`         // Compatible with old version v2 interface
	Failover       bool                `json:"failover"`         // Compatible with old version v2 interface
	SrcTaskDetail  MysqlSrcTaskDetail  `json:"src_task_detail"`  // Compatible with old version v2 interface
	DestTaskDetail KafkaDestTaskDetail `json:"dest_task_detail"` // Compatible with old version v2 interface

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
