package models

import (
	"time"

	"github.com/actiontech/dtle/drivers/mysql/common"
)

type JobListReqV2 struct {
	FilterJobType     string `query:"filter_job_type"`
	FilterJobId       string `query:"filter_job_id"`
	FilterJobStatus   string `query:"filter_job_status"`
	FilterJobSrcIP    string `query:"filter_job_src_ip"`
	FilterJobSrcPort  string `query:"filter_job_src_port"`
	FilterJobDestIP   string `query:"filter_job_dest_ip"`
	FilterJobDestPort string `query:"filter_job_dest_port"`
	OrderBy           string `query:"order_by"`
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

type ConnectionInfo struct {
	SrcDataBase DatabaseConnectionConfig `json:"src_data_base"`
	DstDataBase DatabaseConnectionConfig `json:"dst_data_base"`
	DstKafka    KafkaDestTaskConfig      `json:"dst_kafka"`
}

type Configuration struct {
	FailOver           bool          `json:"fail_over"`
	RetryTimes         int           `json:"retry_times"`
	ReplChanBufferSize int           `json:"repl_chan_buffer_size"`
	GroupMaxSize       int           `json:"group_max_size"`
	ChunkSize          int           `json:"chunk_size"`
	GroupTimeout       int           `json:"group_timeout"`
	DropTableIfExists  bool          `json:"drop_table_if_exists"`
	SkipCreateDbTable  bool          `json:"skip_create_db_table"`
	MySQLConfig        *MySQLConfig  `json:"my_sql_config,omitempty"`
	OracleConfig       *OracleConfig `json:"oracle_config"`
	// todo oracle逻辑实现后删除
	BinlogRelay           bool `json:"binlog_relay"`
	ParallelWorkers       int  `json:"parallel_workers"`
	UseMySQLDependency    bool `json:"use_my_sql_dependency"`
	DependencyHistorySize int  `json:"dependency_history_size"`
}

type MySQLConfig struct {
	BinlogRelay           bool `json:"binlog_relay"`
	ParallelWorkers       int  `json:"parallel_workers"`
	UseMySQLDependency    bool `json:"use_my_sql_dependency"`
	DependencyHistorySize int  `json:"dependency_history_size"`
}
type OracleConfig struct {
}

type TaskLog struct {
	TaskEvents   []TaskEvent `json:"task_events"`
	NodeId       string      `json:"node_id"`
	AllocationId string      `json:"allocation_id"`
	Address      string      `json:"address"`
	Target       string      `json:"target"`
}

type BasicTaskProfile struct {
	JobBaseInfo       JobBaseInfo         `json:"job_base_info"`
	DtleNodeInfos     []DtleNodeInfo      `json:"dtle_node_infos"`
	ConnectionInfo    ConnectionInfo      `json:"connection_info"`
	Configuration     Configuration       `json:"configuration"`
	ReplicateDoDb     []*DataSourceConfig `json:"replicate_do_db"`
	ReplicateIgnoreDb []*DataSourceConfig `json:"replicate_ignore_db"`
}

type MysqlToMysqlJobDetailRespV2 struct {
	BasicTaskProfile BasicTaskProfile `json:"basic_task_profile"`
	TaskLogs         []TaskLog        `json:"task_logs"`
	BaseResp
}

type MysqlDestTaskDetail struct {
	Allocations []AllocationDetail `json:"allocations"`
	TaskConfig  DestTaskConfig     `json:"task_config"`
}

type MysqlSrcTaskDetail struct {
	Allocations []AllocationDetail `json:"allocations"`
	TaskConfig  SrcTaskConfig      `json:"task_config"`
}

type AllocationDetail struct {
	NodeId        string     `json:"node_id"`
	AllocationId  string     `json:"allocation_id"`
	TaskStatus    TaskStatus `json:"task_status"`
	DesiredStatus string     `json:"desired_status"`
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

type SrcTaskConfig struct {
	TaskName            string              `json:"task_name" validate:"required"`
	NodeId              string              `json:"node_id,omitempty"`
	DatabaseType        string              `json:"database_type"`
	ReplicateDoDb       []*DataSourceConfig `json:"replicate_do_db"`
	ReplicateIgnoreDb   []*DataSourceConfig `json:"replicate_ignore_db"`
	SkipCreateDbTable   bool                `json:"skip_create_db_table"`
	DropTableIfExists   bool                `json:"drop_table_if_exists"`
	MysqlSrcTaskConfig  MysqlSrcTaskConfig  `json:"mysql_src_task_config"`
	OracleSrcTaskConfig OracleSrcTaskConfig `json:"oracle_src_task_config"`
	GroupMaxSize        int                 `json:"group_max_size"`
	GroupTimeout        int                 `json:"group_timeout"`
	ReplChanBufferSize  int64               `json:"repl_chan_buffer_size"`
	ChunkSize           int64               `json:"chunk_size"`

	// todo 代码梳理后删除以下字段
	Gtid                  string                    `json:"gtid"`
	MysqlConnectionConfig *DatabaseConnectionConfig `json:"mysql_connection_config" validate:"required"`
	BinlogRelay           bool                      `json:"binlog_relay"`
	WaitOnJob             string                    `json:"wait_on_job"`
	AutoGtid              bool                      `json:"auto_gtid"`
}

type MysqlSrcTaskConfig struct {
	Gtid                  string                    `json:"gtid"`
	MysqlConnectionConfig *DatabaseConnectionConfig `json:"mysql_connection_config"`
	BinlogRelay           bool                      `json:"binlog_relay"`
	WaitOnJob             string                    `json:"wait_on_job"`
	AutoGtid              bool                      `json:"auto_gtid"`
}

type OracleSrcTaskConfig struct {
	Scn                    int                       `json:"scn"`
	OracleConnectionConfig *DatabaseConnectionConfig `json:"oracle_connection_config"`
}

type DestTaskConfig struct {
	TaskName            string              `json:"task_name" validate:"required"`
	NodeId              string              `json:"node_id,omitempty"`
	DatabaseType        string              `json:"database_type"`
	MysqlDestTaskConfig MysqlDestTaskConfig `json:"mysql_dest_task_config"`

	// todo 代码梳理后删除以下字段
	MysqlConnectionConfig *DatabaseConnectionConfig `json:"mysql_connection_config" validate:"required"`
	ParallelWorkers       int                       `json:"parallel_workers"`
	UseMySQLDependency    bool                      `json:"use_my_sql_dependency"`
	DependencyHistorySize int                       `json:"dependency_history_size"`
}

type MysqlDestTaskConfig struct {
	ParallelWorkers       int  `json:"parallel_workers"`
	UseMySQLDependency    bool `json:"use_my_sql_dependency"`
	DependencyHistorySize int  `json:"dependency_history_size"`
}

type DataSourceConfig struct {
	TableSchema       string         `json:"table_schema"`
	TableSchemaRegex  string         `json:"table_schema_regex"`
	TableSchemaRename string         `json:"table_schema_rename"`
	Tables            []*TableConfig `json:"tables"`
}

type TableConfig struct {
	TableName     string   `json:"table_name"`
	TableRegex    string   `json:"table_regex"`
	TableRename   string   `json:"table_rename"`
	ColumnMapFrom []string `json:"column_map_from"`
	Where         string   `json:"where"`
}
type DatabaseConnectionConfig struct {
	Host         string `json:"host"`
	Port         uint32 `json:"port"`
	User         string `json:"user"`
	Password     string `json:"password"`
	ServiceName  string `json:"service_name"`
	DatabaseType string `json:"database_type"`
}

type CreateOrUpdateMysqlToMysqlJobParamV2 struct {
	JobId               string          `json:"job_id" validate:"required"`
	TaskStepName        string          `json:"task_step_name"`
	Reverse             bool            `json:"reverse"`
	Failover            *bool           `json:"failover" example:"true"`
	IsPasswordEncrypted bool            `json:"is_password_encrypted"`
	SrcTask             *SrcTaskConfig  `json:"src_task" validate:"required"`
	DestTask            *DestTaskConfig `json:"dest_task" validate:"required"`
	Retry               int             `json:"retry"`
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
	SrcTask                  *SrcTaskConfig       `json:"src_task" validate:"required"`
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
