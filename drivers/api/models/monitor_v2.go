package models

type GetTaskProgressReqV2 struct {
	AllocationId   string `query:"allocation_id" validate:"required"`
	TaskName       string `query:"task_name" validate:"required"`
	FilterTaskName string `query:"filter_task_name"`
}

type GetTaskProgressRespV2 struct {
	TaskStatus *TaskProgress `json:"tasks_status"`
	BaseResp
}

type TaskProgress struct {
	CurrentCoordinates *CurrentCoordinates    `json:"current_coordinates"`
	DelayCount         *DelayCount            `json:"delay_count"`
	ProgressPct        string                 `json:"progress_PCT"`
	ExecMasterRowCount int64                  `json:"exec_master_row_count"`
	ExecMasterTxCount  int64                  `json:"exec_master_tx_count"`
	ReadMasterRowCount int64                  `json:"read_master_row_count"`
	ReadMasterTxCount  int64                  `json:"read_master_tx_count"`
	ETA                string                 `json:"ETA"`
	Backlog            string                 `json:"backlog"`
	ThroughputStat     *ThroughputStat        `json:"throughput_status"`
	NatsMsgStat        *NatsMessageStatistics `json:"nats_message_status"`
	BufferStat         *BufferStat            `json:"buffer_status"`
	Stage              string                 `json:"stage"`
	Timestamp          int64                  `json:"timestamp"`
}

type CurrentCoordinates struct {
	File               string `json:"file"`
	Position           int64  `json:"position"`
	GtidSet            string `json:"gtid_set"`
	RelayMasterLogFile string `json:"relay_master_log_file"`
	ReadMasterLogPos   int64  `json:"read_master_log_pos"`
	RetrievedGtidSet   string `json:"retrieved_gtid_set"`
}

type DelayCount struct {
	Num  uint64 `json:"num"`
	Time int64  `json:"time"`
}
type ThroughputStat struct {
	Num  uint64 `json:"num"`
	Time uint64 `json:"time"`
}
type BufferStat struct {
	BinlogEventQueueSize int `json:"binlog_event_queue_size"`
	ExtractorTxQueueSize int `json:"extractor_tx_queue_size"`
	ApplierTxQueueSize   int `json:"applier_tx_queue_size"`
	SendByTimeout        int `json:"send_by_timeout"`
	SendBySizeFull       int `json:"send_by_size_full"`
}

type NatsMessageStatistics struct {
	InMsgs     uint64 `json:"in_messages"`
	OutMsgs    uint64 `json:"out_messages"`
	InBytes    uint64 `json:"in_bytes"`
	OutBytes   uint64 `json:"out_bytes"`
	Reconnects uint64 `json:"reconnects"`
}
