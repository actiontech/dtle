package models

type ValidateJobReqV2 struct {
	JobId               string          `json:"job_id" validate:"required"`
	IsPasswordEncrypted bool            `json:"is_password_encrypted"`
	SrcTaskConfig       *SrcTaskConfig  `json:"src_task" validate:"required"`
	DestTaskConfig      *DestTaskConfig `json:"dest_task" validate:"required"`
}

type ValidateJobRespV2 struct {
	// DriverConfigValidated indicates whether the agent validated the driver
	DriverConfigValidated bool `json:"driver_config_validated"`
	// config
	MysqlValidationTasks []*MysqlTaskValidationReport `json:"mysql_task_validation_report"`
	JobValidationError   string                       `json:"job_validation_error"`
	JobValidationWarning string                       `json:"job_validation_warning"`
	BaseResp
}

type MysqlTaskValidationReport struct {
	TaskName             string                `json:"task_name"`
	ConnectionValidation *ConnectionValidation `json:"connection_validation"`
	PrivilegesValidation *PrivilegesValidation `json:"privileges_validation"`
	GtidModeValidation   *GtidModeValidation   `json:"gtid_mode_validation"`
	ServerIdValidation   *ServerIDValidation   `json:"server_id_validation"`
	BinlogValidation     *BinlogValidation     `json:"binlog_validation"`
}

type BinlogValidation struct {
	Validated bool `json:"validated"`
	// Error is a string version of any error that may have occured
	Error string `json:"error"`
}

type GtidModeValidation struct {
	Validated bool `json:"validated"`
	// Error is a string version of any error that may have occured
	Error string `json:"error"`
}

type ServerIDValidation struct {
	Validated bool `json:"validated"`
	// Error is a string version of any error that may have occured
	Error string `json:"error"`
}

type PrivilegesValidation struct {
	Validated bool `json:"validated"`
	// Error is a string version of any error that may have occured
	Error string `json:"error"`
}

type ConnectionValidation struct {
	Validated bool `json:"validated"`
	// Error is a string version of any error that may have occured
	Error string `json:"error"`
}
