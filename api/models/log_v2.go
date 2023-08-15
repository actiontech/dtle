package models

type UpdataLogLevelReqV2 struct {
	DtleLogLevel string  `form:"dtle_log_level" validate:"required"`
	DebugJob     *string `form:"debug_job"`
}

type UpdataLogLevelRespV2 struct {
	DtleLogLevel string `json:"dtle_log_level"`
	DebugJob     string `json:"debug_job"`
	BaseResp
}

type DiagnosisJobReqV2 struct {
	JobId string `query:"job_id" validate:"required"`
}
