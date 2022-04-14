package models

type UpdataLogLevelReqV2 struct {
	DtleLogLevel string `form:"dtle_log_level" validate:"required"`
}

type UpdataLogLevelRespV2 struct {
	DtleLogLevel string `json:"dtle_log_level"`
	BaseResp
}
