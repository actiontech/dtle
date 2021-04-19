package models

type JobListReqV2 struct {
	FilterJobType *string `query:"filter_job_type"`
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
