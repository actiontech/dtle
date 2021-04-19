package v2

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/actiontech/dtle/drivers/api/models"

	"github.com/actiontech/dtle/drivers/api/handler"
	nomadApi "github.com/hashicorp/nomad/api"
	"github.com/labstack/echo/v4"
)

// @Description get job list.
// @Tags job
// @Success 200 {object} models.JobListRespV2
// @Param filter_job_type query string false "filter job type"
// @Router /v2/jobs/migration [get]
func JobListV2(c echo.Context) error {
	reqParam := new(models.JobListReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	jobs := []models.JobListItemV2{}
	url := handler.BuildUrl("/v1/jobs")
	resp, err := http.Get(url)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke /v1/jobs of nomad failed, error: %v", err)))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("read response body failed, error: %v", err)))
	}

	nomadJobs := []nomadApi.JobListStub{}
	if err := json.Unmarshal(body, &nomadJobs); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("unmarshel response body failed, error: %v", err)))
	}

	for _, nomadJob := range nomadJobs {
		jobType := getJobTypeFromJobId(nomadJob.ID)
		if nil != reqParam.FilterJobType && *reqParam.FilterJobType != string(jobType) {
			continue
		}
		jobs = append(jobs, models.JobListItemV2{
			JobId:                nomadJob.ID,
			JobName:              nomadJob.Name,
			JobStatus:            nomadJob.Status,
			JobStatusDescription: nomadJob.StatusDescription,
		})
	}
	return c.JSON(http.StatusOK, &models.JobListRespV2{
		Jobs:     jobs,
		BaseResp: models.BuildBaseResp(nil),
	})
}

type DtleJobType string

const (
	DtleJobTypeMigration = DtleJobType("migration")
	DtleJobTypeSync      = DtleJobType("sync")
	DtleJobTypeKafka     = DtleJobType("kafka")
	DtleJobTypeUnknown   = DtleJobType("unknown")
)

func addJobTypeToJobId(raw string, jobType DtleJobType) string {
	return fmt.Sprintf(`%v-%v`, raw, jobType)
}

func getJobTypeFromJobId(jobId string) DtleJobType {
	segs := strings.Split(jobId, "-")
	if len(segs) < 2 {
		return DtleJobTypeUnknown
	}

	jobType := DtleJobType(segs[len(segs)-1])
	switch jobType {
	case DtleJobTypeMigration, DtleJobTypeSync, DtleJobTypeKafka:
		return jobType
	default:
		return DtleJobTypeUnknown
	}
}