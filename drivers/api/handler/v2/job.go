package v2

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/actiontech/dtle/drivers/mysql/common"

	"github.com/actiontech/dtle/g"

	"github.com/actiontech/dtle/drivers/api/models"

	"github.com/actiontech/dtle/drivers/api/handler"
	nomadApi "github.com/hashicorp/nomad/api"
	"github.com/labstack/echo/v4"
)

// @Description get job list.
// @Tags job
// @Success 200 {object} models.JobListRespV2
// @Param filter_job_type query string false "filter job type"
// @Router /v2/jobs [get]
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

// @Description get job detail.
// @Tags job
// @Success 200 {object} models.JobDetailRespV2
// @Param job_id query string true "job id"
// @Router /v2/job/detail [get]
func GetJobDetailV2(c echo.Context) error {
	reqParam := new(models.JobDetailReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}

	jobId := reqParam.JobId
	if jobId == "" {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("job_id is required")))
	}
	url := handler.BuildUrl(fmt.Sprintf("/v1/job/%v", jobId))
	nomadJob := nomadApi.Job{}
	if err := handler.InvokeNomadGetApi(url, &nomadJob); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke nomad api %v failed: %v", url, err)))
	}

	url = handler.BuildUrl(fmt.Sprintf("/v1/job/%v/allocations",*nomadJob.ID))
	allocations := []nomadApi.Allocation{}
	if err := handler.InvokeNomadGetApi(url, &allocations); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke nomad api %v failed: %v", url, err)))
	}

	destTaskDetail, srcTaskDetail, err := buildMysqlToMysqlJobDetailResp(nomadJob, allocations)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("build job detail response failed: %v", err)))
	}

	return c.JSON(http.StatusOK, &models.JobDetailRespV2{
		JobId:          jobId,
		JobName:        *nomadJob.Name,
		SrcTaskDetail:  srcTaskDetail,
		DestTaskDetail: destTaskDetail,
		BaseResp:       models.BuildBaseResp(nil),
	})
}

func buildMysqlToMysqlJobDetailResp(nomadJob nomadApi.Job, nomadAllocations []nomadApi.Allocation) (destTaskDetail models.MysqlDestTaskDetail, srcTaskDetail models.MysqlSrcTaskDetail, err error) {
	convertTaskConfigMapToInternalTaskConfig := func(m map[string]interface{}) (internalConfig common.DtleTaskConfig, err error) {
		bs, err := json.Marshal(m)
		if nil != err {
			return common.DtleTaskConfig{}, fmt.Errorf("marshal config map failed: %v", err)
		}
		if err = json.Unmarshal(bs, &internalConfig); nil != err {
			return common.DtleTaskConfig{}, fmt.Errorf("unmarshal config map failed: %v", err)
		}
		return internalConfig, nil
	}
	convertInternalMysqlDataSourceToApi := func(internalDataSource []*common.DataSource) []models.MysqlDataSourceConfig {
		apiMysqlDataSource := []models.MysqlDataSourceConfig{}
		for _, db := range internalDataSource {
			tables := []models.MysqlTableConfig{}
			for _, tb := range db.Tables {
				tables = append(tables, models.MysqlTableConfig{
					TableName:     tb.TableName,
					TableRename:   &tb.TableRename,
					ColumnMapFrom: tb.ColumnMapFrom,
					Where:         &tb.Where,
				})
			}
			apiMysqlDataSource = append(apiMysqlDataSource, models.MysqlDataSourceConfig{
				TableSchema:       db.TableSchema,
				TableSchemaRename: &db.TableSchemaRename,
				Tables:            tables,
			})
		}
		return apiMysqlDataSource
	}

	getTaskStatusFromAllocInfo := func(nomadAllocation nomadApi.Allocation, apiTaskStatus *models.TaskStatus, nomadTask *nomadApi.Task) {
		if nomadTaskState, ok := nomadAllocation.TaskStates[nomadTask.Name]; ok {
			for _, e := range nomadTaskState.Events {
				if nil != err {
					g.Logger.Debug("buildMysqlToMysqlJobDetailResp()", "parse time failed. err", err)
				}
				apiTaskStatus.TaskEvents = append(apiTaskStatus.TaskEvents, models.TaskEvent{
					EventType:  e.Type,
					SetupError: e.SetupError,
					Message:    e.Message,
					Time:       time.Unix(0, e.Time).In(time.Local).Format(time.RFC3339),
				})
			}
			apiTaskStatus.Status = nomadTaskState.State
			apiTaskStatus.StartedAt = nomadTaskState.StartedAt.In(time.Local)
			apiTaskStatus.FinishedAt = nomadTaskState.FinishedAt.In(time.Local)
		}
	}

	taskGroupToNomadAlloc := make(map[string]nomadApi.Allocation)
	for _, a := range nomadAllocations {
		taskGroupToNomadAlloc[a.TaskGroup] = a
	}

	for _, tg := range nomadJob.TaskGroups {
		for _, t := range tg.Tasks {
			internalTaskConfig, err := convertTaskConfigMapToInternalTaskConfig(t.Config)
			if nil != err {
				return models.MysqlDestTaskDetail{}, models.MysqlSrcTaskDetail{}, fmt.Errorf("convert task config failed: %v", err)
			}

			nodeId := ""
			alloc, allocExisted := taskGroupToNomadAlloc[*tg.Name]
			if allocExisted {
				nodeId = alloc.NodeID
			}

			replicateDoDb := convertInternalMysqlDataSourceToApi(internalTaskConfig.ReplicateDoDb)
			replicateIgnoreDb := convertInternalMysqlDataSourceToApi(internalTaskConfig.ReplicateIgnoreDb)
			taskType := common.TaskTypeFromString(t.Name)
			switch taskType {
			case common.TaskTypeSrc:
				dtleTaskConfigForApi := models.MysqlSrcTaskConfig{
					TaskName:           t.Name,
					NodeId:             nodeId,
					Gtid:               &internalTaskConfig.Gtid,
					GroupMaxSize:       &internalTaskConfig.GroupMaxSize,
					ChunkSize:          &internalTaskConfig.ChunkSize,
					DropTableIfExists:  &internalTaskConfig.DropTableIfExists,
					SkipCreateDbTable:  &internalTaskConfig.SkipCreateDbTable,
					ReplChanBufferSize: &internalTaskConfig.ReplChanBufferSize,
					ReplicateDoDb:      replicateDoDb,
					ReplicateIgnoreDb:  replicateIgnoreDb,
					MysqlConnectionConfig: models.MysqlConnectionConfig{
						MysqlHost:     internalTaskConfig.ConnectionConfig.Host,
						MysqlPort:     internalTaskConfig.ConnectionConfig.Port,
						MysqlUser:     internalTaskConfig.ConnectionConfig.User,
						MysqlPassword: "*",
					},
				}
				srcTaskDetail.TaskConfig = dtleTaskConfigForApi
				if allocExisted {
					getTaskStatusFromAllocInfo(alloc, &srcTaskDetail.TaskStatus, t)
				}
				break
			case common.TaskTypeDest:
				dtleTaskConfigForApi := models.MysqlDestTaskConfig{
					TaskName:        t.Name,
					NodeId:          nodeId,
					ParallelWorkers: &internalTaskConfig.ParallelWorkers,
					MysqlConnectionConfig: models.MysqlConnectionConfig{
						MysqlHost:     internalTaskConfig.ConnectionConfig.Host,
						MysqlPort:     internalTaskConfig.ConnectionConfig.Port,
						MysqlUser:     internalTaskConfig.ConnectionConfig.User,
						MysqlPassword: "*",
					},
				}
				destTaskDetail.TaskConfig = dtleTaskConfigForApi
				if allocExisted {
					getTaskStatusFromAllocInfo(alloc, &destTaskDetail.TaskStatus, t)
				}
				break
			case common.TaskTypeUnknown:
				continue
			}
		}
	}

	return destTaskDetail, srcTaskDetail, nil
}