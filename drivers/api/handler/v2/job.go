package v2

import (
	"bytes"
	"encoding/json"
	"errors"
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
		if "" != reqParam.FilterJobType && reqParam.FilterJobType != string(jobType) {
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

// @Description create or update migration job.
// @Tags job
// @Accept application/json
// @Param migration_job_config body models.CreateOrUpdateMysqlToMysqlJobParamV2 true "migration job config"
// @Success 200 {object} models.CreateOrUpdateMysqlToMysqlJobRespV2
// @Router /v2/job/migration [post]
func CreateOrUpdateMigrationJobV2(c echo.Context) error {
	jobParam := new(models.CreateOrUpdateMysqlToMysqlJobParamV2)
	if err := c.Bind(jobParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}

	realPwd, err := handler.DecryptMysqlPassword(jobParam.SrcTask.MysqlConnectionConfig.MysqlPassword)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("decrypt src mysql password failed: %v", err)))
	}
	jobParam.SrcTask.MysqlConnectionConfig.MysqlPassword = realPwd

	realPwd, err = handler.DecryptMysqlPassword(jobParam.DestTask.MysqlConnectionConfig.MysqlPassword)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("decrypt dest mysql password failed: %v", err)))
	}
	jobParam.DestTask.MysqlConnectionConfig.MysqlPassword = realPwd

	jobId := g.StringElse(jobParam.JobId, jobParam.JobName)
	nomadJob, err := convertMysqlToMysqlJobToNomadJob(jobParam.Failover, jobId, jobParam.JobName, jobParam.SrcTask, jobParam.DestTask)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("convert job param to nomad job request failed, error: %v", err)))
	}

	nomadJobreq := nomadApi.JobRegisterRequest{
		Job: nomadJob,
	}
	nomadJobReqByte, err := json.Marshal(nomadJobreq)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("marshal nomad job request failed, error: %v", err)))
	}

	url := handler.BuildUrl("/v1/jobs")
	resp, err := http.Post(url, "application/x-www-form-urlencoded", bytes.NewReader(nomadJobReqByte))
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke /v1/jobs of nomad failed, error: %v", err)))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("reading forwarded resp faile: %v", err)))
	}

	nomadResp := nomadApi.JobRegisterResponse{}
	if err := json.Unmarshal(body, &nomadResp); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("forwarded faile: %s", string(body))))
	}

	return c.JSON(http.StatusOK, &models.CreateOrUpdateMysqlToMysqlJobRespV2{
		CreateOrUpdateMysqlToMysqlJobParamV2: *jobParam,
		EvalCreateIndex:                      nomadResp.EvalCreateIndex,
		JobModifyIndex:                       nomadResp.JobModifyIndex,
		BaseResp:                             models.BuildBaseResp(errors.New(nomadResp.Warnings)),
	})
}

func convertMysqlToMysqlJobToNomadJob(failover bool, apiJobId, apiJobName string, apiSrcTask *models.MysqlSrcTaskConfig, apiDestTask *models.MysqlDestTaskConfig) (*nomadApi.Job, error) {
	buildTaskGroupItem := func(dtleTaskconfig map[string]interface{}, taskName, nodeId string) (*nomadApi.TaskGroup, error) {
		task := nomadApi.NewTask(taskName, g.PluginName)
		task.Config = dtleTaskconfig
		if nodeId != "" {
			if failover {
				// https://www.nomadproject.io/docs/runtime/interpolation
				newAff := nomadApi.NewAffinity("${node.unique.id}", "=", nodeId, 100)
				task.Affinities = append(task.Affinities, newAff)
			} else {
				// https://www.nomadproject.io/docs/runtime/interpolation
				newConstraint := nomadApi.NewConstraint("${node.unique.id}", "=", nodeId)
				task.Constraints = append(task.Constraints, newConstraint)
			}
		}

		taskGroup := nomadApi.NewTaskGroup(taskName, 1)
		taskGroup.Tasks = append(taskGroup.Tasks, task)
		return taskGroup, nil
	}

	srcTask, err := buildTaskGroupItem(buildMysqlSrcTaskConfigMap(apiSrcTask), apiSrcTask.TaskName, apiSrcTask.NodeId)
	if nil != err {
		return nil, fmt.Errorf("build src task failed: %v", err)
	}

	destTask, err := buildTaskGroupItem(buildMysqlDestTaskConfigMap(apiDestTask), apiDestTask.TaskName, apiDestTask.NodeId)
	if nil != err {
		return nil, fmt.Errorf("build dest task failed: %v", err)
	}

	jobId := addJobTypeToJobId(apiJobId, DtleJobTypeMigration)
	return &nomadApi.Job{
		ID:          &jobId,
		Name:        &apiJobName,
		Datacenters: []string{"dc1"},
		TaskGroups:  []*nomadApi.TaskGroup{srcTask, destTask},
	}, nil
}

func buildMysqlDestTaskConfigMap(config *models.MysqlDestTaskConfig) map[string]interface{} {
	taskConfigInNomadFormat := make(map[string]interface{})

	addNotRequiredParamToMap(taskConfigInNomadFormat, config.ParallelWorkers, "ParallelWorkers")
	taskConfigInNomadFormat["ConnectionConfig"] = buildMysqlConnectionConfigMap(config.MysqlConnectionConfig)

	return taskConfigInNomadFormat
}

func buildMysqlSrcTaskConfigMap(config *models.MysqlSrcTaskConfig) map[string]interface{} {
	taskConfigInNomadFormat := make(map[string]interface{})

	addNotRequiredParamToMap(taskConfigInNomadFormat, config.DropTableIfExists, "DropTableIfExists")
	addNotRequiredParamToMap(taskConfigInNomadFormat, config.ReplChanBufferSize, "ReplChanBufferSize")
	addNotRequiredParamToMap(taskConfigInNomadFormat, config.ChunkSize, "ChunkSize")
	addNotRequiredParamToMap(taskConfigInNomadFormat, config.GroupMaxSize, "GroupMaxSize")
	addNotRequiredParamToMap(taskConfigInNomadFormat, config.Gtid, "Gtid")
	addNotRequiredParamToMap(taskConfigInNomadFormat, config.SkipCreateDbTable, "SkipCreateDbTable")

	taskConfigInNomadFormat["ConnectionConfig"] = buildMysqlConnectionConfigMap(config.MysqlConnectionConfig)
	taskConfigInNomadFormat["ReplicateDoDb"] = buildMysqlDataSourceConfigMap(config.ReplicateDoDb)
	taskConfigInNomadFormat["ReplicateIgnoreDb"] = buildMysqlDataSourceConfigMap(config.ReplicateIgnoreDb)

	return taskConfigInNomadFormat
}

func buildMysqlDataSourceConfigMap(configs []*models.MysqlDataSourceConfig) []map[string]interface{} {
	res := []map[string]interface{}{}

	for _, c := range configs {
		configMap := make(map[string]interface{})
		configMap["TableSchema"] = c.TableSchema
		addNotRequiredParamToMap(configMap, c.TableSchemaRename, "TableSchemaRename")
		configMap["Tables"] = buildMysqlTableConfigMap(c.Tables)

		res = append(res, configMap)
	}
	return res
}

func buildMysqlTableConfigMap(configs []*models.MysqlTableConfig) []map[string]interface{} {
	res := []map[string]interface{}{}

	for _, c := range configs {
		configMap := make(map[string]interface{})
		configMap["TableName"] = c.TableName
		configMap["ColumnMapFrom"] = c.ColumnMapFrom
		addNotRequiredParamToMap(configMap, c.TableRename, "TableRename")
		addNotRequiredParamToMap(configMap, c.Where, "Where")

		res = append(res, configMap)
	}
	return res
}

func buildMysqlConnectionConfigMap(config *models.MysqlConnectionConfig) map[string]interface{} {
	if nil == config {
		return nil
	}
	res := make(map[string]interface{})
	res["Host"] = config.MysqlHost
	res["Port"] = config.MysqlPort
	res["User"] = config.MysqlUser
	res["Password"] = config.MysqlPassword
	return res
}

func addNotRequiredParamToMap(target map[string]interface{}, value interface{}, fieldName string) {
	if nil != value {
		target[fieldName] = value
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

	url = handler.BuildUrl(fmt.Sprintf("/v1/job/%v/allocations", *nomadJob.ID))
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
	convertInternalMysqlDataSourceToApi := func(internalDataSource []*common.DataSource) []*models.MysqlDataSourceConfig {
		apiMysqlDataSource := []*models.MysqlDataSourceConfig{}
		for _, db := range internalDataSource {
			tables := []*models.MysqlTableConfig{}
			for _, tb := range db.Tables {
				tables = append(tables, &models.MysqlTableConfig{
					TableName:     tb.TableName,
					TableRename:   tb.TableRename,
					ColumnMapFrom: tb.ColumnMapFrom,
					Where:         tb.Where,
				})
			}
			apiMysqlDataSource = append(apiMysqlDataSource, &models.MysqlDataSourceConfig{
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
					Gtid:               internalTaskConfig.Gtid,
					GroupMaxSize:       internalTaskConfig.GroupMaxSize,
					ChunkSize:          internalTaskConfig.ChunkSize,
					DropTableIfExists:  internalTaskConfig.DropTableIfExists,
					SkipCreateDbTable:  internalTaskConfig.SkipCreateDbTable,
					ReplChanBufferSize: internalTaskConfig.ReplChanBufferSize,
					ReplicateDoDb:      replicateDoDb,
					ReplicateIgnoreDb:  replicateIgnoreDb,
					MysqlConnectionConfig: &models.MysqlConnectionConfig{
						MysqlHost:     internalTaskConfig.ConnectionConfig.Host,
						MysqlPort:     uint32(internalTaskConfig.ConnectionConfig.Port),
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
					ParallelWorkers: internalTaskConfig.ParallelWorkers,
					MysqlConnectionConfig: &models.MysqlConnectionConfig{
						MysqlHost:     internalTaskConfig.ConnectionConfig.Host,
						MysqlPort:     uint32(internalTaskConfig.ConnectionConfig.Port),
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
