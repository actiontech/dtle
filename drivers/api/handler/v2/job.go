package v2

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/hashicorp/nomad/nomad/structs"

	"github.com/hashicorp/nomad/plugins/drivers"

	"github.com/actiontech/dtle/drivers/mysql/kafka"

	hclog "github.com/hashicorp/go-hclog"

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
	logger := handler.NewLogger().Named("JobListV2")
	logger.Info("validate params")
	reqParam := new(models.JobListReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}

	url := handler.BuildUrl("/v1/jobs")
	logger.Info("invoke nomad api begin", "url", url)
	nomadJobs := []nomadApi.JobListStub{}
	if err := handler.InvokeApiWithKvData(http.MethodGet, url, nil, &nomadJobs); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke nomad api %v failed: %v", url, err)))
	}
	logger.Info("invoke nomad api finished")

	jobs := []models.JobListItemV2{}
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
	DtleJobTypeMigration    = DtleJobType("migration")
	DtleJobTypeSync         = DtleJobType("sync")
	DtleJobTypeSubscription = DtleJobType("subscription")
	DtleJobTypeUnknown      = DtleJobType("unknown")
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
	case DtleJobTypeMigration, DtleJobTypeSync, DtleJobTypeSubscription:
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
	logger := handler.NewLogger().Named("CreateOrUpdateMigrationJobV2")
	return createOrUpdateMysqlToMysqlJob(c, logger, DtleJobTypeMigration)
}

func createOrUpdateMysqlToMysqlJob(c echo.Context, logger hclog.Logger, jobType DtleJobType) error {
	logger.Info("validate params")
	jobParam := new(models.CreateOrUpdateMysqlToMysqlJobParamV2)
	if err := c.Bind(jobParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(jobParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}
	failover := g.PtrToBool(jobParam.Failover, true)

	if jobParam.IsMysqlPasswordEncrypted {
		realPwd, err := handler.DecryptMysqlPassword(jobParam.SrcTask.MysqlConnectionConfig.MysqlPassword, g.RsaPrivateKey)
		if nil != err {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("decrypt src mysql password failed: %v", err)))
		}
		jobParam.SrcTask.MysqlConnectionConfig.MysqlPassword = realPwd

		realPwd, err = handler.DecryptMysqlPassword(jobParam.DestTask.MysqlConnectionConfig.MysqlPassword, g.RsaPrivateKey)
		if nil != err {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("decrypt dest mysql password failed: %v", err)))
		}
		jobParam.DestTask.MysqlConnectionConfig.MysqlPassword = realPwd
	}

	jobId := g.StringElse(jobParam.JobId, jobParam.JobName)
	jobId = addJobTypeToJobId(jobId, jobType)
	nomadJob, err := convertMysqlToMysqlJobToNomadJob(failover, jobId, jobParam.JobName, jobParam.SrcTask, jobParam.DestTask)
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
	logger.Info("invoke nomad api begin", "url", url)
	nomadResp := nomadApi.JobRegisterResponse{}
	if err := handler.InvokePostApiWithJson(url, nomadJobReqByte, &nomadResp); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke nomad api %v failed: %v", url, err)))
	}
	logger.Info("invoke nomad api finished")

	jobParam.SrcTask.MysqlConnectionConfig.MysqlPassword = "*"
	jobParam.DestTask.MysqlConnectionConfig.MysqlPassword = "*"
	jobParam.JobId = jobId

	var respErr error
	if "" != nomadResp.Warnings {
		respErr = errors.New(nomadResp.Warnings)
	}

	return c.JSON(http.StatusOK, &models.CreateOrUpdateMysqlToMysqlJobRespV2{
		CreateOrUpdateMysqlToMysqlJobParamV2: *jobParam,
		EvalCreateIndex:                      nomadResp.EvalCreateIndex,
		JobModifyIndex:                       nomadResp.JobModifyIndex,
		BaseResp:                             models.BuildBaseResp(respErr),
	})
}

func convertMysqlToMysqlJobToNomadJob(failover bool, apiJobId, apiJobName string, apiSrcTask *models.MysqlSrcTaskConfig, apiDestTask *models.MysqlDestTaskConfig) (*nomadApi.Job, error) {
	srcTask, err := buildNomadTaskGroupItem(buildMysqlSrcTaskConfigMap(apiSrcTask), apiSrcTask.TaskName, apiSrcTask.NodeId, failover)
	if nil != err {
		return nil, fmt.Errorf("build src task failed: %v", err)
	}

	destTask, err := buildNomadTaskGroupItem(buildMysqlDestTaskConfigMap(apiDestTask), apiDestTask.TaskName, apiDestTask.NodeId, failover)
	if nil != err {
		return nil, fmt.Errorf("build dest task failed: %v", err)
	}

	jobId := apiJobId
	return &nomadApi.Job{
		ID:          &jobId,
		Name:        &apiJobName,
		Datacenters: []string{"dc1"},
		TaskGroups:  []*nomadApi.TaskGroup{srcTask, destTask},
	}, nil
}

func buildNomadTaskGroupItem(dtleTaskconfig map[string]interface{}, taskName, nodeId string, failover bool) (*nomadApi.TaskGroup, error) {
	task := nomadApi.NewTask(taskName, g.PluginName)
	task.Config = dtleTaskconfig
	if !failover && "" == nodeId {
		return nil, fmt.Errorf("node id should be provided if failover is false. task_name=%v", taskName)
	}
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

// @Description get migration job detail.
// @Tags job
// @Success 200 {object} models.MysqlToMysqlJobDetailRespV2
// @Param job_id query string true "job id"
// @Router /v2/job/migration/detail [get]
func GetMigrationJobDetailV2(c echo.Context) error {
	logger := handler.NewLogger().Named("GetMigrationJobDetailV2")
	return getMysqlToMysqlJobDetail(c, logger, DtleJobTypeMigration)
}

func getMysqlToMysqlJobDetail(c echo.Context, logger hclog.Logger, jobType DtleJobType) error {
	logger.Info("validate params")
	reqParam := new(models.MysqlToMysqlJobDetailReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}

	failover, nomadJob, allocations, err := getJobDetailFromNomad(logger, reqParam.JobId, jobType)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	destTaskDetail, srcTaskDetail, err := buildMysqlToMysqlJobDetailResp(nomadJob, allocations)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("build job detail response failed: %v", err)))
	}

	return c.JSON(http.StatusOK, &models.MysqlToMysqlJobDetailRespV2{
		JobId:          reqParam.JobId,
		JobName:        *nomadJob.Name,
		Failover:       failover,
		SrcTaskDetail:  srcTaskDetail,
		DestTaskDetail: destTaskDetail,
		BaseResp:       models.BuildBaseResp(nil),
	})
}

func getJobDetailFromNomad(logger hclog.Logger, jobId string, jobType DtleJobType) (failover bool, nomadJob nomadApi.Job, nomadAllocations []nomadApi.Allocation, err error) {
	url := handler.BuildUrl(fmt.Sprintf("/v1/job/%v", jobId))
	logger.Info("invoke nomad api begin", "url", url)

	if err := handler.InvokeApiWithKvData(http.MethodGet, url, nil, &nomadJob); nil != err {
		return false, nomadApi.Job{}, nil, fmt.Errorf("invoke nomad api %v failed: %v", url, err)
	}
	logger.Info("invoke nomad api finished")

	if jobType != getJobTypeFromJobId(g.PtrToString(nomadJob.ID, "")) {
		return false, nomadApi.Job{}, nil, fmt.Errorf("this API is for %v job. but got job type=%v by the provided job id", jobType, getJobTypeFromJobId(g.PtrToString(nomadJob.ID, "")))
	}
	url = handler.BuildUrl(fmt.Sprintf("/v1/job/%v/allocations", *nomadJob.ID))
	logger.Info("invoke nomad api begin", "url", url)
	allocations := []nomadApi.Allocation{}
	if err := handler.InvokeApiWithKvData(http.MethodGet, url, nil, &allocations); nil != err {
		return false, nomadApi.Job{}, nil, fmt.Errorf("invoke nomad api %v failed: %v", url, err)
	}
	logger.Info("invoke nomad api finished")

	for _, tg := range nomadJob.TaskGroups {
		for _, t := range tg.Tasks {
			taskType := common.TaskTypeFromString(t.Name)
			if taskType != common.TaskTypeSrc && taskType != common.TaskTypeDest {
				continue
			}
			for _, constraint := range t.Constraints {
				if constraint.LTarget == "${node.unique.id}" && constraint.Operand == "=" {
					// the "failover" was set when created job using api v2
					// all task within the job will have a constraint to specify unique node if "failover" was set as false
					// so we consider the job is set as "failover"=false if there is a task has constraint specifying unique node
					return false, nomadJob, allocations, nil
				}
			}
		}
	}
	return true, nomadJob, allocations, nil
}

func buildMysqlSrcTaskDetail(taskName string, internalTaskConfig common.DtleTaskConfig, allocsFromNomad []nomadApi.Allocation) (srcTaskDetail models.MysqlSrcTaskDetail) {
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

	replicateDoDb := convertInternalMysqlDataSourceToApi(internalTaskConfig.ReplicateDoDb)
	replicateIgnoreDb := convertInternalMysqlDataSourceToApi(internalTaskConfig.ReplicateIgnoreDb)

	srcTaskDetail.TaskConfig = models.MysqlSrcTaskConfig{
		TaskName:           taskName,
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

	allocs := []models.AllocationDetail{}
	for _, a := range allocsFromNomad {
		newAlloc := getTaskDetailStatusFromAllocInfo(a, taskName)
		allocs = append(allocs, newAlloc)
	}
	srcTaskDetail.Allocations = allocs
	return srcTaskDetail
}

func buildMysqlDestTaskDetail(taskName string, internalTaskConfig common.DtleTaskConfig, allocsFromNomad []nomadApi.Allocation) (destTaskDetail models.MysqlDestTaskDetail) {
	destTaskDetail.TaskConfig = models.MysqlDestTaskConfig{
		TaskName:        taskName,
		ParallelWorkers: internalTaskConfig.ParallelWorkers,
		MysqlConnectionConfig: &models.MysqlConnectionConfig{
			MysqlHost:     internalTaskConfig.ConnectionConfig.Host,
			MysqlPort:     uint32(internalTaskConfig.ConnectionConfig.Port),
			MysqlUser:     internalTaskConfig.ConnectionConfig.User,
			MysqlPassword: "*",
		},
	}

	allocs := []models.AllocationDetail{}
	for _, a := range allocsFromNomad {
		newAlloc := getTaskDetailStatusFromAllocInfo(a, taskName)
		allocs = append(allocs, newAlloc)
	}
	destTaskDetail.Allocations = allocs

	return destTaskDetail
}

func getTaskDetailStatusFromAllocInfo(nomadAllocation nomadApi.Allocation, taskName string) models.AllocationDetail {
	newAlloc := models.AllocationDetail{}
	if nomadTaskState, ok := nomadAllocation.TaskStates[taskName]; ok {
		newAlloc.AllocationId = nomadAllocation.ID
		newAlloc.NodeId = nomadAllocation.NodeID
		for _, e := range nomadTaskState.Events {
			newAlloc.TaskStatus.TaskEvents = append(newAlloc.TaskStatus.TaskEvents, models.TaskEvent{
				EventType:  e.Type,
				SetupError: e.SetupError,
				Message:    e.Message,
				Time:       time.Unix(0, e.Time).In(time.Local).Format(time.RFC3339),
			})
		}
		newAlloc.TaskStatus.Status = nomadTaskState.State
		newAlloc.TaskStatus.StartedAt = nomadTaskState.StartedAt.In(time.Local)
		newAlloc.TaskStatus.FinishedAt = nomadTaskState.FinishedAt.In(time.Local)
	}
	return newAlloc
}

func convertTaskConfigMapToInternalTaskConfig(m map[string]interface{}) (internalConfig common.DtleTaskConfig, err error) {
	bs, err := json.Marshal(m)
	if nil != err {
		return common.DtleTaskConfig{}, fmt.Errorf("marshal config map failed: %v", err)
	}
	if err = json.Unmarshal(bs, &internalConfig); nil != err {
		return common.DtleTaskConfig{}, fmt.Errorf("unmarshal config map failed: %v", err)
	}
	return internalConfig, nil
}

func buildMysqlToMysqlJobDetailResp(nomadJob nomadApi.Job, nomadAllocations []nomadApi.Allocation) (destTaskDetail models.MysqlDestTaskDetail, srcTaskDetail models.MysqlSrcTaskDetail, err error) {
	taskGroupToNomadAlloc := make(map[string][]nomadApi.Allocation)
	for _, a := range nomadAllocations {
		taskGroupToNomadAlloc[a.TaskGroup] = append(taskGroupToNomadAlloc[a.TaskGroup], a)
	}

	for _, tg := range nomadJob.TaskGroups {
		for _, t := range tg.Tasks {
			internalTaskConfig, err := convertTaskConfigMapToInternalTaskConfig(t.Config)
			if nil != err {
				return models.MysqlDestTaskDetail{}, models.MysqlSrcTaskDetail{}, fmt.Errorf("convert task config failed: %v", err)
			}

			taskType := common.TaskTypeFromString(t.Name)
			switch taskType {
			case common.TaskTypeSrc:
				srcTaskDetail = buildMysqlSrcTaskDetail(t.Name, internalTaskConfig, taskGroupToNomadAlloc[*tg.Name])
				break
			case common.TaskTypeDest:
				destTaskDetail = buildMysqlDestTaskDetail(t.Name, internalTaskConfig, taskGroupToNomadAlloc[*tg.Name])
				break
			case common.TaskTypeUnknown:
				continue
			}
		}
	}

	return destTaskDetail, srcTaskDetail, nil
}

// @Description create or update sync job.
// @Tags job
// @Accept application/json
// @Param sync_job_config body models.CreateOrUpdateMysqlToMysqlJobParamV2 true "sync job config"
// @Success 200 {object} models.CreateOrUpdateMysqlToMysqlJobRespV2
// @Router /v2/job/sync [post]
func CreateOrUpdateSyncJobV2(c echo.Context) error {
	logger := handler.NewLogger().Named("CreateOrUpdateSyncJobV2")
	return createOrUpdateMysqlToMysqlJob(c, logger, DtleJobTypeSync)
}

// @Description get sync job detail.
// @Tags job
// @Success 200 {object} models.MysqlToMysqlJobDetailRespV2
// @Param job_id query string true "job id"
// @Router /v2/job/sync/detail [get]
func GetSyncJobDetailV2(c echo.Context) error {
	logger := handler.NewLogger().Named("GetSyncJobDetailV2")
	return getMysqlToMysqlJobDetail(c, logger, DtleJobTypeSync)
}

// @Description create or update subscription job.
// @Tags job
// @Accept application/json
// @Param subscription_job_config body models.CreateOrUpdateMysqlToKafkaJobParamV2 true "subscription job config"
// @Success 200 {object} models.CreateOrUpdateMysqlToKafkaJobRespV2
// @Router /v2/job/subscription [post]
func CreateOrUpdateSubscriptionJobV2(c echo.Context) error {
	logger := handler.NewLogger().Named("CreateOrUpdateSubscriptionJobV2")
	return createOrUpdateMysqlToKafkaJob(c, logger, DtleJobTypeSubscription)
}

func createOrUpdateMysqlToKafkaJob(c echo.Context, logger hclog.Logger, jobType DtleJobType) error {
	logger.Info("validate params")
	jobParam := new(models.CreateOrUpdateMysqlToKafkaJobParamV2)
	if err := c.Bind(jobParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(jobParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}
	failover := g.PtrToBool(jobParam.Failover, true)

	if jobParam.IsMysqlPasswordEncrypted {
		realPwd, err := handler.DecryptMysqlPassword(jobParam.SrcTask.MysqlConnectionConfig.MysqlPassword, g.RsaPrivateKey)
		if nil != err {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("decrypt src mysql password failed: %v", err)))
		}
		jobParam.SrcTask.MysqlConnectionConfig.MysqlPassword = realPwd
	}

	jobId := g.StringElse(jobParam.JobId, jobParam.JobName)
	jobId = addJobTypeToJobId(jobId, jobType)
	nomadJob, err := convertMysqlToKafkaJobToNomadJob(failover, jobId, jobParam.JobName, jobParam.SrcTask, jobParam.DestTask)
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
	logger.Info("invoke nomad api begin", "url", url)
	nomadResp := nomadApi.JobRegisterResponse{}
	if err := handler.InvokePostApiWithJson(url, nomadJobReqByte, &nomadResp); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke nomad api %v failed: %v", url, err)))
	}
	logger.Info("invoke nomad api finished")

	jobParam.SrcTask.MysqlConnectionConfig.MysqlPassword = "*"
	jobParam.JobId = jobId

	var respErr error
	if "" != nomadResp.Warnings {
		respErr = errors.New(nomadResp.Warnings)
	}
	return c.JSON(http.StatusOK, &models.CreateOrUpdateMysqlToKafkaJobRespV2{
		CreateOrUpdateMysqlToKafkaJobParamV2: *jobParam,
		EvalCreateIndex:                      nomadResp.EvalCreateIndex,
		JobModifyIndex:                       nomadResp.JobModifyIndex,
		BaseResp:                             models.BuildBaseResp(respErr),
	})
}

func convertMysqlToKafkaJobToNomadJob(failover bool, apiJobId, apiJobName string, apiSrcTask *models.MysqlSrcTaskConfig, apiDestTask *models.KafkaDestTaskConfig) (*nomadApi.Job, error) {
	srcTask, err := buildNomadTaskGroupItem(buildMysqlSrcTaskConfigMap(apiSrcTask), apiSrcTask.TaskName, apiSrcTask.NodeId, failover)
	if nil != err {
		return nil, fmt.Errorf("build src task failed: %v", err)
	}

	destTask, err := buildNomadTaskGroupItem(buildKafkaDestTaskConfigMap(apiDestTask), apiDestTask.TaskName, apiDestTask.NodeId, failover)
	if nil != err {
		return nil, fmt.Errorf("build dest task failed: %v", err)
	}

	jobId := apiJobId
	return &nomadApi.Job{
		ID:          &jobId,
		Name:        &apiJobName,
		Datacenters: []string{"dc1"},
		TaskGroups:  []*nomadApi.TaskGroup{srcTask, destTask},
	}, nil
}

func buildKafkaDestTaskConfigMap(config *models.KafkaDestTaskConfig) map[string]interface{} {
	kafkaConfig := make(map[string]interface{})
	taskConfigInNomadFormat := make(map[string]interface{})

	kafkaConfig["Brokers"] = config.BrokerAddrs
	kafkaConfig["Topic"] = config.Topic
	kafkaConfig["MessageGroupMaxSize"] = config.MessageGroupMaxSize
	kafkaConfig["MessageGroupTimeout"] = config.MessageGroupTimeout
	kafkaConfig["Converter"] = kafka.CONVERTER_JSON
	taskConfigInNomadFormat["KafkaConfig"] = kafkaConfig

	return taskConfigInNomadFormat
}

// @Description get subscription job detail.
// @Tags job
// @Success 200 {object} models.MysqlToKafkaJobDetailRespV2
// @Param job_id query string true "job id"
// @Router /v2/job/subscription/detail [get]
func GetSubscriptionJobDetailV2(c echo.Context) error {
	logger := handler.NewLogger().Named("GetSubscriptionJobDetailV2")
	logger.Info("validate params")
	reqParam := new(models.MysqlToKafkaJobDetailReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}

	failover, nomadJob, allocations, err := getJobDetailFromNomad(logger, reqParam.JobId, DtleJobTypeSubscription)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	destTaskDetail, srcTaskDetail, err := buildMysqlToKafkaJobDetailResp(nomadJob, allocations)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("build job detail response failed: %v", err)))
	}

	return c.JSON(http.StatusOK, &models.MysqlToKafkaJobDetailRespV2{
		JobId:          reqParam.JobId,
		JobName:        *nomadJob.Name,
		Failover:       failover,
		SrcTaskDetail:  srcTaskDetail,
		DestTaskDetail: destTaskDetail,
		BaseResp:       models.BuildBaseResp(nil),
	})
}

func buildMysqlToKafkaJobDetailResp(nomadJob nomadApi.Job, nomadAllocations []nomadApi.Allocation) (destTaskDetail models.KafkaDestTaskDetail, srcTaskDetail models.MysqlSrcTaskDetail, err error) {
	taskGroupToNomadAlloc := make(map[string][]nomadApi.Allocation)
	for _, a := range nomadAllocations {
		taskGroupToNomadAlloc[a.TaskGroup] = append(taskGroupToNomadAlloc[a.TaskGroup], a)
	}

	for _, tg := range nomadJob.TaskGroups {
		for _, t := range tg.Tasks {
			internalTaskConfig, err := convertTaskConfigMapToInternalTaskConfig(t.Config)
			if nil != err {
				return models.KafkaDestTaskDetail{}, models.MysqlSrcTaskDetail{}, fmt.Errorf("convert task config failed: %v", err)
			}

			taskType := common.TaskTypeFromString(t.Name)
			switch taskType {
			case common.TaskTypeSrc:
				srcTaskDetail = buildMysqlSrcTaskDetail(t.Name, internalTaskConfig, taskGroupToNomadAlloc[*tg.Name])
				break
			case common.TaskTypeDest:
				if nil == internalTaskConfig.KafkaConfig {
					return models.KafkaDestTaskDetail{}, models.MysqlSrcTaskDetail{}, fmt.Errorf("can not find kafka task config from nomad")
				}
				destTaskDetail = buildKafkaDestTaskDetail(t.Name, *internalTaskConfig.KafkaConfig, taskGroupToNomadAlloc[*tg.Name])
				break
			case common.TaskTypeUnknown:
				continue
			}
		}
	}

	return destTaskDetail, srcTaskDetail, nil
}

func buildKafkaDestTaskDetail(taskName string, internalTaskKafkaConfig common.KafkaConfig, allocsFromNomad []nomadApi.Allocation) (destTaskDetail models.KafkaDestTaskDetail) {
	destTaskDetail.TaskConfig = models.KafkaDestTaskConfig{
		TaskName:            taskName,
		BrokerAddrs:         internalTaskKafkaConfig.Brokers,
		Topic:               internalTaskKafkaConfig.Topic,
		MessageGroupMaxSize: internalTaskKafkaConfig.MessageGroupMaxSize,
		MessageGroupTimeout: internalTaskKafkaConfig.MessageGroupTimeout,
	}

	for _, a := range allocsFromNomad {
		alloc := getTaskDetailStatusFromAllocInfo(a, taskName)
		destTaskDetail.Allocations = append(destTaskDetail.Allocations, alloc)
	}

	return destTaskDetail
}

// @Description pause job.
// @Tags job
// @accept application/x-www-form-urlencoded
// @Param job_id formData string true "job id"
// @Success 200 {object} models.PauseJobRespV2
// @Router /v2/job/pause [post]
func PauseJobV2(c echo.Context) error {
	logger := handler.NewLogger().Named("PauseJobV2")
	logger.Info("validate params")
	reqParam := new(models.PauseJobReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}

	logger.Info("get allocations of job", "job_id", reqParam.JobId)
	url := handler.BuildUrl("/v1/allocations")
	logger.Info("invoke nomad api begin", "url", url)
	nomadAllocs := []nomadApi.Allocation{}
	if err := handler.InvokeApiWithKvData(http.MethodGet, url, nil, &nomadAllocs); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke nomad api %v failed: %v", url, err)))
	}
	logger.Info("invoke nomad api finished")

	for _, a := range nomadAllocs {
		url = handler.BuildUrl(fmt.Sprintf("/v1/client/allocation/%v/signal", a.ID))
		for taskName, state := range a.TaskStates {
			if state.State == string(drivers.TaskStateRunning) {
				if err := sentSignalToTask(logger, a.ID, taskName, "pause"); nil != err {
					return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("task_name=%v, allocation_id=%v; pause task failed:  %v", taskName, a.ID, err)))
				}
			}
		}
	}

	return c.JSON(http.StatusOK, &models.PauseJobRespV2{
		BaseResp: models.BuildBaseResp(nil),
	})
}

// @Description resume job.
// @Tags job
// @accept application/x-www-form-urlencoded
// @Param job_id formData string true "job id"
// @Success 200 {object} models.ResumeJobRespV2
// @Router /v2/job/resume [post]
func ResumeJobV2(c echo.Context) error {
	logger := handler.NewLogger().Named("ResumeJobV2")
	logger.Info("validate params")
	reqParam := new(models.ResumeJobReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}

	logger.Info("get allocations of job", "job_id", reqParam.JobId)
	url := handler.BuildUrl("/v1/allocations")
	logger.Info("invoke nomad api begin", "url", url)
	nomadAllocs := []nomadApi.Allocation{}
	if err := handler.InvokeApiWithKvData(http.MethodGet, url, nil, &nomadAllocs); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke nomad api %v failed: %v", url, err)))
	}
	logger.Info("invoke nomad api finished")

	for _, a := range nomadAllocs {
		url = handler.BuildUrl(fmt.Sprintf("/v1/client/allocation/%v/signal", a.ID))
		for taskName, state := range a.TaskStates {
			if state.State == string(drivers.TaskStateRunning) { // the status of paused task is 'running'
				if err := sentSignalToTask(logger, a.ID, taskName, "resume"); nil != err {
					return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("task_name=%v, allocation_id=%v; resume task failed:  %v", taskName, a.ID, err)))
				}
			}
		}
	}

	return c.JSON(http.StatusOK, &models.ResumeJobRespV2{
		BaseResp: models.BuildBaseResp(nil),
	})
}

func sentSignalToTask(logger hclog.Logger, allocId, taskName, signal string) error {
	logger.Debug("sentSignalToTask")
	if "" == allocId {
		return fmt.Errorf("allocation id is required")
	}
	param := ""
	if "" == taskName {
		param = fmt.Sprintf(`{"Signal":"%v"}`, signal)
	} else {
		param = fmt.Sprintf(`{"Signal":"%v","Task":"%v"}`, signal, taskName)
	}
	resp := structs.GenericResponse{}
	url := handler.BuildUrl(fmt.Sprintf("/v1/client/allocation/%v/signal", allocId))
	if err := handler.InvokePostApiWithJson(url, []byte(param), &resp); nil != err {
		return fmt.Errorf("invoke nomad api %v failed: %v", url, err)
	}
	return nil
}