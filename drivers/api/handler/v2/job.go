package v2

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/actiontech/dtle/drivers/mysql/mysql"

	"github.com/hashicorp/nomad/nomad/structs"

	"github.com/actiontech/dtle/drivers/mysql/kafka"

	hclog "github.com/hashicorp/go-hclog"

	"github.com/actiontech/dtle/drivers/mysql/common"

	"github.com/actiontech/dtle/g"

	"github.com/actiontech/dtle/drivers/api/models"

	"github.com/actiontech/dtle/drivers/api/handler"
	nomadApi "github.com/hashicorp/nomad/api"
	"github.com/labstack/echo/v4"
)

// @Id JobListV2
// @Description get job list.
// @Tags job
// @Success 200 {object} models.JobListRespV2
// @Security ApiKeyAuth
// @Param filter_job_type query string false "filter job type" Enums(migration,sync,subscription)
// @Param filter_job_id query string false "filter job id"
// @Param filter_job_status query string false "filter job status"
// @Param order_by query string false "order by" default(job_create_time) Enums(job_create_time)
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

	user, err := getCurrentUser(c)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)))
	}
	jobList, err := storeManager.FindJobList()
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v ; get job status list failed: %v", handler.ConsulAddr, err)))
	}
	logger.Info("invoke consul find job list finished")
	nomadJobMap, err := findJobsFromNomad()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("find job err %v", err)))
	}
	logger.Info("invoke nomad find job list finished")
	jobs := make([]common.JobListItemV2, 0)
	for _, consulJob := range jobList {
		jobType := getJobTypeFromJobId(consulJob.JobId)
		if "" != reqParam.FilterJobType && reqParam.FilterJobType != string(jobType) {
			continue
		}
		if !userHasAccess(storeManager, consulJob.User, user) {
			continue
		}
		jobItem := common.JobListItemV2{
			JobId:         consulJob.JobId,
			JobStatus:     consulJob.JobStatus,
			Topic:         consulJob.Topic,
			JobCreateTime: consulJob.JobCreateTime,
			SrcAddrList:   consulJob.SrcAddrList,
			DstAddrList:   consulJob.DstAddrList,
			User:          consulJob.User,
			JobSteps:      consulJob.JobSteps,
		}
		if nomadItem, ok := nomadJobMap[jobItem.JobId]; !ok {
			jobItem.JobStatus = common.DtleJobStatusUndefined
		} else if consulJob.JobStatus == common.DtleJobStatusNonPaused {
			jobItem.JobStatus = nomadItem.Status
		}
		if reqParam.FilterJobStatus != "" && reqParam.FilterJobStatus != jobItem.JobStatus {
			continue
		}
		if reqParam.FilterJobId != "" && !strings.HasPrefix(jobItem.JobId, reqParam.FilterJobId) {
			continue
		}
		jobs = append(jobs, jobItem)
	}

	return c.JSON(http.StatusOK, &models.JobListRespV2{
		Jobs:     jobs,
		BaseResp: models.BuildBaseResp(nil),
	})
}
func findJobsFromNomad() (map[string]nomadApi.JobListStub, error) {
	url := handler.BuildUrl("/v1/jobs")
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	nomadJobs := []nomadApi.JobListStub{}
	if err := json.Unmarshal(body, &nomadJobs); nil != err {
		return nil, err
	}

	nomadJobMap := make(map[string]nomadApi.JobListStub, 0)
	for _, nomadJob := range nomadJobs {
		nomadJobMap[nomadJob.ID] = nomadJob
	}
	return nomadJobMap, nil
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

// @Id CreateOrUpdateMigrationJobV2
// @Description create or update migration job.
// @Tags job
// @Accept application/json
// @Security ApiKeyAuth
// @Param migration_job_config body models.CreateOrUpdateMysqlToMysqlJobParamV2 true "migration job config"
// @Success 200 {object} models.CreateOrUpdateMysqlToMysqlJobRespV2
// @Router /v2/job/migration [post]
func CreateOrUpdateMigrationJobV2(c echo.Context) error {
	logger := handler.NewLogger().Named("CreateOrUpdateMigrationJobV2")
	logger.Info("validate params")
	jobParam := new(models.CreateOrUpdateMysqlToMysqlJobParamV2)
	if err := c.Bind(jobParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(jobParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}

	if err := checkUpdateJobInfo(c, jobParam.JobId); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	user, err := getCurrentUser(c)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	resp, err := createOrUpdateMysqlToMysqlJob(logger, jobParam, user, DtleJobTypeMigration)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	return c.JSON(http.StatusOK, resp)
}

func createOrUpdateMysqlToMysqlJob(logger hclog.Logger, jobParam *models.CreateOrUpdateMysqlToMysqlJobParamV2,
	user *common.User, jobType DtleJobType) (*models.CreateOrUpdateMysqlToMysqlJobRespV2, error) {

	failover := g.PtrToBool(jobParam.Failover, true)
	if jobParam.IsMysqlPasswordEncrypted {
		realPwd, err := handler.DecryptPassword(jobParam.SrcTask.MysqlConnectionConfig.MysqlPassword, g.RsaPrivateKey)
		if nil != err {
			return nil, fmt.Errorf("decrypt src mysql password failed: %v", err)
		}
		jobParam.SrcTask.MysqlConnectionConfig.MysqlPassword = realPwd

		realPwd, err = handler.DecryptPassword(jobParam.DestTask.MysqlConnectionConfig.MysqlPassword, g.RsaPrivateKey)
		if nil != err {
			return nil, fmt.Errorf("decrypt dest mysql password failed: %v", err)
		}
		jobParam.DestTask.MysqlConnectionConfig.MysqlPassword = realPwd
	}

	// set default
	if jobParam.SrcTask.ReplChanBufferSize == 0 {
		jobParam.SrcTask.ReplChanBufferSize = common.DefaultChannelBufferSize
	}
	if jobParam.SrcTask.GroupMaxSize == 0 {
		jobParam.SrcTask.GroupMaxSize = common.DefaultSrcGroupMaxSize
	}
	if jobParam.SrcTask.ChunkSize == 0 {
		jobParam.SrcTask.ChunkSize = common.DefaultChunkSize
	}
	if jobParam.DestTask.ParallelWorkers == 0 {
		jobParam.DestTask.ParallelWorkers = common.DefaultNumWorkers
	}
	if jobParam.SrcTask.GroupTimeout == 0 {
		jobParam.SrcTask.GroupTimeout = common.DefaultSrcGroupTimeout
	}

	jobParam.JobId = addJobTypeToJobId(jobParam.JobId, jobType)
	nomadJob, err := convertMysqlToMysqlJobToNomadJob(failover, jobParam)
	if nil != err {
		return nil, fmt.Errorf("convert job param to nomad job request failed, error: %v", err)
	}

	nomadJobreq := nomadApi.JobRegisterRequest{
		Job: nomadJob,
	}
	nomadJobReqByte, err := json.Marshal(nomadJobreq)
	if nil != err {
		return nil, fmt.Errorf("marshal nomad job request failed, error: %v", err)
	}
	url := handler.BuildUrl("/v1/jobs")
	logger.Info("invoke nomad api begin", "url", url)
	nomadResp := nomadApi.JobRegisterResponse{}
	if err := handler.InvokePostApiWithJson(url, nomadJobReqByte, &nomadResp); nil != err {
		return nil, fmt.Errorf("invoke nomad api %v failed: %v", url, err)
	}
	logger.Info("invoke nomad api finished")

	jobParam.SrcTask.MysqlConnectionConfig.MysqlPassword = "*"
	jobParam.DestTask.MysqlConnectionConfig.MysqlPassword = "*"

	var respErr error
	if "" != nomadResp.Warnings {
		respErr = errors.New(nomadResp.Warnings)
	} else {
		err = buildMySQLJobListItem(logger, jobParam, user)
		if err != nil {
			return nil, err
		}
	}
	return &models.CreateOrUpdateMysqlToMysqlJobRespV2{
		CreateOrUpdateMysqlToMysqlJobParamV2: *jobParam,
		EvalCreateIndex:                      nomadResp.EvalCreateIndex,
		JobModifyIndex:                       nomadResp.JobModifyIndex,
		BaseResp:                             models.BuildBaseResp(respErr),
	}, nil
}

func convertMysqlToMysqlJobToNomadJob(failover bool, jobParams *models.CreateOrUpdateMysqlToMysqlJobParamV2) (*nomadApi.Job, error) {
	srcTask, err := buildNomadTaskGroupItem(buildMysqlSrcTaskConfigMap(jobParams.SrcTask), jobParams.SrcTask.TaskName, jobParams.SrcTask.NodeId, failover, jobParams.Retry)
	if nil != err {
		return nil, fmt.Errorf("build src task failed: %v", err)
	}

	destTask, err := buildNomadTaskGroupItem(buildMysqlDestTaskConfigMap(jobParams.DestTask), jobParams.DestTask.TaskName, jobParams.DestTask.NodeId, failover, jobParams.Retry)
	if nil != err {
		return nil, fmt.Errorf("build dest task failed: %v", err)
	}

	jobId := jobParams.JobId
	return &nomadApi.Job{
		ID:          &jobId,
		Datacenters: []string{"dc1"},
		TaskGroups:  []*nomadApi.TaskGroup{srcTask, destTask},
	}, nil
}

func buildMySQLJobListItem(logger hclog.Logger, jobParam *models.CreateOrUpdateMysqlToMysqlJobParamV2,
	user *common.User) error {
	// add data to consul
	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)
	}
	jobInfo := common.JobListItemV2{
		JobId:         jobParam.JobId,
		JobStatus:     common.DtleJobStatusNonPaused,
		JobCreateTime: time.Now().In(time.Local).Format(time.RFC3339),
		SrcAddrList:   []string{jobParam.SrcTask.MysqlConnectionConfig.MysqlHost},
		DstAddrList:   []string{jobParam.DestTask.MysqlConnectionConfig.MysqlHost},
		User:          fmt.Sprintf("%s:%s", user.Tenant, user.Username),
		JobSteps:      nil,
	}
	if jobParam.Reverse {
		jobInfo.JobStatus = common.DtleJobStatusReverseInit
	}
	if jobParam.TaskStepName == "all" {
		jobInfo.JobSteps = append(jobInfo.JobSteps, common.NewJobStep(mysql.JobFullCopy), common.NewJobStep(mysql.JobIncrCopy))
	} else if jobParam.TaskStepName == mysql.JobFullCopy {
		jobInfo.JobSteps = append(jobInfo.JobSteps, common.NewJobStep(mysql.JobFullCopy))
	} else if jobParam.TaskStepName == mysql.JobIncrCopy {
		jobInfo.JobSteps = append(jobInfo.JobSteps, common.NewJobStep(mysql.JobIncrCopy))
	}
	err = storeManager.SaveJobInfo(jobInfo)
	if nil != err {
		return fmt.Errorf("consul_addr=%v ; sava job info list failed: %v", handler.ConsulAddr, err)
	}
	return nil
}

func buildKafkaJobListItem(logger hclog.Logger, jobParam *models.CreateOrUpdateMysqlToKafkaJobParamV2,
	user *common.User) error {
	// add data to consul
	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)
	}
	jobInfo := common.JobListItemV2{
		JobId:         jobParam.JobId,
		JobStatus:     common.DtleJobStatusNonPaused,
		Topic:         jobParam.DestTask.Topic,
		JobCreateTime: time.Now().In(time.Local).Format(time.RFC3339),
		SrcAddrList:   []string{jobParam.SrcTask.MysqlConnectionConfig.MysqlHost},
		DstAddrList:   jobParam.DestTask.BrokerAddrs,
		User:          fmt.Sprintf("%s:%s", user.Tenant, user.Username),
		JobSteps:      nil,
	}
	if jobParam.TaskStepName == "all" {
		jobInfo.JobSteps = append(jobInfo.JobSteps, common.NewJobStep(mysql.JobFullCopy), common.NewJobStep(mysql.JobIncrCopy))
	} else if jobParam.TaskStepName == mysql.JobFullCopy {
		jobInfo.JobSteps = append(jobInfo.JobSteps, common.NewJobStep(mysql.JobFullCopy))
	} else if jobParam.TaskStepName == mysql.JobIncrCopy {
		jobInfo.JobSteps = append(jobInfo.JobSteps, common.NewJobStep(mysql.JobIncrCopy))
	}
	err = storeManager.SaveJobInfo(jobInfo)
	if nil != err {
		return fmt.Errorf("consul_addr=%v ; sava job info list failed: %v", handler.ConsulAddr, err)
	}
	return nil
}

func buildNomadTaskGroupItem(dtleTaskconfig map[string]interface{}, taskName, nodeId string, failover bool, retryTimes int) (*nomadApi.TaskGroup, error) {
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
	reschedulePolicy, restartPolicy := buildRestartPolicy(retryTimes)
	taskGroup.ReschedulePolicy = reschedulePolicy
	taskGroup.RestartPolicy = restartPolicy
	taskGroup.Tasks = append(taskGroup.Tasks, task)
	return taskGroup, nil
}

func buildRestartPolicy(RestartAttempts int) (*nomadApi.ReschedulePolicy, *nomadApi.RestartPolicy) {
	// set default ReschedulePolicy and default RestartPolicy interval
	// https://github.com/actiontech/dtle-docs-cn/blob/master/4/4.3_job_configuration.md#restart--reschedule
	defaultRescheduleAttempts := 1
	defaultRescheduleInterval := time.Duration(1800000000000)
	defaultRescheduleUnlimited := false

	defaultRestartInterval := time.Duration(1800000000000)
	defaultRestartMode := "fail"

	return &nomadApi.ReschedulePolicy{
			Attempts:  &defaultRescheduleAttempts,
			Interval:  &defaultRescheduleInterval,
			Unlimited: &defaultRescheduleUnlimited,
		}, &nomadApi.RestartPolicy{
			Interval: &defaultRestartInterval,
			Attempts: &RestartAttempts,
			Mode:     &defaultRestartMode,
		}
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
	addNotRequiredParamToMap(taskConfigInNomadFormat, config.GroupTimeout, "GroupTimeout")
	addNotRequiredParamToMap(taskConfigInNomadFormat, config.Gtid, "Gtid")
	addNotRequiredParamToMap(taskConfigInNomadFormat, config.SkipCreateDbTable, "SkipCreateDbTable")
	addNotRequiredParamToMap(taskConfigInNomadFormat, config.BinlogRelay, "BinlogRelay")
	addNotRequiredParamToMap(taskConfigInNomadFormat, config.WaitOnJob, "WaitOnJob")
	addNotRequiredParamToMap(taskConfigInNomadFormat, config.AutoGtid, "AutoGtid")

	taskConfigInNomadFormat["ConnectionConfig"] = buildMysqlConnectionConfigMap(config.MysqlConnectionConfig)
	taskConfigInNomadFormat["ReplicateDoDb"] = buildMysqlDataSourceConfigMap(config.ReplicateDoDb)
	taskConfigInNomadFormat["ReplicateIgnoreDb"] = buildMysqlDataSourceConfigMap(config.ReplicateIgnoreDb)

	return taskConfigInNomadFormat
}

func buildMysqlDataSourceConfigMap(configs []*models.MysqlDataSourceConfig) []map[string]interface{} {
	res := []map[string]interface{}{}

	for _, c := range configs {
		configMap := make(map[string]interface{})
		addNotRequiredParamToMap(configMap, c.TableSchema, "TableSchema")
		addNotRequiredParamToMap(configMap, c.TableSchemaRename, "TableSchemaRename")
		addNotRequiredParamToMap(configMap, c.TableSchemaRegex, "TableSchemaRegex")
		configMap["Tables"] = buildMysqlTableConfigMap(c.Tables)

		res = append(res, configMap)
	}
	return res
}

func buildMysqlTableConfigMap(configs []*models.MysqlTableConfig) []map[string]interface{} {
	res := []map[string]interface{}{}

	for _, c := range configs {
		configMap := make(map[string]interface{})
		if len(c.ColumnMapFrom) != 0 {
			configMap["ColumnMapFrom"] = c.ColumnMapFrom
		}
		addNotRequiredParamToMap(configMap, c.TableName, "TableName")
		addNotRequiredParamToMap(configMap, c.TableRegex, "TableRegex")
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
	if handler.IsEmpty(value) {
		return
	}
	target[fieldName] = value
}

// @Id GetMigrationJobDetailV2
// @Description get migration job detail.
// @Tags job
// @Success 200 {object} models.MysqlToMysqlJobDetailRespV2
// @Security ApiKeyAuth
// @Param job_id query string true "job id"
// @Router /v2/job/migration/detail [get]
func GetMigrationJobDetailV2(c echo.Context) error {
	logger := handler.NewLogger().Named("GetMigrationJobDetailV2")
	return GetMysqlToMysqlJobDetail(c, logger, DtleJobTypeMigration)
}

func GetMysqlToMysqlJobDetail(c echo.Context, logger hclog.Logger, jobType DtleJobType) error {
	logger.Info("validate params")
	reqParam := new(models.MysqlToMysqlJobDetailReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}
	err := checkJobAccess(c, reqParam.JobId)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	resp, err := getMysqlToMysqlJobDetail(logger, reqParam.JobId, jobType)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	resp.BasicTaskProfile.ConnectionInfo.SrcDataBase.MysqlPassword = "*"
	resp.BasicTaskProfile.ConnectionInfo.DstDataBase.MysqlPassword = "*"

	return c.JSON(http.StatusOK, resp)
}

func getMysqlToMysqlJobDetail(logger hclog.Logger, jobId string, jobType DtleJobType) (*models.MysqlToMysqlJobDetailRespV2, error) {
	failover, nomadJob, allocations, err := getJobDetailFromNomad(logger, jobId, jobType)
	if nil != err {
		return nil, err
	}
	destTaskDetail, srcTaskDetail, err := buildMysqlToMysqlJobDetailResp(nomadJob, allocations)
	if nil != err {
		return nil, fmt.Errorf("build job detail response failed: %v", err)
	}
	basicTaskProfile, taskLog, err := buildBasicTaskProfile(logger, jobId, &srcTaskDetail, &destTaskDetail, nil)
	if nil != err {
		return nil, fmt.Errorf("build job basic task profile failed: %v", err)
	}
	basicTaskProfile.Configuration.FailOver = failover
	if len(nomadJob.TaskGroups) != 0 {
		basicTaskProfile.Configuration.RetryTimes = *nomadJob.TaskGroups[0].RestartPolicy.Attempts
	}
	return &models.MysqlToMysqlJobDetailRespV2{
		BasicTaskProfile: basicTaskProfile,
		TaskLogs:         taskLog,
		BaseResp:         models.BuildBaseResp(nil),
	}, nil
}

func buildBasicTaskProfile(logger hclog.Logger, jobId string, srcTaskDetail *models.MysqlSrcTaskDetail,
	destMySqlTaskDetail *models.MysqlDestTaskDetail, destKafkaTaskDetail *models.KafkaDestTaskDetail) (models.BasicTaskProfile, []models.TaskLog, error) {
	nodes, err := FindNomadNodes(logger)
	if nil != err {
		return models.BasicTaskProfile{}, nil, fmt.Errorf("find nodes info response failed: %v", err)
	}
	nodeId2Addr := make(map[string]string, 0)
	for _, node := range nodes {
		nodeId2Addr[node.NodeId] = node.NodeAddress
	}
	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return models.BasicTaskProfile{}, nil, fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)
	}
	consulJobItem, err := storeManager.GetJobInfo(jobId)
	if err != nil {
		return models.BasicTaskProfile{}, nil, fmt.Errorf("consul_addr=%v; get ket %v Job Item failed: %v", jobId, handler.ConsulAddr, err)
	}

	nomadJobMap, err := findJobsFromNomad()
	if err != nil {
		return models.BasicTaskProfile{}, nil, fmt.Errorf("find nomad job list err %v", err)
	}
	basicTaskProfile := models.BasicTaskProfile{}
	basicTaskProfile.JobBaseInfo = models.JobBaseInfo{
		JobId:             jobId,
		SubscriptionTopic: consulJobItem.Topic,
		JobStatus:         consulJobItem.JobStatus,
		JobCreateTime:     consulJobItem.JobCreateTime,
		JobSteps:          consulJobItem.JobSteps,
		Delay:             0,
	}
	if nomadJobItem, ok := nomadJobMap[consulJobItem.JobId]; ok && basicTaskProfile.JobBaseInfo.JobStatus == common.DtleJobStatusNonPaused {
		basicTaskProfile.JobBaseInfo.JobStatus = nomadJobItem.Status
	}
	basicTaskProfile.Configuration = models.Configuration{
		BinlogRelay:        srcTaskDetail.TaskConfig.BinlogRelay,
		FailOver:           false,
		ReplChanBufferSize: int(srcTaskDetail.TaskConfig.ReplChanBufferSize),
		GroupMaxSize:       srcTaskDetail.TaskConfig.GroupMaxSize,
		ChunkSize:          int(srcTaskDetail.TaskConfig.ChunkSize),
		GroupTimeout:       srcTaskDetail.TaskConfig.GroupTimeout,
		DropTableIfExists:  srcTaskDetail.TaskConfig.DropTableIfExists,
		SkipCreateDbTable:  srcTaskDetail.TaskConfig.SkipCreateDbTable,
	}
	basicTaskProfile.ConnectionInfo = models.ConnectionInfo{
		SrcDataBase: *srcTaskDetail.TaskConfig.MysqlConnectionConfig,
	}
	basicTaskProfile.ReplicateDoDb = srcTaskDetail.TaskConfig.ReplicateDoDb
	basicTaskProfile.ReplicateIgnoreDb = srcTaskDetail.TaskConfig.ReplicateIgnoreDb

	dtleNodeInfosMap := make(map[string]models.DtleNodeInfo, 0)
	taskLogs := make([]models.TaskLog, 0)
	for _, srcAllocation := range srcTaskDetail.Allocations {
		dtleNode := models.DtleNodeInfo{
			NodeId:   srcAllocation.NodeId,
			NodeAddr: nodeId2Addr[srcAllocation.NodeId],
			DataSource: fmt.Sprintf("%v:%v", srcTaskDetail.TaskConfig.MysqlConnectionConfig.MysqlHost,
				srcTaskDetail.TaskConfig.MysqlConnectionConfig.MysqlPort),
			Source: "src",
		}
		if _, ok := dtleNodeInfosMap[fmt.Sprintf("%s:%s", dtleNode.NodeId, dtleNode.DataSource)]; !ok {
			dtleNodeInfosMap[fmt.Sprintf("%s:%s", dtleNode.NodeId, dtleNode.DataSource)] = dtleNode
		}
		taskLogs = append(taskLogs, models.TaskLog{
			TaskEvents:   srcAllocation.TaskStatus.TaskEvents,
			NodeId:       srcAllocation.NodeId,
			AllocationId: srcAllocation.AllocationId,
			Address:      nodeId2Addr[srcAllocation.NodeId],
			Target:       "src",
		})
	}
	if destMySqlTaskDetail != nil {
		basicTaskProfile.ConnectionInfo.DstDataBase = *destMySqlTaskDetail.TaskConfig.MysqlConnectionConfig
		basicTaskProfile.Configuration.ParallelWorkers = destMySqlTaskDetail.TaskConfig.ParallelWorkers

		for _, destAllocation := range destMySqlTaskDetail.Allocations {
			dtleNode := models.DtleNodeInfo{
				NodeId:   destAllocation.NodeId,
				NodeAddr: nodeId2Addr[destAllocation.NodeId],
				DataSource: fmt.Sprintf("%v:%v", destMySqlTaskDetail.TaskConfig.MysqlConnectionConfig.MysqlHost,
					destMySqlTaskDetail.TaskConfig.MysqlConnectionConfig.MysqlPort),
				Source: "dst",
			}
			if _, ok := dtleNodeInfosMap[fmt.Sprintf("%s:%s", dtleNode.NodeId, dtleNode.DataSource)]; !ok {
				dtleNodeInfosMap[fmt.Sprintf("%s:%s", dtleNode.NodeId, dtleNode.DataSource)] = dtleNode
			}
			taskLogs = append(taskLogs, models.TaskLog{
				TaskEvents:   destAllocation.TaskStatus.TaskEvents,
				NodeId:       destAllocation.NodeId,
				AllocationId: destAllocation.AllocationId,
				Address:      nodeId2Addr[destAllocation.NodeId],
				Target:       "dst",
			})
		}
	}
	if destKafkaTaskDetail != nil {
		for _, destAllocation := range destKafkaTaskDetail.Allocations {
			dtleNode := models.DtleNodeInfo{
				NodeId:     destAllocation.NodeId,
				NodeAddr:   nodeId2Addr[destAllocation.NodeId],
				DataSource: fmt.Sprintf("%s", destKafkaTaskDetail.TaskConfig.BrokerAddrs),
				Source:     "dst",
			}
			if _, ok := dtleNodeInfosMap[fmt.Sprintf("%s:%s", dtleNode.NodeId, dtleNode.DataSource)]; !ok {
				dtleNodeInfosMap[fmt.Sprintf("%s:%s", dtleNode.NodeId, dtleNode.DataSource)] = dtleNode
			}
			taskLogs = append(taskLogs, models.TaskLog{
				TaskEvents:   destAllocation.TaskStatus.TaskEvents,
				NodeId:       destAllocation.NodeId,
				AllocationId: destAllocation.AllocationId,
				Address:      nodeId2Addr[destAllocation.NodeId],
				Target:       "dst",
			})
		}
	}
	for _, dtleNode := range dtleNodeInfosMap {
		basicTaskProfile.DtleNodeInfos = append(basicTaskProfile.DtleNodeInfos, dtleNode)
	}

	return basicTaskProfile, taskLogs, nil
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
					TableRegex:    tb.TableRegex,
					TableRename:   tb.TableRename,
					ColumnMapFrom: tb.ColumnMapFrom,
					Where:         tb.Where,
				})
			}
			apiMysqlDataSource = append(apiMysqlDataSource, &models.MysqlDataSourceConfig{
				TableSchema:       db.TableSchema,
				TableSchemaRegex:  db.TableSchemaRegex,
				TableSchemaRename: db.TableSchemaRename,
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
			MysqlPassword: internalTaskConfig.ConnectionConfig.Password,
		},
		BinlogRelay:  internalTaskConfig.BinlogRelay,
		GroupTimeout: internalTaskConfig.GroupTimeout,
		WaitOnJob:    internalTaskConfig.WaitOnJob,
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
			MysqlPassword: internalTaskConfig.ConnectionConfig.Password,
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
				Message:    e.DisplayMessage,
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

// @Id CreateOrUpdateSyncJobV2
// @Description create or update sync job.
// @Tags job
// @Accept application/json
// @Security ApiKeyAuth
// @Param sync_job_config body models.CreateOrUpdateMysqlToMysqlJobParamV2 true "sync job config"
// @Success 200 {object} models.CreateOrUpdateMysqlToMysqlJobRespV2
// @Router /v2/job/sync [post]
func CreateOrUpdateSyncJobV2(c echo.Context) error {
	logger := handler.NewLogger().Named("CreateOrUpdateSyncJobV2")
	jobParam := new(models.CreateOrUpdateMysqlToMysqlJobParamV2)
	if err := c.Bind(jobParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(jobParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}
	if err := checkUpdateJobInfo(c, jobParam.JobId); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	user, err := getCurrentUser(c)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	resp, err := createOrUpdateMysqlToMysqlJob(logger, jobParam, user, DtleJobTypeSync)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	return c.JSON(http.StatusOK, resp)
}

// @Id GetSyncJobDetailV2
// @Description get sync job detail.
// @Tags job
// @Success 200 {object} models.MysqlToMysqlJobDetailRespV2
// @Security ApiKeyAuth
// @Param job_id query string true "job id"
// @Router /v2/job/sync/detail [get]
func GetSyncJobDetailV2(c echo.Context) error {
	logger := handler.NewLogger().Named("GetSyncJobDetailV2")
	return GetMysqlToMysqlJobDetail(c, logger, DtleJobTypeSync)
}

// @Id CreateOrUpdateSubscriptionJobV2
// @Description create or update subscription job.
// @Tags job
// @Accept application/json
// @Security ApiKeyAuth
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

	if err := checkUpdateJobInfo(c, jobParam.JobId); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	failover := g.PtrToBool(jobParam.Failover, true)

	if jobParam.IsMysqlPasswordEncrypted {
		realPwd, err := handler.DecryptPassword(jobParam.SrcTask.MysqlConnectionConfig.MysqlPassword, g.RsaPrivateKey)
		if nil != err {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("decrypt src mysql password failed: %v", err)))
		}
		jobParam.SrcTask.MysqlConnectionConfig.MysqlPassword = realPwd
	}

	// set default
	if jobParam.SrcTask.ReplChanBufferSize == 0 {
		jobParam.SrcTask.ReplChanBufferSize = common.DefaultChannelBufferSize
	}
	if jobParam.SrcTask.GroupMaxSize == 0 {
		jobParam.SrcTask.GroupMaxSize = common.DefaultSrcGroupMaxSize
	}
	if jobParam.SrcTask.GroupTimeout == 0 {
		jobParam.SrcTask.GroupTimeout = common.DefaultSrcGroupTimeout
	}
	if jobParam.SrcTask.ChunkSize == 0 {
		jobParam.SrcTask.ChunkSize = common.DefaultChunkSize
	}
	if jobParam.DestTask.MessageGroupMaxSize == 0 {
		jobParam.DestTask.MessageGroupMaxSize = common.DefaultKafkaMessageGroupMaxSize
	}
	if jobParam.DestTask.MessageGroupTimeout == 0 {
		jobParam.DestTask.MessageGroupTimeout = common.DefaultKafkaMessageGroupTimeout
	}

	jobParam.JobId = addJobTypeToJobId(jobParam.JobId, jobType)
	nomadJob, err := convertMysqlToKafkaJobToNomadJob(failover, jobParam)
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

	var respErr error
	if "" != nomadResp.Warnings {
		respErr = errors.New(nomadResp.Warnings)
	} else {
		user, err := getCurrentUser(c)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
		}
		err = buildKafkaJobListItem(logger, jobParam, user)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
		}
	}
	return c.JSON(http.StatusOK, &models.CreateOrUpdateMysqlToKafkaJobRespV2{
		CreateOrUpdateMysqlToKafkaJobParamV2: *jobParam,
		EvalCreateIndex:                      nomadResp.EvalCreateIndex,
		JobModifyIndex:                       nomadResp.JobModifyIndex,
		BaseResp:                             models.BuildBaseResp(respErr),
	})
}

func convertMysqlToKafkaJobToNomadJob(failover bool, apiJobParams *models.CreateOrUpdateMysqlToKafkaJobParamV2) (*nomadApi.Job, error) {
	srcTask, err := buildNomadTaskGroupItem(buildMysqlSrcTaskConfigMap(apiJobParams.SrcTask), apiJobParams.SrcTask.TaskName, apiJobParams.SrcTask.NodeId, failover, apiJobParams.Retry)
	if nil != err {
		return nil, fmt.Errorf("build src task failed: %v", err)
	}

	destTask, err := buildNomadTaskGroupItem(buildKafkaDestTaskConfigMap(apiJobParams.DestTask), apiJobParams.DestTask.TaskName, apiJobParams.DestTask.NodeId, failover, apiJobParams.Retry)
	if nil != err {
		return nil, fmt.Errorf("build dest task failed: %v", err)
	}

	return &nomadApi.Job{
		ID:          &apiJobParams.JobId,
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

// @Id GetSubscriptionJobDetailV2
// @Description get subscription job detail.
// @Tags job
// @Success 200 {object} models.MysqlToKafkaJobDetailRespV2
// @Security ApiKeyAuth
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

	err := checkJobAccess(c, reqParam.JobId)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	failover, nomadJob, allocations, err := getJobDetailFromNomad(logger, reqParam.JobId, DtleJobTypeSubscription)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	destTaskDetail, srcTaskDetail, err := buildMysqlToKafkaJobDetailResp(nomadJob, allocations)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("build job detail response failed: %v", err)))
	}

	basicTaskProfile, taskLog, err := buildBasicTaskProfile(logger, reqParam.JobId, &srcTaskDetail, nil, &destTaskDetail)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("build job basic task profile failed: %v", err)))
	}
	basicTaskProfile.Configuration.FailOver = failover
	if len(nomadJob.TaskGroups) != 0 {
		basicTaskProfile.Configuration.RetryTimes = *nomadJob.TaskGroups[0].RestartPolicy.Attempts
	}
	return c.JSON(http.StatusOK, &models.MysqlToKafkaJobDetailRespV2{
		BasicTaskProfile: basicTaskProfile,
		TaskLogs:         taskLog,
		BaseResp:         models.BuildBaseResp(nil),
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

// @Id PauseJobV2
// @Description pause job.
// @Tags job
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
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

	err := checkJobAccess(c, reqParam.JobId)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	logger.Info("get allocations of job", "job_id", reqParam.JobId)
	url := handler.BuildUrl(fmt.Sprintf("/v1/job/%v/allocations", reqParam.JobId))
	logger.Info("invoke nomad api begin", "url", url)
	nomadAllocs := []nomadApi.Allocation{}
	if err := handler.InvokeApiWithKvData(http.MethodGet, url, nil, &nomadAllocs); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke nomad api %v failed: %v", url, err)))
	}
	logger.Info("invoke nomad api finished")

	if len(nomadAllocs) == 0 {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("can not find allocations of the job[%v]", reqParam.JobId)))
	}

	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)))
	}
	consulJobItem, err := storeManager.GetJobInfo(reqParam.JobId)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("job_id=%v; get job status failed: %v", reqParam.JobId, err)))
	}
	if consulJobItem.JobStatus == common.DtleJobStatusPaused {
		return c.JSON(http.StatusOK, &models.PauseJobRespV2{
			BaseResp: models.BuildBaseResp(nil),
		})
	}

	consulJobItem.JobStatus = common.DtleJobStatusPaused
	consulJobItem.JobId = reqParam.JobId
	// update metadata first
	if err := storeManager.SaveJobInfo(*consulJobItem); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("job_id=%v; update job from consul failed: %v", reqParam.JobId, err)))
	}

	needRollbackMetadata := false
	defer func() {
		if needRollbackMetadata {
			logger.Info("pause job failed, rollback metadata")
			consulJobItem.JobStatus = common.DtleJobStatusNonPaused
			if err := storeManager.SaveJobInfo(*consulJobItem); nil != err {
				logger.Error("rollback metadata failed", "error", err)
			}
		}
	}()

	// pause job
	for _, a := range nomadAllocs {
		if a.DesiredStatus == "run" { // the allocations will be stop by nomad if it is not desired to run. and there is no need to pause these allocations
			if err := sentSignalToTask(logger, a.ID, "pause"); nil != err {
				needRollbackMetadata = true
				return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("allocation_id=%v; pause task failed:  %v", a.ID, err)))
			}
		}
	}

	return c.JSON(http.StatusOK, &models.PauseJobRespV2{
		BaseResp: models.BuildBaseResp(nil),
	})
}

// @Id ResumeJobV2
// @Description resume job.
// @Tags job
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
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

	err := checkJobAccess(c, reqParam.JobId)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	logger.Info("get allocations of job", "job_id", reqParam.JobId)
	url := handler.BuildUrl(fmt.Sprintf("/v1/job/%v/allocations", reqParam.JobId))
	logger.Info("invoke nomad api begin", "url", url)
	nomadAllocs := []nomadApi.Allocation{}
	if err := handler.InvokeApiWithKvData(http.MethodGet, url, nil, &nomadAllocs); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke nomad api %v failed: %v", url, err)))
	}
	logger.Info("invoke nomad api finished")

	if len(nomadAllocs) == 0 {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("job_id=%v; can not find allocations of the job", reqParam.JobId)))
	}

	// update metadata first
	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)))
	}
	consulJobItem, err := storeManager.GetJobInfo(reqParam.JobId)
	if nil != err {
		return c.JSON(http.StatusInternalServerError,
			models.BuildBaseResp(fmt.Errorf("job_id=%v; get job status failed: %v", reqParam.JobId, err)))
	}

	if consulJobItem.JobStatus != common.DtleJobStatusPaused {
		return c.JSON(http.StatusOK, &models.PauseJobRespV2{
			BaseResp: models.BuildBaseResp(nil),
		})
	}

	consulJobItem.JobStatus = common.DtleJobStatusNonPaused
	if err := storeManager.SaveJobInfo(*consulJobItem); nil != err {
		return c.JSON(http.StatusInternalServerError,
			models.BuildBaseResp(fmt.Errorf("job_id=%v; update job from consul failed: %v", reqParam.JobId, err)))
	}

	needRollbackMetadata := false
	defer func() {
		if needRollbackMetadata {
			logger.Info("resume job failed, rollback metadata")
			consulJobItem.JobStatus = common.DtleJobStatusPaused
			if err := storeManager.SaveJobInfo(*consulJobItem); nil != err {
				logger.Error("rollback metadata failed", "error", err)
			}
		}
	}()

	// resume job
	for _, a := range nomadAllocs {
		if a.DesiredStatus == "run" { // the allocations will be stop by nomad if it is not desired to run. and there is no need to pause these allocations
			if err := sentSignalToTask(logger, a.ID, "resume"); nil != err {
				return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("allocation_id=%v; resume task failed:  %v", a.ID, err)))
			}
		}

	}

	return c.JSON(http.StatusOK, &models.ResumeJobRespV2{
		BaseResp: models.BuildBaseResp(nil),
	})
}

func sentSignalToTask(logger hclog.Logger, allocId, signal string) error {
	logger.Debug("sentSignalToTask")
	if "" == allocId {
		return fmt.Errorf("allocation id is required")
	}
	param := fmt.Sprintf(`{"Signal":"%v"}`, signal)
	resp := structs.GenericResponse{}
	url := handler.BuildUrl(fmt.Sprintf("/v1/client/allocation/%v/signal", allocId))
	if err := handler.InvokePostApiWithJson(url, []byte(param), &resp); nil != err {
		return fmt.Errorf("invoke nomad api %v failed: %v", url, err)
	}
	return nil
}

// @Id DeleteJobV2
// @Description delete job.
// @Tags job
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param job_id formData string true "job id"
// @Success 200 {object} models.DeleteJobRespV2
// @Router /v2/job/delete [post]
func DeleteJobV2(c echo.Context) error {
	logger := handler.NewLogger().Named("DeleteJobV2")
	logger.Info("validate params")
	reqParam := new(models.DeleteJobReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}
	err := checkJobAccess(c, reqParam.JobId)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	logger.Info("delete job from nomad", "job_id", reqParam.JobId)
	url := handler.BuildUrl(fmt.Sprintf("/v1/job/%v?purge=true", reqParam.JobId))
	logger.Info("invoke nomad api begin", "url", url, "method", "DELETE")
	nomadResp := nomadApi.JobDeregisterResponse{}
	if err := handler.InvokeHttpUrlWithBody(http.MethodDelete, url, nil, &nomadResp); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke nomad api failed: %v", err)))
	}
	logger.Info("invoke nomad api finished")

	logger.Info("delete metadata of job from consul", "job_id", reqParam.JobId)
	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("get consul client failed: %v", err)))
	}
	if err := storeManager.DestroyJob(reqParam.JobId); nil != err {
		return c.JSON(http.StatusInternalServerError,
			models.BuildBaseResp(fmt.Errorf("delete metadata of job[job_id=%v] from consul failed: %v", reqParam.JobId, err)))
	}

	return c.JSON(http.StatusOK, &models.DeleteJobRespV2{
		BaseResp: models.BuildBaseResp(nil),
	})
}

// @Id GetJobGtid
// @Description get src task current gtid.
// @Tags job
// @Success 200 {object} models.JobGtidResp
// @Security ApiKeyAuth
// @Param job_id query string true "job id"
// @Router /v2/job/gtid [get]
func GetJobGtid(c echo.Context) error {
	logger := handler.NewLogger().Named("GetJobGtid")

	reqParam := new(models.GetJobGtidReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}

	err := checkJobAccess(c, reqParam.JobId)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)))
	}
	Gtid, err := storeManager.GetGtidForJob(reqParam.JobId)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v ; get job status list failed: %v", handler.ConsulAddr, err)))
	}

	return c.JSON(http.StatusOK, &models.JobGtidResp{
		Gtid:     Gtid,
		BaseResp: models.BaseResp{},
	})
}

// @Summary start reverse-init job
// @Id ReverseStartJob
// @Tags job
// @Description Finish Job.
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param job_id formData string true "job id"
// @Success 200 {object} models.ReverseStartRespV2
// @Router /v2/job/reverse_start [post]
func ReverseStartJob(c echo.Context) error {
	logger := handler.NewLogger().Named("ReverseStartJobV2")
	logger.Info("validate params")
	reqParam := new(models.ReverseStartReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}
	err := checkJobAccess(c, reqParam.JobId)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	// get wait on job from current job detail info
	_, nomadJob, allocations, err := getJobDetailFromNomad(logger, reqParam.JobId, getJobTypeFromJobId(reqParam.JobId))
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	_, srcTaskDetail, err := buildMysqlToMysqlJobDetailResp(nomadJob, allocations)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("build job detail response failed: %v", err)))
	}

	// finish wait on job
	waitOnJob := srcTaskDetail.TaskConfig.WaitOnJob
	logger.Info("get allocations of job", "job_id", waitOnJob)
	url := handler.BuildUrl(fmt.Sprintf("/v1/job/%v/allocations", waitOnJob))
	logger.Info("invoke nomad api begin", "url", url)
	nomadAllocs := []nomadApi.Allocation{}
	if err := handler.InvokeApiWithKvData(http.MethodGet, url, nil, &nomadAllocs); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke nomad api %v failed: %v", url, err)))
	}
	logger.Info("invoke nomad api finished")

	if len(nomadAllocs) == 0 {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("can not find allocations of the job[%v]", reqParam.JobId)))
	}

	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)))
	}
	consulJobItem, err := storeManager.GetJobInfo(waitOnJob)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("job_id=%v; get job status failed: %v", reqParam.JobId, err)))
	}
	if consulJobItem.JobStatus == common.DtleJobStatusPaused {
		return c.JSON(http.StatusInternalServerError, &models.ReverseStartRespV2{
			BaseResp: models.BuildBaseResp(errors.New("job was paused")),
		})
	}
	noRunJob := true
	// finish job
	for _, a := range nomadAllocs {
		if a.DesiredStatus == "run" && a.TaskGroup == "src" { // the allocations will be stop by nomad if it is not desired to run. and there is no need to finish these allocations
			noRunJob = false
			if err := sentSignalToTask(logger, a.ID, "finish"); nil != err {
				return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("allocation_id=%v; finish task failed:  %v", a.ID, err)))
			}
		}
	}
	if noRunJob {
		return c.JSON(http.StatusInternalServerError,
			models.BuildBaseResp(fmt.Errorf("cannot find a src allocation task whose desired status is run")))
	}

	return c.JSON(http.StatusOK, &models.ReverseStartRespV2{
		BaseResp: models.BuildBaseResp(nil),
	})
}

// @Id ReverseJob
// @Description returnJob
// @Tags job
// @Accept application/json
// @Security ApiKeyAuth
// @Param reverse_config body models.ReverseJobReq true "reverse config config"
// @Success 200 {object} models.ReverseJobResp
// @Router /v2/job/reverse [post]
func ReverseJob(c echo.Context) error {
	logger := handler.NewLogger().Named("ReverseJob")
	logger.Info("validate params")
	reqParam := new(models.ReverseJobReq)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}
	err := checkJobAccess(c, reqParam.JobId)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)))
	}
	consulJobItem, err := storeManager.GetJobInfo(reqParam.JobId)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("job_id=%v; get job status failed: %v", reqParam.JobId, err)))
	}

	// job name
	jobType := getJobTypeFromJobId(consulJobItem.JobId)
	switch jobType {
	case DtleJobTypeMigration, DtleJobTypeSync:
		originalJob, err := getMysqlToMysqlJobDetail(logger, consulJobItem.JobId, jobType)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
		}

		reverseJobParam := new(models.CreateOrUpdateMysqlToMysqlJobParamV2)
		reverseJobParam.JobId = fmt.Sprintf("%s-%s", "reverse", strings.Replace(consulJobItem.JobId, fmt.Sprintf("-%s", jobType), "", -1))
		reverseJobParam.TaskStepName = mysql.JobIncrCopy
		reverseJobParam.Failover = &originalJob.BasicTaskProfile.Configuration.FailOver
		reverseJobParam.Reverse = true
		reverseJobParam.SrcTask = &models.MysqlSrcTaskConfig{
			TaskName:              "src",
			GroupMaxSize:          originalJob.BasicTaskProfile.Configuration.GroupMaxSize,
			ChunkSize:             int64(originalJob.BasicTaskProfile.Configuration.ChunkSize),
			DropTableIfExists:     originalJob.BasicTaskProfile.Configuration.DropTableIfExists,
			SkipCreateDbTable:     originalJob.BasicTaskProfile.Configuration.SkipCreateDbTable,
			ReplChanBufferSize:    int64(originalJob.BasicTaskProfile.Configuration.ReplChanBufferSize),
			ReplicateDoDb:         originalJob.BasicTaskProfile.ReplicateDoDb,
			ReplicateIgnoreDb:     originalJob.BasicTaskProfile.ReplicateIgnoreDb,
			MysqlConnectionConfig: &originalJob.BasicTaskProfile.ConnectionInfo.DstDataBase,
			BinlogRelay:           originalJob.BasicTaskProfile.Configuration.BinlogRelay,
			GroupTimeout:          originalJob.BasicTaskProfile.Configuration.GroupTimeout,
			WaitOnJob:             consulJobItem.JobId,
			AutoGtid:              true,
		}
		reverseJobParam.DestTask = &models.MysqlDestTaskConfig{
			TaskName:              "dest",
			ParallelWorkers:       originalJob.BasicTaskProfile.Configuration.ParallelWorkers,
			MysqlConnectionConfig: &originalJob.BasicTaskProfile.ConnectionInfo.SrcDataBase,
		}

		// the node must be bound to a fixed data source
		for _, node := range originalJob.BasicTaskProfile.DtleNodeInfos {
			if node.Source == "src" {
				reverseJobParam.DestTask.NodeId = node.NodeId
			} else if node.Source == "dst" {
				reverseJobParam.SrcTask.NodeId = node.NodeId
			}
		}

		if reqParam.ReverseConfig != nil {
			reverseJobParam.SrcTask.MysqlConnectionConfig.MysqlUser = reqParam.ReverseConfig.SrcUser
			reverseJobParam.SrcTask.MysqlConnectionConfig.MysqlPassword = reqParam.ReverseConfig.SrcPwd
			reverseJobParam.DestTask.MysqlConnectionConfig.MysqlUser = reqParam.ReverseConfig.DestUser
			reverseJobParam.DestTask.MysqlConnectionConfig.MysqlPassword = reqParam.ReverseConfig.DstPwd
		}
		reverseJobParam.Retry = originalJob.BasicTaskProfile.Configuration.RetryTimes

		// IsMysqlPasswordEncrypted is set to default false then decrypt pwd
		if reqParam.ReverseConfig.IsMysqlPasswordEncrypted {
			err := decryptMySQLPwd(reverseJobParam.SrcTask, reverseJobParam.DestTask)
			if nil != err {
				return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
			}
			reverseJobParam.IsMysqlPasswordEncrypted = false
		}
		// validate job
		validationTasks, err := validateTaskConfig(reverseJobParam.SrcTask, reverseJobParam.DestTask)
		if nil != err {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("validate task config failed: %v", err)))
		}
		for i := range validationTasks {
			if validationTasks[i].ConnectionValidation.Error != "" {
				return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("validate task config fail,check Mysql connection info")))
			}
			if validationTasks[i].PrivilegesValidation.Error != "" {
				return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("validate task config fail,check Mysql privileges info")))
			}
		}

		user, err := getCurrentUser(c)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
		}
		_, err = createOrUpdateMysqlToMysqlJob(logger, reverseJobParam, user, jobType)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
		}
	default:
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(errors.New("job type is not supported")))
	}

	return c.JSON(http.StatusOK, &models.ReverseJobResp{
		BaseResp: models.BuildBaseResp(nil),
	})
}

func checkJobAccess(c echo.Context, jobId string) error {
	logger := handler.NewLogger().Named("checkJobAccess")
	logger.Info("start checkJobAccess")
	user, err := getCurrentUser(c)
	if err != nil {
		return fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)
	}
	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)
	}
	job, err := storeManager.GetJobInfo(jobId)
	if nil != err {
		return fmt.Errorf("consul_addr=%v ; get job status list failed: %v", handler.ConsulAddr, err)
	}

	if !userHasAccess(storeManager, job.User, user) {
		return fmt.Errorf("current user %v:%v has not access to operate  job job_id=%v", user.Tenant, user.Username, jobId)
	}
	return nil
}

func checkUpdateJobInfo(c echo.Context, jobId string) error {
	logger := handler.NewLogger().Named("checkUpdateJobInfo")
	logger.Info("start checkUpdateJobInfo")
	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)
	}
	jobInfo, err := storeManager.GetJobInfo(jobId)
	if nil != err {
		return fmt.Errorf("job_id=%v; get job status failed: %v", jobId, err)
	} else if jobInfo.User != "" {
		err = checkJobAccess(c, jobId)
		if err != nil {
			return err
		}
	}
	return nil
}
