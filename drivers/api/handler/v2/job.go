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

	"github.com/actiontech/dtle/drivers/mysql/common"

	"github.com/actiontech/dtle/g"

	"github.com/actiontech/dtle/drivers/api/models"

	"github.com/actiontech/dtle/drivers/api/handler"
	nomadApi "github.com/hashicorp/nomad/api"
	"github.com/labstack/echo/v4"
)

// @Id MigrationJobListV2
// @Description get job list.
// @Tags job
// @Success 200 {object} models.JobListRespV2
// @Security ApiKeyAuth
// @Param filter_job_id query string false "filter job id"
// @Param filter_job_src_ip query string false "filter job src ip"
// @Param filter_job_src_port query string false "filter job src port"
// @Param filter_job_dest_ip query string false "filter job dest ip"
// @Param filter_job_dest_port query string false "filter job dest port"
// @Param filter_job_status query string false "filter job status"
// @Param order_by query string false "order by" default(job_create_time) Enums(job_create_time)
// @Router /v2/jobs/migration [get]
func MigrationJobListV2(c echo.Context) error {
	return JobListV2(c, DtleJobTypeMigration)
}

// @Id SyncJobListV2
// @Description get sync job list.
// @Tags job
// @Success 200 {object} models.JobListRespV2
// @Security ApiKeyAuth
// @Param filter_job_id query string false "filter job id"
// @Param filter_job_src_ip query string false "filter job src ip"
// @Param filter_job_src_port query string false "filter job src port"
// @Param filter_job_dest_ip query string false "filter job dest ip"
// @Param filter_job_dest_port query string false "filter job dest port"
// @Param filter_job_status query string false "filter job status"
// @Param order_by query string false "order by" default(job_create_time) Enums(job_create_time)
// @Router /v2/jobs/sync [get]
func SyncJobListV2(c echo.Context) error {
	return JobListV2(c, DtleJobTypeSync)
}

// @Id SubscriptionJobListV2
// @Description get subscription job list.
// @Tags job
// @Success 200 {object} models.JobListRespV2
// @Security ApiKeyAuth
// @Param filter_job_id query string false "filter job id"
// @Param filter_job_src_ip query string false "filter job src ip"
// @Param filter_job_src_port query string false "filter job src port"
// @Param filter_job_dest_ip query string false "filter job dest ip"
// @Param filter_job_dest_port query string false "filter job dest port"
// @Param filter_job_status query string false "filter job status"
// @Param order_by query string false "order by" default(job_create_time) Enums(job_create_time)
// @Router /v2/jobs/subscription [get]
func SubscriptionJobListV2(c echo.Context) error {
	return JobListV2(c, DtleJobTypeSubscription)
}

func JobListV2(c echo.Context, filterJobType DtleJobType) error {
	logger := handler.NewLogger().Named("JobListV2")
	reqParam := new(models.JobListReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
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
		jobType := GetJobTypeFromJobId(consulJob.JobId)
		if filterJobType != jobType {
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
		if !(filterJobAddr(jobItem.SrcAddrList, reqParam.FilterJobSrcIP, reqParam.FilterJobSrcPort) &&
			filterJobAddr(jobItem.DstAddrList, reqParam.FilterJobDestIP, reqParam.FilterJobDestPort)) {
			continue
		}

		jobs = append(jobs, jobItem)
	}

	return c.JSON(http.StatusOK, &models.JobListRespV2{
		Jobs:     jobs,
		BaseResp: models.BuildBaseResp(nil),
	})
}

func filterJobAddr(addrList []string, ip, port string) bool {
	if ip == "" && port == "" {
		return true
	}

	for _, addr := range addrList {
		// database addr =  ip:port
		// kafka addr    =  ip
		addrList := strings.Split(addr, ":")

		if len(addrList) == 2 {
			// ip port filter
			if ip != "" && port != "" {
				if addrList[0] == ip && addrList[1] == port {
					return true
				}
			}
			// port filter
			if port != "" && ip == "" {
				if addrList[1] == port {
					return true
				}
			}
		}
		// ip filter
		if ip != "" && port == "" {
			if addrList[0] == ip {
				return true
			}
		}

	}
	return false
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

func GetJobTypeFromJobId(jobId string) DtleJobType {
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

func CreateOrUpdateMigrationJobV2(c echo.Context, create bool) error {
	logger := handler.NewLogger().Named("CreateMigrationJobV2")
	reqParam := new(models.CreateOrUpdateMysqlToMysqlJobParamV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	// create need add job type
	if create {
		reqParam.JobId = addJobTypeToJobId(reqParam.JobId, DtleJobTypeMigration)
	}
	if err := checkUpdateJobInfo(c, reqParam.JobId, create); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	user, err := getCurrentUser(c)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	resp, err := createOrUpdateMysqlToMysqlJob(logger, reqParam, user)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	return c.JSON(http.StatusOK, resp)
}

// @Id CreateMigrationJobV2
// @Description create migration job.
// @Tags job
// @Accept application/json
// @Security ApiKeyAuth
// @Param migration_job_config body models.CreateOrUpdateMysqlToMysqlJobParamV2 true "migration job config"
// @Success 200 {object} models.CreateOrUpdateMysqlToMysqlJobRespV2
// @Router /v2/job/migration/create [post]
func CreateMigrationJobV2(c echo.Context) error {
	return CreateOrUpdateMigrationJobV2(c, true)
}

// @Id UpdateMigrationJobV2
// @Description update migration job.
// @Tags job
// @Accept application/json
// @Security ApiKeyAuth
// @Param migration_job_config body models.CreateOrUpdateMysqlToMysqlJobParamV2 true "migration job config"
// @Success 200 {object} models.CreateOrUpdateMysqlToMysqlJobRespV2
// @Router /v2/job/migration/update [post]
func UpdateMigrationJobV2(c echo.Context) error {
	return CreateOrUpdateMigrationJobV2(c, false)
}

func createOrUpdateMysqlToMysqlJob(logger g.LoggerType, jobParam *models.CreateOrUpdateMysqlToMysqlJobParamV2,
	user *common.User) (*models.CreateOrUpdateMysqlToMysqlJobRespV2, error) {

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
	if !jobParam.DestTask.UseMySQLDependency && jobParam.DestTask.DependencyHistorySize == 0 {
		jobParam.DestTask.DependencyHistorySize = common.DefaultDependencyHistorySize
	}

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
	srcTask, srcDataCenter, err := buildNomadTaskGroupItem(buildMysqlSrcTaskConfigMap(jobParams.SrcTask), jobParams.SrcTask.TaskName, jobParams.SrcTask.NodeId, failover, jobParams.Retry)
	if nil != err {
		return nil, fmt.Errorf("build src task failed: %v", err)
	}
	destTask, destDataCenter, err := buildNomadTaskGroupItem(buildMysqlDestTaskConfigMap(jobParams.DestTask), jobParams.DestTask.TaskName, jobParams.DestTask.NodeId, failover, jobParams.Retry)
	if nil != err {
		return nil, fmt.Errorf("build dest task failed: %v", err)
	}
	dataCenters, err := buildDataCenters(srcDataCenter, destDataCenter)
	if nil != err {
		return nil, fmt.Errorf("build job dada center failed: %v", err)
	}
	return &nomadApi.Job{
		ID:          &jobParams.JobId,
		Datacenters: dataCenters,
		TaskGroups:  []*nomadApi.TaskGroup{srcTask, destTask},
	}, nil
}

func buildDataCenters(srcDataCenter, destDataCenter string) ([]string, error) {
	dataCenters := make([]string, 0)
	if srcDataCenter != "" && destDataCenter != "" {
		dataCenters = append(dataCenters, srcDataCenter, destDataCenter)
	} else {
		nodes, err := FindNodeList()
		if err != nil {
			return nil, err
		}
		for _, node := range nodes {
			dataCenters = append(dataCenters, node.Datacenter)
		}
	}
	dataCenters = removeDuplicateElement(dataCenters)
	return dataCenters, nil
}

func removeDuplicateElement(datas []string) []string {
	result := make([]string, 0, len(datas))
	temp := map[string]struct{}{}
	for _, item := range datas {
		if _, ok := temp[item]; !ok {
			temp[item] = struct{}{}
			result = append(result, item)
		}
	}
	return result
}

func buildMySQLJobListItem(logger g.LoggerType, jobParam *models.CreateOrUpdateMysqlToMysqlJobParamV2,
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
		SrcAddrList:   []string{fmt.Sprintf("%s:%d", jobParam.SrcTask.MysqlConnectionConfig.MysqlHost, jobParam.SrcTask.MysqlConnectionConfig.MysqlPort)},
		DstAddrList:   []string{fmt.Sprintf("%s:%d", jobParam.DestTask.MysqlConnectionConfig.MysqlHost, jobParam.DestTask.MysqlConnectionConfig.MysqlPort)},
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

func buildKafkaJobListItem(logger g.LoggerType, jobParam *models.CreateOrUpdateMysqlToKafkaJobParamV2,
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

func buildNomadTaskGroupItem(dtleTaskconfig map[string]interface{}, taskName, nodeId string, failover bool, retryTimes int) (*nomadApi.TaskGroup, string, error) {
	dataCenter := ""
	task := nomadApi.NewTask(taskName, g.PluginName)
	task.Config = dtleTaskconfig
	if !failover && "" == nodeId {
		return nil, dataCenter, fmt.Errorf("node id should be provided if failover is false. task_name=%v", taskName)
	}
	if nodeId != "" {
		if failover {
			// https://www.nomadproject.io/docs/runtime/interpolation
			newAff := nomadApi.NewAffinity("${node.unique.id}", "=", nodeId, 100)
			task.Affinities = append(task.Affinities, newAff)
			if node, err := GetNodeInfo(nodeId); err != nil {
				return nil, dataCenter, err
			} else if node.Datacenter != "" {
				newConstraint := nomadApi.NewConstraint("${node.datacenter}", "=", node.Datacenter)
				task.Constraints = append(task.Constraints, newConstraint)
				dataCenter = node.Datacenter
			}
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
	return taskGroup, dataCenter, nil
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
	addNotRequiredParamToMap(taskConfigInNomadFormat, config.UseMySQLDependency, "UseMySQLDependency")
	addNotRequiredParamToMap(taskConfigInNomadFormat, config.DependencyHistorySize, "DependencyHistorySize")
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
	addNotRequiredParamToMap(taskConfigInNomadFormat, config.ExpandSyntaxSupport, "ExpandSyntaxSupport")

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

func GetMysqlToMysqlJobDetail(c echo.Context, logger g.LoggerType, jobType DtleJobType) error {

	reqParam := new(models.MysqlToMysqlJobDetailReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
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

func getMysqlToMysqlJobDetail(logger g.LoggerType, jobId string, jobType DtleJobType) (*models.MysqlToMysqlJobDetailRespV2, error) {
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

func buildBasicTaskProfile(logger g.LoggerType, jobId string, srcTaskDetail *models.MysqlSrcTaskDetail,
	destMySqlTaskDetail *models.MysqlDestTaskDetail, destKafkaTaskDetail *models.KafkaDestTaskDetail) (models.BasicTaskProfile, []models.TaskLog, error) {
	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return models.BasicTaskProfile{}, nil, fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)
	}

	consulJobItem, err := storeManager.GetJobInfo(jobId)
	if err != nil {
		return models.BasicTaskProfile{}, nil, fmt.Errorf("consul_addr=%v; get ket %v Job Item failed: %v", jobId, handler.ConsulAddr, err)
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

	nomadJobMap, err := findJobsFromNomad()
	if err != nil {
		return models.BasicTaskProfile{}, nil, fmt.Errorf("find nomad job list err %v", err)
	}
	if nomadJobItem, ok := nomadJobMap[consulJobItem.JobId]; ok && basicTaskProfile.JobBaseInfo.JobStatus == common.DtleJobStatusNonPaused {
		basicTaskProfile.JobBaseInfo.JobStatus = nomadJobItem.Status
	}
	basicTaskProfile.Configuration = models.Configuration{
		BinlogRelay:         srcTaskDetail.TaskConfig.BinlogRelay,
		FailOver:            false,
		ReplChanBufferSize:  int(srcTaskDetail.TaskConfig.ReplChanBufferSize),
		GroupMaxSize:        srcTaskDetail.TaskConfig.GroupMaxSize,
		ChunkSize:           int(srcTaskDetail.TaskConfig.ChunkSize),
		GroupTimeout:        srcTaskDetail.TaskConfig.GroupTimeout,
		DropTableIfExists:   srcTaskDetail.TaskConfig.DropTableIfExists,
		SkipCreateDbTable:   srcTaskDetail.TaskConfig.SkipCreateDbTable,
		ExpandSyntaxSupport: srcTaskDetail.TaskConfig.ExpandSyntaxSupport,
	}
	basicTaskProfile.ConnectionInfo = models.ConnectionInfo{
		SrcDataBase: *srcTaskDetail.TaskConfig.MysqlConnectionConfig,
	}
	basicTaskProfile.ReplicateDoDb = srcTaskDetail.TaskConfig.ReplicateDoDb
	basicTaskProfile.ReplicateIgnoreDb = srcTaskDetail.TaskConfig.ReplicateIgnoreDb

	nodes, err := FindNomadNodes(logger)
	if nil != err {
		return models.BasicTaskProfile{}, nil, fmt.Errorf("find nodes info response failed: %v", err)
	}
	nodeId2Addr := make(map[string]string, 0)
	for _, node := range nodes {
		nodeId2Addr[node.NodeId] = node.NodeAddress
	}
	taskLogs := make([]models.TaskLog, 0)
	for _, srcAllocation := range srcTaskDetail.Allocations {
		taskLogs = append(taskLogs, models.TaskLog{
			TaskEvents:   srcAllocation.TaskStatus.TaskEvents,
			NodeId:       srcAllocation.NodeId,
			AllocationId: srcAllocation.AllocationId,
			Address:      nodeId2Addr[srcAllocation.NodeId],
			Target:       "src",
		})
		if srcAllocation.DesiredStatus == nomadApi.AllocDesiredStatusRun {
			dtleNode := models.DtleNodeInfo{
				NodeId:   srcAllocation.NodeId,
				NodeAddr: nodeId2Addr[srcAllocation.NodeId],
				DataSource: fmt.Sprintf("%v:%v", srcTaskDetail.TaskConfig.MysqlConnectionConfig.MysqlHost,
					srcTaskDetail.TaskConfig.MysqlConnectionConfig.MysqlPort),
				Source: "src",
			}
			basicTaskProfile.DtleNodeInfos = append(basicTaskProfile.DtleNodeInfos, dtleNode)
		}
	}
	if destMySqlTaskDetail != nil {
		basicTaskProfile.ConnectionInfo.DstDataBase = *destMySqlTaskDetail.TaskConfig.MysqlConnectionConfig
		basicTaskProfile.Configuration.ParallelWorkers = destMySqlTaskDetail.TaskConfig.ParallelWorkers
		basicTaskProfile.Configuration.UseMySQLDependency = destMySqlTaskDetail.TaskConfig.UseMySQLDependency
		basicTaskProfile.Configuration.DependencyHistorySize = destMySqlTaskDetail.TaskConfig.DependencyHistorySize
		for _, destAllocation := range destMySqlTaskDetail.Allocations {
			taskLogs = append(taskLogs, models.TaskLog{
				TaskEvents:   destAllocation.TaskStatus.TaskEvents,
				NodeId:       destAllocation.NodeId,
				AllocationId: destAllocation.AllocationId,
				Address:      nodeId2Addr[destAllocation.NodeId],
				Target:       "dst",
			})
			if destAllocation.DesiredStatus == nomadApi.AllocDesiredStatusRun {
				dtleNode := models.DtleNodeInfo{
					NodeId:   destAllocation.NodeId,
					NodeAddr: nodeId2Addr[destAllocation.NodeId],
					DataSource: fmt.Sprintf("%v:%v", destMySqlTaskDetail.TaskConfig.MysqlConnectionConfig.MysqlHost,
						destMySqlTaskDetail.TaskConfig.MysqlConnectionConfig.MysqlPort),
					Source: "dst",
				}
				basicTaskProfile.DtleNodeInfos = append(basicTaskProfile.DtleNodeInfos, dtleNode)
			}
		}
	}
	if destKafkaTaskDetail != nil {
		basicTaskProfile.ConnectionInfo.DstKafka = destKafkaTaskDetail.TaskConfig
		for _, destAllocation := range destKafkaTaskDetail.Allocations {
			taskLogs = append(taskLogs, models.TaskLog{
				TaskEvents:   destAllocation.TaskStatus.TaskEvents,
				NodeId:       destAllocation.NodeId,
				AllocationId: destAllocation.AllocationId,
				Address:      nodeId2Addr[destAllocation.NodeId],
				Target:       "dst",
			})
			if destAllocation.DesiredStatus == nomadApi.AllocDesiredStatusRun {
				dtleNode := models.DtleNodeInfo{
					NodeId:     destAllocation.NodeId,
					NodeAddr:   nodeId2Addr[destAllocation.NodeId],
					DataSource: strings.Join(destKafkaTaskDetail.TaskConfig.BrokerAddrs, ","),
					Source:     "dst",
				}
				basicTaskProfile.DtleNodeInfos = append(basicTaskProfile.DtleNodeInfos, dtleNode)
			}
		}
	}
	return basicTaskProfile, taskLogs, nil
}

func getJobDetailFromNomad(logger g.LoggerType, jobId string, jobType DtleJobType) (failover bool, nomadJob nomadApi.Job, nomadAllocations []nomadApi.Allocation, err error) {
	url := handler.BuildUrl(fmt.Sprintf("/v1/job/%v", jobId))
	logger.Info("invoke nomad api begin", "url", url)

	if err := handler.InvokeApiWithKvData(http.MethodGet, url, nil, &nomadJob); nil != err {
		return false, nomadApi.Job{}, nil, fmt.Errorf("invoke nomad api %v failed: %v", url, err)
	}
	logger.Info("invoke nomad api finished")

	if jobType != GetJobTypeFromJobId(g.PtrToString(nomadJob.ID, "")) {
		return false, nomadApi.Job{}, nil, fmt.Errorf("this API is for %v job. but got job type=%v by the provided job id", jobType, GetJobTypeFromJobId(g.PtrToString(nomadJob.ID, "")))
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
		BinlogRelay:         internalTaskConfig.BinlogRelay,
		GroupTimeout:        internalTaskConfig.GroupTimeout,
		WaitOnJob:           internalTaskConfig.WaitOnJob,
		ExpandSyntaxSupport: internalTaskConfig.ExpandSyntaxSupport,
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
		UseMySQLDependency:    internalTaskConfig.UseMySQLDependency,
		DependencyHistorySize: internalTaskConfig.DependencyHistorySize,
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
		newAlloc.DesiredStatus = nomadAllocation.DesiredStatus
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

func CreateOrUpdateSyncJobV2(c echo.Context, create bool) error {
	logger := handler.NewLogger().Named("CreateOrUpdateSyncJobV2")
	jobParam := new(models.CreateOrUpdateMysqlToMysqlJobParamV2)
	if err := handler.BindAndValidate(logger, c, jobParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	// create need add job type
	if create {
		jobParam.JobId = addJobTypeToJobId(jobParam.JobId, DtleJobTypeSync)
	}
	if err := checkUpdateJobInfo(c, jobParam.JobId, create); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	user, err := getCurrentUser(c)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	resp, err := createOrUpdateMysqlToMysqlJob(logger, jobParam, user)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	return c.JSON(http.StatusOK, resp)
}

// @Id CreateSyncJobV2
// @Description create sync job.
// @Tags job
// @Accept application/json
// @Security ApiKeyAuth
// @Param sync_job_config body models.CreateOrUpdateMysqlToMysqlJobParamV2 true "sync job config"
// @Success 200 {object} models.CreateOrUpdateMysqlToMysqlJobRespV2
// @Router /v2/job/sync/create [post]
func CreateSyncJobV2(c echo.Context) error {
	return CreateOrUpdateSyncJobV2(c, true)
}

// @Id UpdateSyncJobV2
// @Description update sync job.
// @Tags job
// @Accept application/json
// @Security ApiKeyAuth
// @Param sync_job_config body models.CreateOrUpdateMysqlToMysqlJobParamV2 true "sync job config"
// @Success 200 {object} models.CreateOrUpdateMysqlToMysqlJobRespV2
// @Router /v2/job/sync/update [post]
func UpdateSyncJobV2(c echo.Context) error {
	return CreateOrUpdateSyncJobV2(c, false)
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

// @Id CreateSubscriptionJobV2
// @Description create subscription job.
// @Tags job
// @Accept application/json
// @Security ApiKeyAuth
// @Param subscription_job_config body models.CreateOrUpdateMysqlToKafkaJobParamV2 true "subscription job config"
// @Success 200 {object} models.CreateOrUpdateMysqlToKafkaJobRespV2
// @Router /v2/job/subscription/create [post]
func CreateSubscriptionJobV2(c echo.Context) error {
	logger := handler.NewLogger().Named("CreateSubscriptionJobV2")
	return createOrUpdateMysqlToKafkaJob(c, logger, DtleJobTypeSubscription, true)
}

// @Id CreateOrUpdateSubscriptionJobV2
// @Description update subscription job.
// @Tags job
// @Accept application/json
// @Security ApiKeyAuth
// @Param subscription_job_config body models.CreateOrUpdateMysqlToKafkaJobParamV2 true "subscription job config"
// @Success 200 {object} models.CreateOrUpdateMysqlToKafkaJobRespV2
// @Router /v2/job/subscription/update [post]
func UpdateSubscriptionJobV2(c echo.Context) error {
	logger := handler.NewLogger().Named("UpdateSubscriptionJobV2")
	return createOrUpdateMysqlToKafkaJob(c, logger, DtleJobTypeSubscription, false)
}

func createOrUpdateMysqlToKafkaJob(c echo.Context, logger g.LoggerType, jobType DtleJobType, create bool) error {

	jobParam := new(models.CreateOrUpdateMysqlToKafkaJobParamV2)
	if err := handler.BindAndValidate(logger, c, jobParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	if err := checkUpdateJobInfo(c, jobParam.JobId, create); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	// create need add job type
	if create {
		jobParam.JobId = addJobTypeToJobId(jobParam.JobId, jobType)
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
	srcTask, srcDataCenter, err := buildNomadTaskGroupItem(buildMysqlSrcTaskConfigMap(apiJobParams.SrcTask), apiJobParams.SrcTask.TaskName, apiJobParams.SrcTask.NodeId, failover, apiJobParams.Retry)
	if nil != err {
		return nil, fmt.Errorf("build src task failed: %v", err)
	}

	destTask, destDataCenter, err := buildNomadTaskGroupItem(buildKafkaDestTaskConfigMap(apiJobParams.DestTask), apiJobParams.DestTask.TaskName, apiJobParams.DestTask.NodeId, failover, apiJobParams.Retry)
	if nil != err {
		return nil, fmt.Errorf("build dest task failed: %v", err)
	}
	dataCenters, err := buildDataCenters(srcDataCenter, destDataCenter)
	if nil != err {
		return nil, fmt.Errorf("build job dada center failed: %v", err)
	}
	return &nomadApi.Job{
		ID:          &apiJobParams.JobId,
		Datacenters: dataCenters,
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
	reqParam := new(models.MysqlToKafkaJobDetailReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
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
	basicTaskProfile.ConnectionInfo.SrcDataBase.MysqlPassword = "*"
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

// @Id PauseMigrationJobV2
// @Description pause migration job.
// @Tags job
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param job_id formData string true "job id"
// @Success 200 {object} models.PauseJobRespV2
// @Router /v2/job/migration/pause [post]
func PauseMigrationJobV2(c echo.Context) error {
	return PauseJobV2(c, DtleJobTypeMigration)
}

// @Id PauseSyncJobV2
// @Description pause sync job.
// @Tags job
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param job_id formData string true "job id"
// @Success 200 {object} models.PauseJobRespV2
// @Router /v2/job/sync/pause [post]
func PauseSyncJobV2(c echo.Context) error {
	return PauseJobV2(c, DtleJobTypeSync)
}

// @Id PauseSubscriptionJobV2
// @Description pause subscription job.
// @Tags job
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param job_id formData string true "job id"
// @Success 200 {object} models.PauseJobRespV2
// @Router /v2/job/subscription/pause [post]
func PauseSubscriptionJobV2(c echo.Context) error {
	return PauseJobV2(c, DtleJobTypeSubscription)
}

func PauseJobV2(c echo.Context, filterJobType DtleJobType) error {
	logger := handler.NewLogger().Named("PauseJobV2")
	reqParam := new(models.PauseJobReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	if filterJobType != GetJobTypeFromJobId(reqParam.JobId) {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("only supports job of type %v", filterJobType)))
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

// @Id ResumeMigrationJobV2
// @Description resume migration job.
// @Tags job
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param job_id formData string true "job id"
// @Success 200 {object} models.ResumeJobRespV2
// @Router /v2/job/migration/resume [post]
func ResumeMigrationJobV2(c echo.Context) error {
	return ResumeJobV2(c, DtleJobTypeMigration)
}

// @Id ResumeSyncJobV2
// @Description resume sync job.
// @Tags job
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param job_id formData string true "job id"
// @Success 200 {object} models.ResumeJobRespV2
// @Router /v2/job/sync/resume [post]
func ResumeSyncJobV2(c echo.Context) error {
	return ResumeJobV2(c, DtleJobTypeSync)
}

// @Id ResumeSubscriptionJobV2
// @Description resume subscription job.
// @Tags job
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param job_id formData string true "job id"
// @Success 200 {object} models.ResumeJobRespV2
// @Router /v2/job/subscription/resume [post]
func ResumeSubscriptionJobV2(c echo.Context) error {
	return ResumeJobV2(c, DtleJobTypeSubscription)
}

func ResumeJobV2(c echo.Context, filterJobType DtleJobType) error {
	logger := handler.NewLogger().Named("ResumeJobV2")
	reqParam := new(models.ResumeJobReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	if filterJobType != GetJobTypeFromJobId(reqParam.JobId) {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("only supports job of type %v", filterJobType)))
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

func sentSignalToTask(logger g.LoggerType, allocId, signal string) error {
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

// @Id DeleteMigrationJobV2
// @Description delete migration job.
// @Tags job
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param job_id formData string true "job id"
// @Success 200 {object} models.DeleteJobRespV2
// @Router /v2/job/migration/delete [post]
func DeleteMigrationJobV2(c echo.Context) error {
	return DeleteJobV2(c, DtleJobTypeMigration)
}

// @Id DeleteSyncJobV2
// @Description delete sync job.
// @Tags job
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param job_id formData string true "job id"
// @Success 200 {object} models.DeleteJobRespV2
// @Router /v2/job/sync/delete [post]
func DeleteSyncJobV2(c echo.Context) error {
	return DeleteJobV2(c, DtleJobTypeSync)
}

// @Id DeleteSubscriptionJobV2
// @Description delete subscription job.
// @Tags job
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param job_id formData string true "job id"
// @Success 200 {object} models.DeleteJobRespV2
// @Router /v2/job/subscription/delete [post]
func DeleteSubscriptionJobV2(c echo.Context) error {
	return DeleteJobV2(c, DtleJobTypeSubscription)
}

func DeleteJobV2(c echo.Context, filterJobType DtleJobType) error {
	logger := handler.NewLogger().Named("DeleteJobV2")
	reqParam := new(models.DeleteJobReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	if filterJobType != GetJobTypeFromJobId(reqParam.JobId) {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("only supports job of type %v", filterJobType)))
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

// @Id GetJobGtidV2
// @Description get src task current gtid.
// @Tags job
// @Success 200 {object} models.JobGtidResp
// @Security ApiKeyAuth
// @Param job_id query string true "job id"
// @Router /v2/job/gtid [get]
func GetJobGtidV2(c echo.Context) error {
	logger := handler.NewLogger().Named("GetJobGtidV2")
	reqParam := new(models.GetJobGtidReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
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
// @Id ReverseStartMigrationJobV2
// @Tags job
// @Description Start Reverse Job.
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param job_id formData string true "job id"
// @Success 200 {object} models.ReverseStartRespV2
// @Router /v2/job/migration/reverse_start [post]
func ReverseStartMigrationJobV2(c echo.Context) error {
	return ReverseStartJobV2(c, DtleJobTypeMigration)
}

// @Summary start reverse-init job
// @Id ReverseStartSyncJobV2
// @Tags job
// @Description Start Reverse Job.
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param job_id formData string true "job id"
// @Success 200 {object} models.ReverseStartRespV2
// @Router /v2/job/sync/reverse_start [post]
func ReverseStartSyncJobV2(c echo.Context) error {
	return ReverseStartJobV2(c, DtleJobTypeSync)
}

// @Summary start reverse-init job
// @Id ReverseStartJobV2
// @Tags job
// @Description Finish Job.
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param job_id formData string true "job id"
// @Success 200 {object} models.ReverseStartRespV2
// @Router /v2/job/reverse_start [post]
func ReverseStartJobV2(c echo.Context, filterJobType DtleJobType) error {
	logger := handler.NewLogger().Named("ReverseStartJobV2")
	reqParam := new(models.ReverseStartReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	if filterJobType != GetJobTypeFromJobId(reqParam.JobId) {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("only supports job of type %v", filterJobType)))
	}
	err := checkJobAccess(c, reqParam.JobId)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	// get wait on job from current job detail info
	_, nomadJob, allocations, err := getJobDetailFromNomad(logger, reqParam.JobId, GetJobTypeFromJobId(reqParam.JobId))
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

// @Id ReverseMigrationJobV2
// @Description reverse migration Job
// @Tags job
// @Accept application/json
// @Security ApiKeyAuth
// @Param reverse_config body models.ReverseJobReq true "reverse config config"
// @Success 200 {object} models.ReverseJobResp
// @Router /v2/job/migration/reverse [post]
func ReverseMigrationJobV2(c echo.Context) error {
	return ReverseJobV2(c, DtleJobTypeMigration)
}

// @Id ReverseSyncJobV2
// @Description reverse sync Job
// @Tags job
// @Accept application/json
// @Security ApiKeyAuth
// @Param reverse_config body models.ReverseJobReq true "reverse config config"
// @Success 200 {object} models.ReverseJobResp
// @Router /v2/job/sync/reverse [post]
func ReverseSyncJobV2(c echo.Context) error {
	return ReverseJobV2(c, DtleJobTypeSync)
}

func ReverseJobV2(c echo.Context, filterJobType DtleJobType) error {
	logger := handler.NewLogger().Named("ReverseJobV2")

	reqParam := new(models.ReverseJobReq)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	if GetJobTypeFromJobId(reqParam.JobId) != filterJobType {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("cannot operate job of type %v", filterJobType)))
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
	jobType := GetJobTypeFromJobId(consulJobItem.JobId)
	switch jobType {
	case DtleJobTypeMigration, DtleJobTypeSync:
		originalJob, err := getMysqlToMysqlJobDetail(logger, consulJobItem.JobId, jobType)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
		}

		reverseJobParam := new(models.CreateOrUpdateMysqlToMysqlJobParamV2)
		reverseJobParam.JobId = fmt.Sprintf("%s-%s", "reverse", consulJobItem.JobId)
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
			ExpandSyntaxSupport:   originalJob.BasicTaskProfile.Configuration.ExpandSyntaxSupport,
		}
		reverseJobParam.DestTask = &models.MysqlDestTaskConfig{
			TaskName:              "dest",
			ParallelWorkers:       originalJob.BasicTaskProfile.Configuration.ParallelWorkers,
			MysqlConnectionConfig: &originalJob.BasicTaskProfile.ConnectionInfo.SrcDataBase,
			DependencyHistorySize: originalJob.BasicTaskProfile.Configuration.DependencyHistorySize,
			UseMySQLDependency:    originalJob.BasicTaskProfile.Configuration.UseMySQLDependency,
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
			// IsMysqlPasswordEncrypted is set to default false then decrypt pwd
			if reqParam.ReverseConfig.IsMysqlPasswordEncrypted {
				err := decryptMySQLPwd(reverseJobParam.SrcTask, reverseJobParam.DestTask)
				if nil != err {
					return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
				}
				reverseJobParam.IsMysqlPasswordEncrypted = false
			}
		}
		reverseJobParam.Retry = originalJob.BasicTaskProfile.Configuration.RetryTimes

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
		_, err = createOrUpdateMysqlToMysqlJob(logger, reverseJobParam, user)
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

func checkUpdateJobInfo(c echo.Context, jobId string, create bool) error {
	logger := handler.NewLogger().Named("checkUpdateJobInfo")
	logger.Info("start checkUpdateJobInfo")
	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)
	}
	if storeManager.CheckJobExists(jobId) == create {
		return fmt.Errorf("please confirm whether the job [ %v ] already exists", jobId)
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
