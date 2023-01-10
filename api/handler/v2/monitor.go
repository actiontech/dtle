package v2

import (
	"fmt"
	"net"
	"net/http"

	"github.com/actiontech/dtle/g"

	"github.com/actiontech/dtle/api/handler"
	"github.com/actiontech/dtle/api/models"
	"github.com/labstack/echo/v4"
)

// @Id GetTaskProgressV2
// @Description get progress of tasks within an allocation.
// @Tags monitor
// @Security ApiKeyAuth
// @Param allocation_id query string true "allocation id"
// @Param task_name query string true "task name"
// @Param nomad_http_address query string false "nomad_http_address is the http address of the nomad that the target dtle is running with. ignore it if you are not sure what to provide"
// @Success 200 {object} models.GetTaskProgressRespV2
// @Router /v2/monitor/task [get]
func GetTaskProgressV2(c echo.Context) error {
	logger := handler.NewLogger().Named("GetTaskProgressV2")

	reqParam := new(models.GetTaskProgressReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	logger.Warn("/v2/monitor/task is unimplemented, returning dummy data", "AllocationId", reqParam.AllocationId)

	//storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	//if err != nil {
	//	return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v; connect to consul: failed: %v", handler.ConsulAddr, err)))
	//}

	//storeManager.GetGtidForJob(reqParam.)

	res := models.GetTaskProgressRespV2{
		BaseResp: models.BuildBaseResp(nil),
	}
	res.TaskStatus = &models.TaskProgress{
		CurrentCoordinates: &models.CurrentCoordinates{},
		DelayCount:         &models.DelayCount{},
		//ProgressPct:        taskStatus.ProgressPct,
		ExecMasterRowCount: 0,
		ExecMasterTxCount:  0,
		ReadMasterRowCount: 0,
		ReadMasterTxCount:  0,
		//ETA:                taskStatus.ETA,
		//Backlog:            taskStatus.Backlog,
		//ThroughputStat:     throughputStat,
		//NatsMsgStat: &models.NatsMessageStatistics{
		//	InMsgs:     taskStatus.MsgStat.InMsgs,
		//	OutMsgs:    taskStatus.MsgStat.OutMsgs,
		//	InBytes:    taskStatus.MsgStat.InBytes,
		//	OutBytes:   taskStatus.MsgStat.OutBytes,
		//	Reconnects: taskStatus.MsgStat.Reconnects,
		//},
		//BufferStat: &models.BufferStat{
		//	BinlogEventQueueSize: taskStatus.BufferStat.BinlogEventQueueSize,
		//	ExtractorTxQueueSize: taskStatus.BufferStat.ExtractorTxQueueSize,
		//	ApplierTxQueueSize:   taskStatus.BufferStat.ApplierTxQueueSize,
		//	SendByTimeout:        taskStatus.BufferStat.SendByTimeout,
		//	SendBySizeFull:       taskStatus.BufferStat.SendBySizeFull,
		//},
		Stage:     "TODO",
		//Timestamp: taskStatus.Timestamp,
	}

	return c.JSON(http.StatusOK, &res)
}

func getApiAddrFromAgentConfig(agentConfig map[string]interface{}) (ip, port string, err error) {
	plugins, ok := agentConfig["Plugins"].([]interface{})
	if !ok {
		return "", "", fmt.Errorf("cannot find plugin config")
	}

	for _, p := range plugins {
		plugin := p.(map[string]interface{})
		if plugin["Name"] == g.PluginName {
			driverConfig, ok := plugin["Config"].(map[string]interface{})
			if !ok {
				return "", "", fmt.Errorf("cannot find driver config within dtle plugin config")
			}
			addr := driverConfig["api_addr"].(string)
			if ip, port, err = net.SplitHostPort(addr); nil != err {
				return "", "", fmt.Errorf("api_addr=%v is invalid: %v", addr, err)
			}

			return ip, port, nil

		}
	}

	return "", "", fmt.Errorf("cannot find dtle config")
}
