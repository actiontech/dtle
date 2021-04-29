package v2

import (
	"fmt"
	"net"
	"net/http"

	"github.com/actiontech/dtle/drivers/api/handler"
	"github.com/actiontech/dtle/drivers/api/models"
	"github.com/actiontech/dtle/drivers/mysql"
	"github.com/actiontech/dtle/g"
	nomadApi "github.com/hashicorp/nomad/api"
	"github.com/labstack/echo/v4"
)

// @Description get progress of tasks within an allocation.
// @Tags monitor
// @Param allocation_id query string true "allocation id"
// @Param task_name query string true "task name"
// @Param filter_task_name query string false "filter task name"
// @Success 200 {object} models.GetTaskProgressRespV2
// @Router /v2/monitor/task [get]
func GetTaskProgressV2(c echo.Context) error {
	reqParam := new(models.GetTaskProgressReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}

	url := handler.BuildUrl("/v1/allocations")
	nomadAllocs := []nomadApi.Allocation{}
	if err := handler.InvokeNomadGetApi(url, &nomadAllocs); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke nomad api %v failed: %v", url, err)))
	}

	nodeId := ""
	for _, alloc := range nomadAllocs {
		if alloc.ID != reqParam.AllocationId {
			continue
		}
		nodeId = alloc.NodeID
		break
	}
	if "" == nodeId {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("can not find out which node the allocation is running on")))
	}

	url = handler.BuildUrl(fmt.Sprintf("/v1/node/%v", nodeId))
	nomadNode := nomadApi.Node{}
	if err := handler.InvokeNomadGetApi(url, &nomadNode); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke nomad api %v failed: %v", url, err)))
	}

	nodeAddr := nomadNode.HTTPAddr
	targetHost, _, err := net.SplitHostPort(nodeAddr)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("get target host failed: %v", err)))
	}
	selfApiHost, _, err := net.SplitHostPort(handler.ApiAddr)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("get self api host failed: %v", err)))
	}

	res := models.GetTaskProgressRespV2{
		BaseResp: models.BuildBaseResp(nil),
	}
	if targetHost != selfApiHost {
		// forward
		url = fmt.Sprintf("http://%v/v1/agent/self", nodeAddr)
		nomadAgentSelf := nomadApi.AgentSelf{}
		if err := handler.InvokeNomadGetApi(url, &nomadAgentSelf); nil != err {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke nomad api %v failed: %v", url, err)))
		}

		forwardAddr, err := getApiAddrFromAgentConfig(nomadAgentSelf.Config)
		if nil != err {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("getApiAddrFromAgentConfig failed: %v", err)))
		}

		url = fmt.Sprintf("http://%v/v2/monitor/task", forwardAddr)
		args := map[string]string{
			"allocation_id": reqParam.AllocationId,
			"task_name":     reqParam.TaskName,
		}
		if err := handler.InvokeApiWithFormData(http.MethodGet, url, args, &res); nil != err {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("forward api %v failed: %v", url, err)))
		}
	} else {
		taskStatus, ok, err := mysql.AllocIdTaskNameToTaskHandler.GetTaskStatistics(reqParam.AllocationId, reqParam.TaskName)
		if nil != err {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("get task stats failed: %v. allocation_id=%v task_name=%v", err, reqParam.AllocationId, reqParam.TaskName)))
		} else if !ok {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("can not find the task. allocation_id=%v task_name=%v", reqParam.AllocationId, reqParam.TaskName)))
		}

		// build response struct
		var currentCoordinates *models.CurrentCoordinates
		var delayCount *models.DelayCount
		var throughputStat *models.ThroughputStat
		if nil != taskStatus.CurrentCoordinates {
			currentCoordinates = &models.CurrentCoordinates{
				File:               taskStatus.CurrentCoordinates.File,
				Position:           taskStatus.CurrentCoordinates.Position,
				GtidSet:            taskStatus.CurrentCoordinates.GtidSet,
				RelayMasterLogFile: taskStatus.CurrentCoordinates.RelayMasterLogFile,
				ReadMasterLogPos:   taskStatus.CurrentCoordinates.ReadMasterLogPos,
				RetrievedGtidSet:   taskStatus.CurrentCoordinates.RetrievedGtidSet,
			}
		}

		if nil != taskStatus.DelayCount {
			delayCount = &models.DelayCount{
				Num:  taskStatus.DelayCount.Num,
				Time: taskStatus.DelayCount.Time,
			}
		}

		if nil != taskStatus.ThroughputStat {
			throughputStat = &models.ThroughputStat{
				Num:  taskStatus.ThroughputStat.Num,
				Time: taskStatus.ThroughputStat.Time,
			}
		}

		res.TaskStatus = &models.TaskProgress{
			CurrentCoordinates: currentCoordinates,
			DelayCount:         delayCount,
			ProgressPct:        taskStatus.ProgressPct,
			ExecMasterRowCount: taskStatus.ExecMasterRowCount,
			ExecMasterTxCount:  taskStatus.ExecMasterTxCount,
			ReadMasterRowCount: taskStatus.ReadMasterRowCount,
			ReadMasterTxCount:  taskStatus.ReadMasterTxCount,
			ETA:                taskStatus.ETA,
			Backlog:            taskStatus.Backlog,
			ThroughputStat:     throughputStat,
			NatsMsgStat: &models.NatsMessageStatistics{
				InMsgs:     taskStatus.MsgStat.InMsgs,
				OutMsgs:    taskStatus.MsgStat.OutMsgs,
				InBytes:    taskStatus.MsgStat.InBytes,
				OutBytes:   taskStatus.MsgStat.OutBytes,
				Reconnects: taskStatus.MsgStat.Reconnects,
			},
			BufferStat: &models.BufferStat{
				BinlogEventQueueSize: taskStatus.BufferStat.BinlogEventQueueSize,
				ExtractorTxQueueSize: taskStatus.BufferStat.ExtractorTxQueueSize,
				ApplierTxQueueSize:   taskStatus.BufferStat.ApplierTxQueueSize,
				SendByTimeout:        taskStatus.BufferStat.SendByTimeout,
				SendBySizeFull:       taskStatus.BufferStat.SendBySizeFull,
			},
			Stage:     taskStatus.Stage,
			Timestamp: taskStatus.Timestamp,
		}
	}

	return c.JSON(http.StatusOK, &res)
}

func getApiAddrFromAgentConfig(agentConfig map[string]interface{}) (addr string, err error) {
	plugins, ok := agentConfig["Plugins"].([]interface{})
	if !ok {
		return "", fmt.Errorf("can not find plugin config")
	}

	for _, p := range plugins {
		plugin := p.(map[string]interface{})
		if plugin["Name"] == g.PluginName {
			driverConfig, ok := plugin["Config"].(map[string]interface{})
			if !ok {
				return "", fmt.Errorf("can not find driver config within dtle plugin config")
			}

			addr = driverConfig["api_addr"].(string)
			if _, _, err = net.SplitHostPort(addr); nil != err {
				return "", fmt.Errorf("api_addr=%v is invalid: %v", addr, err)
			}

			return addr, nil

		}
	}

	return "", fmt.Errorf("can not find dtle config")
}
