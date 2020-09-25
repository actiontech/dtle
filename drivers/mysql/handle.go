package mysql

import (
	"context"
	"fmt"
	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/actiontech/dtle/drivers/mysql/kafka"
	"github.com/actiontech/dtle/drivers/mysql/mysql"
	"github.com/armon/go-metrics"
	"github.com/pkg/errors"
	"strings"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins/drivers"
)

type taskType int
const (
	taskTypeUnknown taskType = iota
	taskTypeSrc
	taskTypeDest
)

func taskTypeFromString(s string) taskType {
	switch strings.ToLower(s) {
	case "src", "source":
		return taskTypeSrc
	case "dst", "dest", "destination":
		return taskTypeDest
	default:
		return taskTypeUnknown
	}
}

type taskHandle struct {
	logger hclog.Logger

	// stateLock syncs access to all fields below
	stateLock sync.RWMutex

	taskConfig  *drivers.TaskConfig
	procState   drivers.TaskState
	startedAt   time.Time
	completedAt time.Time
	exitResult  *drivers.ExitResult

	runner DriverHandle

	ctx        context.Context
	cancelFunc context.CancelFunc
	waitCh     chan *drivers.ExitResult
	stats      *common.TaskStatistics
}

func newDtleTaskHandle(logger hclog.Logger, cfg *drivers.TaskConfig, state drivers.TaskState, started time.Time) *taskHandle {
	h := &taskHandle{
		logger:      logger,
		stateLock:   sync.RWMutex{},
		taskConfig:  cfg,
		procState:   state,
		startedAt:   started,
		completedAt: time.Time{},
		exitResult:  nil,
		waitCh:      make(chan *drivers.ExitResult, 1),
	}
	h.ctx, h.cancelFunc = context.WithCancel(context.TODO())
	return h
}

func (h *taskHandle) TaskStatus() (*drivers.TaskStatus, error) {
	h.stateLock.RLock()
	defer h.stateLock.RUnlock()

	m := map[string]string{}

	stat, err := h.runner.Stats()
	if err != nil {
		return nil, errors.Wrap(err, "runner.Stats")
	}
	m["GtidSet"] = stat.CurrentCoordinates.GtidSet
	// TODO Cannot get InspectTask -> TaskStatus called by any API.
	// See https://github.com/hashicorp/nomad/issues/4848
	return &drivers.TaskStatus{
		ID:          h.taskConfig.ID,
		Name:        h.taskConfig.Name,
		State:       h.procState,
		StartedAt:   h.startedAt,
		CompletedAt: h.completedAt,
		ExitResult:  h.exitResult,
		DriverAttributes: m,
	}, nil
}

func (h *taskHandle) IsRunning() bool {
	h.stateLock.RLock()
	defer h.stateLock.RUnlock()
	return h.procState == drivers.TaskStateRunning
}

func (h *taskHandle) run(taskConfig *common.DtleTaskConfig, d *Driver) {
	var err error
	h.stateLock.Lock()
	if h.exitResult == nil {
		h.exitResult = &drivers.ExitResult{}
	}
	h.procState = drivers.TaskStateRunning
	h.stateLock.Unlock()

	// TODO: detect if the taskConfig OOMed

	cfg := h.taskConfig
	ctx := &common.ExecContext{
		Subject:    cfg.JobName,
		StateDir:   d.config.DataDir,
	}

	taskConfig.SetDefaultForEmpty()
	driverConfig := &common.MySQLDriverConfig{DtleTaskConfig: *taskConfig}

	switch taskTypeFromString(cfg.TaskGroupName) {
	case taskTypeSrc:
		h.runner, err = mysql.NewExtractor(ctx, driverConfig, d.logger, d.storeManager, h.waitCh)
		if err != nil {
			h.exitResult.Err = errors.Wrap(err, "NewExtractor")
			return
		}
		go h.runner.Run()
	case taskTypeDest:
		if taskConfig.KafkaConfig != nil {
			d.logger.Debug("found kafka", "KafkaConfig", taskConfig.KafkaConfig)
			h.runner = kafka.NewKafkaRunner(ctx, taskConfig.KafkaConfig, d.logger,
				d.storeManager, d.config.NatsAdvertise, h.waitCh)
			go h.runner.Run()
		} else {
			h.runner, err = mysql.NewApplier(ctx, driverConfig, d.logger, d.storeManager,
				d.config.NatsAdvertise, h.waitCh)
			if err != nil {
				h.exitResult.Err = errors.Wrap(err, "NewApplier")
				return
			}
			go h.runner.Run()
		}
	case taskTypeUnknown:
		h.exitResult.Err = fmt.Errorf("unknown processor type: %+v", cfg.TaskGroupName)
		return
	}

	go func() {
		duration := time.Duration(d.config.StatsCollectionInterval) * time.Second
		t := time.NewTimer(0)
		for {
			select {
			case <-h.ctx.Done():
				return
			case <-t.C:
				s, err := h.runner.Stats()
				if err != nil {
					// ignore
				} else {
					h.stats = s
					if d.config.PublishMetrics {
						h.logger.Debug("emitStats")
						h.emitStats(s)
					}
				}
				t.Reset(duration)
			}
		}
	}()
}


func (h *taskHandle) emitStats(ru *common.TaskStatistics) {
	const srcFullFactor float32 = 4.5
	const dstFullFactor float32 = 5
	const srcIncrFactor float32 = 19
	const dstIncrFactor float32 = 9.5

	labels := []metrics.Label{{"task_name", fmt.Sprintf("%s_%s", h.taskConfig.JobName, h.taskConfig.TaskGroupName)}}

	metrics.SetGaugeWithLabels([]string{"network", "in_msgs"}, float32(ru.MsgStat.InMsgs), labels)
	metrics.SetGaugeWithLabels([]string{"network", "out_msgs"}, float32(ru.MsgStat.OutMsgs), labels)
	metrics.SetGaugeWithLabels([]string{"network", "in_bytes"}, float32(ru.MsgStat.InBytes), labels)
	metrics.SetGaugeWithLabels([]string{"network", "out_bytes"}, float32(ru.MsgStat.OutBytes), labels)
	switch taskTypeFromString(h.taskConfig.TaskGroupName) {
	case taskTypeSrc:
		metrics.SetGaugeWithLabels([]string{"buffer", "event_queue_size"}, float32(ru.BufferStat.BinlogEventQueueSize), labels)
		metrics.SetGaugeWithLabels([]string{"buffer", "src_queue_size"}, float32(ru.BufferStat.ExtractorTxQueueSize), labels)
		metrics.SetGaugeWithLabels([]string{"buffer", "send_by_timeout"}, float32(ru.BufferStat.SendByTimeout), labels)
		metrics.SetGaugeWithLabels([]string{"buffer", "send_by_size_full"}, float32(ru.BufferStat.SendBySizeFull), labels)

		metrics.SetGaugeWithLabels([]string{"memory.full_kb_est"}, float32(ru.MemoryStat.Full) * srcFullFactor / 1024, labels)
		metrics.SetGaugeWithLabels([]string{"memory.incr_kb_est"}, float32(ru.MemoryStat.Incr) * srcIncrFactor / 1024, labels)
	case taskTypeDest:
		metrics.SetGaugeWithLabels([]string{"buffer", "dest_queue_size"}, float32(ru.BufferStat.ApplierTxQueueSize), labels)

		metrics.SetGaugeWithLabels([]string{"memory.full_kb_est"}, float32(ru.MemoryStat.Full) * dstFullFactor / 1024, labels)
		metrics.SetGaugeWithLabels([]string{"memory.incr_kb_est"}, float32(ru.MemoryStat.Incr) * dstIncrFactor / 1024, labels)
	case taskTypeUnknown:
	}

	metrics.SetGaugeWithLabels([]string{"memory.full_kb_count"}, float32(ru.MemoryStat.Full) / 1024, labels)
	metrics.SetGaugeWithLabels([]string{"memory.incr_kb_count"}, float32(ru.MemoryStat.Incr) / 1024, labels)

	if ru.TableStats != nil {
		metrics.SetGaugeWithLabels([]string{"table", "insert"}, float32(ru.TableStats.InsertCount), labels)
		metrics.SetGaugeWithLabels([]string{"table", "update"}, float32(ru.TableStats.UpdateCount), labels)
		metrics.SetGaugeWithLabels([]string{"table", "delete"}, float32(ru.TableStats.DelCount), labels)
	}

	if ru.DelayCount != nil {
		// TODO
		//metrics.SetGaugeWithLabels([]string{"delay", "num"}, float32(ru.DelayCount.Num), labels)
		metrics.SetGaugeWithLabels([]string{"delay", "time"}, float32(ru.DelayCount.Time), labels)
	}

	if ru.ThroughputStat != nil {
		metrics.SetGaugeWithLabels([]string{"throughput", "num"}, float32(ru.ThroughputStat.Num), labels)
		metrics.SetGaugeWithLabels([]string{"throughput", "time"}, float32(ru.ThroughputStat.Time), labels)
	}
}

func (h *taskHandle) Destroy() bool {
	h.stateLock.RLock()
	//driver.des
	h.cancelFunc()
	if h.runner != nil {
		err := h.runner.Shutdown()
		if err != nil {
			h.logger.Error("error in h.runner.Shutdown", "err", err)
		}
	}
	return h.procState == drivers.TaskStateExited
}

type DriverHandle interface {
	Run()

	// Shutdown is used to stop the task
	Shutdown() error

	// Stats returns aggregated stats of the driver
	Stats() (*common.TaskStatistics, error)
}
