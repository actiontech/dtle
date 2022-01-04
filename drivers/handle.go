package drivers

import (
	"context"
	"fmt"
	"github.com/actiontech/dtle/drivers/common"
	"sync"
	"time"

	"github.com/actiontech/dtle/drivers/oracle/applier"
	"github.com/actiontech/dtle/drivers/oracle/extractor"

	"github.com/actiontech/dtle/g"

	"github.com/actiontech/dtle/drivers/kafka"
	"github.com/actiontech/dtle/drivers/mysql"
	"github.com/armon/go-metrics"
	"github.com/pkg/errors"

	"github.com/hashicorp/nomad/plugins/drivers"
)

type taskHandle struct {
	logger g.LoggerType

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

	driverConfig *common.MySQLDriverConfig
}

func newDtleTaskHandle(logger g.LoggerType, cfg *drivers.TaskConfig, state drivers.TaskState, started time.Time) *taskHandle {
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

	if h.runner != nil {
		stat, err := h.runner.Stats()
		if err != nil {
			return nil, errors.Wrap(err, "runner.Stats")
		}
		m["GtidSet"] = stat.CurrentCoordinates.GtidSet
	}

	// TODO Cannot get InspectTask -> TaskStatus called by any API.
	// See https://github.com/hashicorp/nomad/issues/4848
	return &drivers.TaskStatus{
		ID:               h.taskConfig.ID,
		Name:             h.taskConfig.Name,
		State:            h.procState,
		StartedAt:        h.startedAt,
		CompletedAt:      h.completedAt,
		ExitResult:       h.exitResult,
		DriverAttributes: m,
	}, nil
}

func (h *taskHandle) onError(err error) {
	h.waitCh <- &drivers.ExitResult{
		ExitCode:  common.TaskStateDead,
		Signal:    0,
		OOMKilled: false,
		Err:       err,
	}
}

func (h *taskHandle) IsRunning() bool {
	h.stateLock.RLock()
	defer h.stateLock.RUnlock()
	return h.procState == drivers.TaskStateRunning
}

func (h *taskHandle) resumeTask(d *drivers2.Driver) error {
	var err error
	h.runner, err = h.NewRunner(d)
	if err != nil {
		return err
	}
	go h.runner.Run()
	return nil
}

func (h *taskHandle) run(d *drivers2.Driver) {
	h.stateLock.Lock()
	if h.exitResult == nil {
		h.exitResult = &drivers.ExitResult{}
	}
	h.procState = drivers.TaskStateRunning
	h.stateLock.Unlock()

	consulJobInfo, err := d.storeManager.GetJobInfo(h.taskConfig.JobName)
	if nil != err {
		h.onError(errors.Wrap(err, "get pause status from consul failed"))
		return
	}

	if consulJobInfo.JobStatus == common.DtleJobStatusPaused {
		h.logger.Info("job is paused. not starting the runner")
	} else {
		err = h.resumeTask(d)
		if err != nil {
			h.onError(err)
			return
		}
	}

	go func() {
		duration := time.Duration(d.config.StatsCollectionInterval) * time.Second
		t := time.NewTimer(0)
		for {
			select {
			case <-h.ctx.Done():
				return
			case <-t.C:
				if h.runner != nil {
					s, err := h.runner.Stats()
					if err != nil {
						// ignore
					} else {
						h.stats = s
						if d.config.PublishMetrics {
							h.logger.Trace("emitStats")
							h.emitStats(s)
						}
					}
				}
				t.Reset(duration)
			}
		}
	}()
}

func (h *taskHandle) NewRunner(d *drivers2.Driver) (runner DriverHandle, err error) {
	ctx := &common.ExecContext{
		Subject:  h.taskConfig.JobName,
		StateDir: d.config.DataDir,
	}

	switch common.TaskTypeFromString(h.taskConfig.Name) {
	case common.TaskTypeSrc:
		if h.driverConfig.OracleConfig != nil {
			h.logger.Debug("found oracle src", "OracleConfig", h.driverConfig.OracleConfig)
			runner, err = extractor.NewExtractorOracle(ctx, h.driverConfig, h.logger, d.storeManager, h.waitCh)
			if err != nil {
				return nil, errors.Wrap(err, "NewExtractor")
			}
		} else {
			runner, err = mysql.NewExtractor(ctx, h.driverConfig, h.logger, d.storeManager, h.waitCh, h.ctx)
			if err != nil {
				return nil, errors.Wrap(err, "NewOracleExtractor")
			}
		}
	case common.TaskTypeDest:
		h.logger.Debug("found dest", "allConfig", h.driverConfig)
		if h.driverConfig.KafkaConfig != nil {
			h.logger.Debug("found kafka", "KafkaConfig", h.driverConfig.KafkaConfig)
			runner, err = kafka.NewKafkaRunner(ctx, h.driverConfig.KafkaConfig, h.logger,
				d.storeManager, d.config.NatsAdvertise, h.waitCh, h.ctx)
			if err != nil {
				return nil, errors.Wrap(err, "NewKafkaRunner")
			}
		} else if h.driverConfig.OracleConfig != nil {
			h.logger.Debug("found oracle dest", "OracleConfig", h.driverConfig.OracleConfig)
			runner, err = applier.NewApplierOracle(ctx, h.driverConfig, h.logger, d.storeManager,
				d.config.NatsAdvertise, h.waitCh, d.eventer, h.taskConfig)
			if err != nil {
				return nil, errors.Wrap(err, "NewOracleRunner")
			}
		} else {
			runner, err = mysql.NewApplier(ctx, h.driverConfig, h.logger, d.storeManager,
				d.config.NatsAdvertise, h.waitCh, d.eventer, h.taskConfig, h.ctx)
			if err != nil {
				return nil, errors.Wrap(err, "NewApplier")
			}
		}
	case common.TaskTypeUnknown:
		return nil, fmt.Errorf("unknown processor type: %+v", h.taskConfig.Name)
	}
	return runner, err
}

func (h *taskHandle) emitStats(ru *common.TaskStatistics) {
	const srcFullFactor float32 = 4.5
	const dstFullFactor float32 = 5
	const srcIncrFactor float32 = 19
	const dstIncrFactor float32 = 9.5

	labels := []metrics.Label{{"task_name", fmt.Sprintf("%s_%s", h.taskConfig.JobName, h.taskConfig.Name)}}

	metrics.SetGaugeWithLabels([]string{"network", "in_msgs"}, float32(ru.MsgStat.InMsgs), labels)
	metrics.SetGaugeWithLabels([]string{"network", "out_msgs"}, float32(ru.MsgStat.OutMsgs), labels)
	metrics.SetGaugeWithLabels([]string{"network", "in_bytes"}, float32(ru.MsgStat.InBytes), labels)
	metrics.SetGaugeWithLabels([]string{"network", "out_bytes"}, float32(ru.MsgStat.OutBytes), labels)
	switch common.TaskTypeFromString(h.taskConfig.Name) {
	case common.TaskTypeSrc:
		metrics.SetGaugeWithLabels([]string{"buffer", "event_queue_size"}, float32(ru.BufferStat.BinlogEventQueueSize), labels)
		metrics.SetGaugeWithLabels([]string{"buffer", "src_queue_size"}, float32(ru.BufferStat.ExtractorTxQueueSize), labels)
		metrics.SetGaugeWithLabels([]string{"buffer", "send_by_timeout"}, float32(ru.BufferStat.SendByTimeout), labels)
		metrics.SetGaugeWithLabels([]string{"buffer", "send_by_size_full"}, float32(ru.BufferStat.SendBySizeFull), labels)

		metrics.SetGaugeWithLabels([]string{"memory.full_kb_est"}, float32(ru.MemoryStat.Full)*srcFullFactor/1024, labels)
		metrics.SetGaugeWithLabels([]string{"memory.incr_kb_est"}, float32(ru.MemoryStat.Incr)*srcIncrFactor/1024, labels)
	case common.TaskTypeDest:
		metrics.SetGaugeWithLabels([]string{"buffer", "dest_queue_size"}, float32(ru.BufferStat.ApplierMsgQueueSize), labels)
		metrics.SetGaugeWithLabels([]string{"buffer", "dest_queue2_size"}, float32(ru.BufferStat.ApplierTxQueueSize), labels)

		metrics.SetGaugeWithLabels([]string{"memory.full_kb_est"}, float32(ru.MemoryStat.Full)*dstFullFactor/1024, labels)
		metrics.SetGaugeWithLabels([]string{"memory.incr_kb_est"}, float32(ru.MemoryStat.Incr)*dstIncrFactor/1024, labels)
	case common.TaskTypeUnknown:
	}

	metrics.SetGaugeWithLabels([]string{"memory.full_kb_count"}, float32(ru.MemoryStat.Full)/1024, labels)
	metrics.SetGaugeWithLabels([]string{"memory.incr_kb_count"}, float32(ru.MemoryStat.Incr)/1024, labels)

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

	if nil != ru.HandledTxCount.AppliedTxCount {
		metrics.SetGaugeWithLabels([]string{"dest_applied_incr_tx_count"}, float32(*ru.HandledTxCount.AppliedTxCount), labels)
	}
	if nil != ru.HandledTxCount.ExtractedTxCount {
		metrics.SetGaugeWithLabels([]string{"src_extracted_incr_tx_count"}, float32(*ru.HandledTxCount.ExtractedTxCount), labels)
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

	Finish1() error
}
