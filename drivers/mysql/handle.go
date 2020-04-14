package mysql

import (
	"context"
	"fmt"
	"github.com/actiontech/dtle/drivers/mysql/mysql"
	"github.com/actiontech/dtle/drivers/mysql/mysql/common"
	"github.com/pkg/errors"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins/drivers"
)

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

	ctx context.Context
	cancelFunc context.CancelFunc
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
	}
	h.ctx, h.cancelFunc = context.WithCancel(context.TODO())
	return h
}

func (h *taskHandle) TaskStatus() *drivers.TaskStatus {
	h.stateLock.RLock()
	defer h.stateLock.RUnlock()

	return &drivers.TaskStatus{
		ID:          h.taskConfig.ID,
		Name:        h.taskConfig.Name,
		State:       h.procState,
		StartedAt:   h.startedAt,
		CompletedAt: h.completedAt,
		ExitResult:  h.exitResult,
	}
}

func (h *taskHandle) IsRunning() bool {
	h.stateLock.RLock()
	defer h.stateLock.RUnlock()
	return h.procState == drivers.TaskStateRunning
}

func (h *taskHandle) run(taskConfig *DtleTaskConfig, d *Driver) {
	var err error
	h.stateLock.Lock()
	if h.exitResult == nil {
		h.exitResult = &drivers.ExitResult{}
	}
	h.procState = drivers.TaskStateRunning
	h.stateLock.Unlock()

	// TODO: detect if the taskConfig OOMed

	cfg := h.taskConfig
	ctx := &common.ExecContext{cfg.JobName, cfg.TaskGroupName, 100 * 1024 * 1024, "/opt/binlog"}

	driverConfig, _ := InitConfig(taskConfig)

	switch cfg.TaskGroupName {
	case TaskTypeSrc:
		{
			//	d.logger.Debug("NewExtractor ReplicateDoDb: %v", driverConfig.ReplicateDoDb)
			// Create the extractor

			h.runner, err = mysql.NewExtractor(ctx, driverConfig, d.logger, d.storeManager)
			if err != nil {
				h.exitResult.Err = errors.Wrap(err, "NewExtractor")
				return
			}
			go h.runner.Run()
		}
	case TaskTypeDest:
		{
			err := d.storeManager.PutNats(cfg.JobName, d.config.NatsAdvertise)
			if err != nil {
				h.exitResult.Err = errors.Wrap(err, "PutNats")
				return
			}

			d.logger.Debug("print host", hclog.Fmt("%+v", driverConfig.ConnectionConfig.Host))

			driverConfig.NatsAddr = d.config.NatsAdvertise
			//	d.logger.Warn("NewApplier ReplicateDoDb: %v", driverConfig.ReplicateDoDb)

			h.runner, err = mysql.NewApplier(ctx, driverConfig, d.logger, d.storeManager)
			if err != nil {
				h.exitResult.Err = errors.Wrap(err, "NewApplier")
				return
			}
			go h.runner.Run()
		}
	default:
		h.exitResult.Err = fmt.Errorf("unknown processor type: %+v", cfg.TaskGroupName)
		return
	}

}
func (h *taskHandle) Destroy() bool {
	h.stateLock.RLock()
	//driver.des
	h.cancelFunc()
	err := h.runner.Shutdown()
	if err != nil {
		h.logger.Error("error in h.runner.Shutdown", "err", err)
	}
	return h.procState == drivers.TaskStateRunning
}

type DriverHandle interface {
	Run()
	// Returns an opaque handle that can be used to re-open the handle
	ID() string

	// WaitChan is used to return a channel used wait for task completion
	WaitCh() chan *drivers.ExitResult

	// Shutdown is used to stop the task
	Shutdown() error

	// Stats returns aggregated stats of the driver
	Stats() (*mysql.TaskStatistics, error)
}
