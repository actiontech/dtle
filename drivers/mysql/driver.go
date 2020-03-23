package mysql

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

		"github.com/hashicorp/go-hclog"
		"math/rand"
	config "github.com/actiontech/dtle/drivers/mysql/mysql/config"
	"github.com/actiontech/dtle/drivers/mysql/mysql/common"
	"github.com/actiontech/dtle/drivers/mysql/mysql"
//	"github.com/actiontech/dtle/drivers/mysql/mysql"
	"github.com/actiontech/dtle/drivers/shared/eventer"
	"github.com/actiontech/dtle/helper/pluginutils/loader"
	"github.com/actiontech/dtle/nomad/structs"
	"github.com/actiontech/dtle/plugins/base"
	"github.com/actiontech/dtle/plugins/drivers"
		"github.com/actiontech/dtle/plugins/shared/hclspec"
	pstructs "github.com/actiontech/dtle/plugins/shared/structs"
)

const (
	// pluginName is the name of the plugin
	pluginName = "mysql"

	// fingerprintPeriod is the interval at which the driver will send fingerprint responses
	fingerprintPeriod = 30 * time.Second

	// The key populated in Node Attributes to indicate presence of the Java driver
	driverAttr        = "driver.mysql"
	driverVersionAttr = "driver.mysql.version"

	// taskHandleVersion is the version of task handle which this driver sets
	// and understands how to decode driver state
	taskHandleVersion = 1
	TaskTypeSrc       = "Src"
	TaskTypeDest      = "Dest"
)

var (
	// PluginID is the mysql plugin metadata registered in the plugin
	// catalog.
	PluginID = loader.PluginID{
		Name:       pluginName,
		PluginType: base.PluginTypeDriver,
	}

	// PluginConfig is the java driver factory function registered in the
	// plugin catalog.
	PluginConfig = &loader.InternalPluginConfig{
		Config:  map[string]interface{}{},
		Factory: func(l hclog.Logger) interface{} { return NewDriver(l) },
	}

	// pluginInfo is the response returned for the PluginInfo RPC
	pluginInfo = &base.PluginInfoResponse{
		Type:              base.PluginTypeDriver,
		PluginApiVersions: []string{drivers.ApiVersion010},
		PluginVersion:     "0.1.0",
		Name:              pluginName,
	}

	// configSpec is the hcl specification returned by the ConfigSchema RPC
	configSpec = hclspec.NewObject(map[string]*hclspec.Spec{})

	// taskConfigSpec is the hcl specification for the driver config section of
	// a taskConfig within a job. It is returned in the TaskConfigSchema RPC
	taskConfigSpec = hclspec.NewObject(map[string]*hclspec.Spec{})

	// capabilities is returned by the Capabilities RPC and indicates what
	// optional features this driver supports
	capabilities = &drivers.Capabilities{
		SendSignals: false,
		Exec:        false,
		FSIsolation: drivers.FSIsolationNone,
		NetIsolationModes: []drivers.NetIsolationMode{
			drivers.NetIsolationModeHost,
			drivers.NetIsolationModeGroup,
		},
	}

	_ drivers.DriverPlugin = (*Driver)(nil)
)

func init() {
	if runtime.GOOS == "linux" {
		capabilities.FSIsolation = drivers.FSIsolationChroot
	}
}

// TaskState is the state which is encoded in the handle returned in
// StartTask. This information is needed to rebuild the taskConfig state and handler
// during recovery.
type TaskState struct {
	TaskConfig *drivers.TaskConfig
	StartedAt  time.Time
}

// Driver is a driver for running images via Java
type Driver struct {
	// eventer is used to handle multiplexing of TaskEvents calls such that an
	// event can be broadcast to all callers
	eventer *eventer.Eventer

	// tasks is the in memory datastore mapping taskIDs to taskHandle
	tasks *taskStore

	// ctx is the context for the driver. It is passed to other subsystems to
	// coordinate shutdown
	ctx context.Context

	// nomadConf is the client agent's configuration
	nomadConfig *base.ClientDriverConfig

	// signalShutdown is called when the driver is shutting down and cancels the
	// ctx passed to any subsystems
	signalShutdown context.CancelFunc

	// logger will log to the Nomad agent
	logger hclog.Logger

	taskName  string
	allocID   string
	node      *structs.Node
	extractor *mysql.Extractor
}


func NewDriver(logger hclog.Logger) drivers.DriverPlugin {
	ctx, cancel := context.WithCancel(context.Background())
	logger = logger.Named(pluginName)
	return &Driver{
		eventer:        eventer.NewEventer(ctx, logger),
		tasks:          newTaskStore(),
		ctx:            ctx,
		signalShutdown: cancel,
		logger:         logger,
	}
}

/*func NewMySQLDriver(ctx *DriverContext) Driver {
	return &MySQLDriver{DriverContext: *ctx}
}
*/
func (d *Driver) PluginInfo() (*base.PluginInfoResponse, error) {
	return pluginInfo, nil
}

func (d *Driver) ConfigSchema() (*hclspec.Spec, error) {
	return configSpec, nil
}

func (d *Driver) SetConfig(cfg *base.Config) error {
	if cfg != nil && cfg.AgentConfig != nil {
		d.nomadConfig = cfg.AgentConfig.Driver
	}
	return nil
}

func (d *Driver) TaskConfigSchema() (*hclspec.Spec, error) {
	return taskConfigSpec, nil
}

func (d *Driver) Capabilities() (*drivers.Capabilities, error) {
	return capabilities, nil
}

func (d *Driver) Fingerprint(ctx context.Context) (<-chan *drivers.Fingerprint, error) {
	ch := make(chan *drivers.Fingerprint)
	go d.handleFingerprint(ctx, ch)
	return ch, nil
}

//It allows the driver to indicate its health to the client.
// The channel returned should immediately send an initial Fingerprint,
// then send periodic updates at an interval that is appropriate for the driver until the context is canceled.
func (d *Driver) handleFingerprint(ctx context.Context, ch chan *drivers.Fingerprint) {
	ticker := time.NewTimer(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			ticker.Reset(fingerprintPeriod)
			ch <- d.buildFingerprint()
		}
	}
}

//get the driver status
func (d *Driver) buildFingerprint() *drivers.Fingerprint {

	var health drivers.HealthState
	var desc string
	attrs := map[string]*pstructs.Attribute{}
	if  runtime.GOOS == "linux" {
		health = drivers.HealthStateHealthy
		desc = "ready"
		attrs["driver.mysql"] = pstructs.NewBoolAttribute(true)
		attrs["driver.mysql.version"] = pstructs.NewStringAttribute("12")
	} else {
		health = drivers.HealthStateUndetected
		desc = "disabled"
	}
	return &drivers.Fingerprint{
		Attributes:        attrs,
		Health:            health,
		HealthDescription: desc,
	}
}

func (d *Driver) RecoverTask(handle *drivers.TaskHandle) error {
	if handle == nil {
		return fmt.Errorf("handle cannot be nil")
	}

	// COMPAT(0.10): pre 0.9 upgrade path check
	/*if handle.Version == 0 {
		return d.recoverPre09Task(handle)
	}*/

	// If already attached to handle there's nothing to recover.
	if _, ok := d.tasks.Get(handle.Config.ID); ok {
		d.logger.Debug("nothing to recover; task already exists",
			"task_id", handle.Config.ID,
			"task_name", handle.Config.Name,
		)
		return nil
	}

	var taskState TaskState
	if err := handle.GetDriverState(&taskState); err != nil {
		d.logger.Error("failed to decode taskConfig state from handle", "error", err, "task_id", handle.Config.ID)
		return fmt.Errorf("failed to decode taskConfig state from handle: %v", err)
	}

	h := &taskHandle{
		taskConfig: taskState.TaskConfig,
		procState:  drivers.TaskStateRunning,
		startedAt:  taskState.StartedAt,
		exitResult: &drivers.ExitResult{},
	}

	d.tasks.Set(taskState.TaskConfig.ID, h)

	go h.run()
	return nil
}

func (d *Driver) StartTask(cfg *drivers.TaskConfig) (*drivers.TaskHandle, *drivers.DriverNetwork, error) {
	if _, ok := d.tasks.Get(cfg.ID); ok {
		return nil, nil, fmt.Errorf("task with ID %q already started", cfg.ID)
	}
	var driverConfig config.MySQLDriverConfig
	if err := cfg.DecodeDriverConfig(&driverConfig); err != nil {
		return nil ,nil , fmt.Errorf("failed to decode driver config: %v", err)
	}

	ctx := &common.ExecContext{cfg.JobName, cfg.TaskGroupName, 100 * 1024 * 1024, "/opt/binlog"}
	switch cfg.TaskGroupName {
	case TaskTypeSrc:
		{
			d.logger.Debug("NewExtractor ReplicateDoDb: %v", driverConfig.ReplicateDoDb)
			// Create the extractor
			e, err := mysql.NewExtractor(ctx, &driverConfig, d.logger)
			if err != nil {
				return  nil,nil,fmt.Errorf("failed to create extractor  e: %v", err)
			}
			go e.Run()
			//d.extractor = e

		}
	case TaskTypeDest:
		{
			d.logger.Warn("NewApplier ReplicateDoDb: %v", driverConfig.ReplicateDoDb)
			a, err := mysql.NewApplier(ctx, &driverConfig,d.logger)
			if err != nil {
				return nil,nil, fmt.Errorf("failed to create Applier  e: %v", err)
			}
			go a.Run()

		}
	default:
		{
			return nil,nil,fmt.Errorf("unknown processor type : %+v", cfg.TaskGroupName)
		}
	}
	handle := drivers.NewTaskHandle(taskHandleVersion)
	handle.Config = cfg
	h := &taskHandle{
		taskConfig: cfg,
		procState:  drivers.TaskStateRunning,
		startedAt:  time.Now().Round(time.Millisecond),
		logger:     d.logger,
	}
	driverState := TaskState{
		TaskConfig: cfg,
		StartedAt:  time.Now().Round(time.Millisecond),
	}
	if err := handle.SetDriverState(&driverState); err != nil {
		d.logger.Error("failed to start task, error setting driver state", "error", err)
		return nil, nil, fmt.Errorf("failed to set driver state: %v", err)
	}
	d.tasks.Set(cfg.ID, h)
	go h.run()
	return handle, nil, nil
}

func (d *Driver) WaitTask(ctx context.Context, taskID string) (<-chan *drivers.ExitResult, error) {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}

	ch := make(chan *drivers.ExitResult)
	go d.handleWait(ctx, handle, ch)

	return ch, nil
}

func (d *Driver) handleWait(ctx context.Context, handle *taskHandle, ch chan *drivers.ExitResult) {
	defer close(ch)
	var result *drivers.ExitResult
	/*ps, err := handle.exec.Wait(ctx)
	if err != nil {
		result = &drivers.ExitResult{
			Err: fmt.Errorf("executor: error waiting on process: %v", err),
		}
	} else {
		result = &drivers.ExitResult{
			ExitCode: ps.ExitCode,
			Signal:   ps.Signal,
		}
	}*/

	select {
	case <-ctx.Done():
		return
	case <-d.ctx.Done():
		return
	case ch <- result:
	}
}

func (d *Driver) StopTask(taskID string, timeout time.Duration, signal string) error {
	/*	handle, ok := d.tasks.Get(taskID)
		if !ok {
			return drivers.ErrTaskNotFound
		}

		if err := handle.exec.Shutdown(signal, timeout); err != nil {
			if handle.pluginClient.Exited() {
				return nil
			}
			return fmt.Errorf("executor Shutdown failed: %v", err)
		}*/

	return nil
}

func (d *Driver) DestroyTask(taskID string, force bool) error {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}
	if handle.IsRunning() && !force {
		return fmt.Errorf("cannot destroy running task")
	}
	handle.Destroy()
	d.tasks.Delete(taskID)
	return nil
}

func (d *Driver) InspectTask(taskID string) (*drivers.TaskStatus, error) {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}

	return handle.TaskStatus(), nil
}

func (d *Driver) TaskStats(ctx context.Context, taskID string, interval time.Duration) (<-chan *drivers.TaskResourceUsage, error) {
	ch := make(chan *drivers.TaskResourceUsage)
	go d.handleStats(ctx, ch)
	return ch, nil
}
func (d *Driver) handleStats(ctx context.Context, ch chan<- *drivers.TaskResourceUsage) {
	timer := time.NewTimer(0)
	for {
		select {
		case <-timer.C:
			// Generate random value for the memory usage
			s := &drivers.TaskResourceUsage{
				ResourceUsage: &drivers.ResourceUsage{
					MemoryStats: &drivers.MemoryStats{
						RSS:      rand.Uint64(),
						Measured: []string{"RSS"},
					},
				},
				Timestamp: time.Now().UTC().UnixNano(),
			}
			select {
			case <-ctx.Done():
				return
			case ch <- s:
			default:
			}
		case <-ctx.Done():
			return
		}
	}
}

func (d *Driver) TaskEvents(ctx context.Context) (<-chan *drivers.TaskEvent, error) {
	return d.eventer.TaskEvents(ctx)
}

func (d *Driver) SignalTask(taskID string, signal string) error {
	h, ok := d.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}

	if h.exitResult == nil {
		return nil
	}

	return errors.New(h.exitResult.Err.Error())
}

func (d *Driver) ExecTask(taskID string, cmd []string, timeout time.Duration) (*drivers.ExecTaskResult, error) {
	h, ok := d.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}

	res := drivers.ExecTaskResult{
		Stdout:     []byte(fmt.Sprintf("Exec(%q, %q)", h.taskConfig.Name, cmd)),
		ExitResult: &drivers.ExitResult{},
	}
	return &res, nil
}

// GetAbsolutePath returns the absolute path of the passed binary by resolving
// it in the path and following symlinks.
func GetAbsolutePath(bin string) (string, error) {
	lp, err := exec.LookPath(bin)
	if err != nil {
		return "", fmt.Errorf("failed to resolve path to %q executable: %v", bin, err)
	}

	return filepath.EvalSymlinks(lp)
}

func (d *Driver) Shutdown() {
	d.signalShutdown()
}
