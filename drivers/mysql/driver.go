package mysql

import (
	"context"
	"fmt"
	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/actiontech/dtle/drivers/mysql/config"
	"github.com/actiontech/dtle/g"
	"github.com/pkg/errors"
	"time"

	"github.com/actiontech/dtle/drivers/mysql/route"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/drivers/shared/eventer"
	"github.com/hashicorp/nomad/plugins/base"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/hashicorp/nomad/plugins/shared/hclspec"
	pstructs "github.com/hashicorp/nomad/plugins/shared/structs"
	"github.com/julienschmidt/httprouter"
	gnatsd "github.com/nats-io/nats-server/v2/server"
	stand "github.com/nats-io/nats-streaming-server/server"
	"net"
	"net/http"
)

const (
	// fingerprintPeriod is the interval at which the driver will send fingerprint responses
	fingerprintPeriod = 30 * time.Second

	driverAttr        = "driver.dtle"
	driverVersionAttr = "driver.dtle.version"
	driverFullVersionAttr = "driver.dtle.full_version"

	// taskHandleVersion is the version of task handle which this driver sets
	// and understands how to decode driver state
	taskHandleVersion = 1
)

var (
	// pluginInfo is the response returned for the PluginInfo RPC
	pluginInfo = &base.PluginInfoResponse{
		Type:              base.PluginTypeDriver,
		PluginApiVersions: []string{drivers.ApiVersion010},
		PluginVersion:     "0.1.0",
		Name:              g.PluginName,
	}

	// configSpec is the hcl specification returned by the ConfigSchema RPC
	configSpec = hclspec.NewObject(map[string]*hclspec.Spec{
		"nats_bind": hclspec.NewDefault(hclspec.NewAttr("nats_bind", "string", false),
			hclspec.NewLiteral(`"0.0.0.0:8193"`)),
		"nats_advertise": hclspec.NewDefault(hclspec.NewAttr("nats_advertise", "string", false),
			hclspec.NewLiteral(`"127.0.0.1:8193"`)),
		"api_addr": hclspec.NewDefault(hclspec.NewAttr("api_addr", "string", false),
			hclspec.NewLiteral(`""`)),
		"nomad_addr": hclspec.NewDefault(hclspec.NewAttr("nomad_addr", "string", false),
			hclspec.NewLiteral(`"127.0.0.1:4646"`)),
		"consul": hclspec.NewAttr("consul", "list(string)", true),
		"data_dir": hclspec.NewDefault(hclspec.NewAttr("data_dir", "string", false),
			hclspec.NewLiteral(`"/var/lib/nomad"`)),
	})

	// taskConfigSpec is the hcl specification for the driver config section of
	// a taskConfig within a job. It is returned in the TaskConfigSchema RPC
	taskConfigSpec = hclspec.NewObject(map[string]*hclspec.Spec{
		"ReplicateDoDb" :hclspec.NewBlockList("ReplicateDoDb",  hclspec.NewObject(map[string]*hclspec.Spec{
			"TableSchema": hclspec.NewAttr("TableSchema", "string", false),
			"TableSchemaRegex": hclspec.NewAttr("TableSchemaRegex", "string", false),
			"TableSchemaRenameRegex": hclspec.NewAttr("TableSchemaRenameRegex", "string", false),
			"TableSchemaRename": hclspec.NewAttr("TableSchemaRename", "string", false),
			"TableSchemaScope": hclspec.NewAttr("TableSchemaScope", "string", false),
			"Tables" :hclspec.NewBlockList("Tables", hclspec.NewObject(map[string]*hclspec.Spec{
				"TableName": hclspec.NewAttr("TableName", "string", false),
				"TableRegex": hclspec.NewAttr("TableRegex", "string", false),
				"TableRename": hclspec.NewAttr("TableRename", "string", false),
				"TableRenameRegex": hclspec.NewAttr("TableRenameRegex", "string", false),
				"TableSchema": hclspec.NewAttr("TableSchema", "string", false),
				"TableSchemaRename": hclspec.NewAttr("TableSchemaRename", "string", false),
				"Where": hclspec.NewAttr("Where", "string", false),
			})),
		})),
		"ReplicateIgnoreDb" :hclspec.NewBlockList("ReplicateIgnoreDb",  hclspec.NewObject(map[string]*hclspec.Spec{
			"TableSchema": hclspec.NewAttr("TableSchema", "string", false),
			"Tables" :hclspec.NewBlockList("Tables", hclspec.NewObject(map[string]*hclspec.Spec{
				"TableName": hclspec.NewAttr("TableName", "string", false),
				"TableSchema": hclspec.NewAttr("TableSchema", "string", false),
			})),
		})),
		"DropTableIfExists":hclspec.NewAttr("DropTableIfExists", "bool", false),
		"ExpandSyntaxSupport":hclspec.NewAttr("ExpandSyntaxSupport", "bool", false),
		"ReplChanBufferSize":hclspec.NewAttr("ReplChanBufferSize", "number", false),
		"TrafficAgainstLimits":hclspec.NewAttr("TrafficAgainstLimits", "number", false),
		"MaxRetries":hclspec.NewAttr("MaxRetries", "number", false),
		"ChunkSize":hclspec.NewAttr("ChunkSize", "number", false),
		"SqlFilter":hclspec.NewAttr("SqlFilter", "list(string)", false),
		"GroupMaxSize":hclspec.NewAttr("GroupMaxSize", "number", false),
		"GroupTimeout":hclspec.NewAttr("GroupTimeout", "number", false),
		"Gtid":hclspec.NewAttr("Gtid", "string", false),
		"BinlogFile":hclspec.NewAttr("BinlogFile", "string", false),
		"BinlogPos":hclspec.NewAttr("BinlogPos", "number", false),
		"GtidStart":hclspec.NewAttr("GtidStart", "string", false),
		"AutoGtid":hclspec.NewAttr("AutoGtid", "bool", false),
		"BinlogRelay":hclspec.NewAttr("BinlogRelay", "bool", false),
		"ParallelWorkers":hclspec.NewAttr("ParallelWorkers", "number", false),
		"SkipCreateDbTable":hclspec.NewAttr("SkipCreateDbTable", "bool", false),
		"SkipPrivilegeCheck":hclspec.NewAttr("SkipPrivilegeCheck", "bool", false),
		"SkipIncrementalCopy":hclspec.NewAttr("SkipIncrementalCopy", "bool", false),
		"ConnectionConfig": hclspec.NewBlock("ConnectionConfig", false, hclspec.NewObject(map[string]*hclspec.Spec{
			"Host": hclspec.NewAttr("Host", "string", true),
			"Port": hclspec.NewAttr("Port", "number", true),
			"User": hclspec.NewAttr("User", "string", true),
			"Password": hclspec.NewAttr("Password", "string", true),
			"Charset": hclspec.NewDefault(hclspec.NewAttr("Charset", "string", false),
				hclspec.NewLiteral(`"utf8mb4"`)),
		})),
		"KafkaConfig": hclspec.NewBlock("KafkaConfig", false, hclspec.NewObject(map[string]*hclspec.Spec{
			"Topic": hclspec.NewAttr("Topic", "string", true),
			"Brokers": hclspec.NewAttr("Brokers", "list(string)", true),
			"Converter": hclspec.NewDefault(hclspec.NewAttr("Converter", "string", false),
				hclspec.NewLiteral(`"json"`)),
		})),
	})

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
)

// TaskState is the state which is encoded in the handle returned in
// StartTask. This information is needed to rebuild the taskConfig state and handler
// during recovery.
type TaskState struct {
	TaskConfig     *drivers.TaskConfig
	DtleTaskConfig *config.DtleTaskConfig
	StartedAt      time.Time
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

	stand *stand.StanServer
	apiServer  *httprouter.Router

	config *DriverConfig

	storeManager *common.StoreManager
}

func NewDriver(logger hclog.Logger) drivers.DriverPlugin {
	logger = logger.Named(g.PluginName)
	logger.Info("dtle NewDriver")

	route.SetLogger(logger)

	ctx, cancel := context.WithCancel(context.Background())
	return &Driver{
		eventer:        eventer.NewEventer(ctx, logger),
		tasks:          newTaskStore(),
		ctx:            ctx,
		signalShutdown: cancel,
		logger:         logger,
	}
}

func (d *Driver) SetupNatsServer(logger hclog.Logger) (err error)  {
	natsAddr, err := net.ResolveTCPAddr("tcp", d.config.NatsBind)
	if err != nil {
		return fmt.Errorf("failed to parse Nats address. addr %v err %v",
			d.config.NatsBind, err)
	}
	nOpts := gnatsd.Options{
		Host:       natsAddr.IP.String(),
		Port:       natsAddr.Port,
		MaxPayload: 100*1024*1024,
		//HTTPPort:   8199,
		LogFile:"/opt/log",
		Debug:   true,
	}
	//logger.Debug("agent: Starting nats streaming server [%v]", "10.186.61.121:8193")
	sOpts := stand.GetDefaultOptions()
	sOpts.ID = config.DefaultClusterID
	s, err := stand.RunServerWithOpts(sOpts, &nOpts)
	if err != nil {
		return err
	}

	logger.Info("Setup nats server", "addr", d.config.NatsBind)

	d.stand = s
	return nil
}

func (d *Driver) SetupApiServer(logger hclog.Logger) (err error)  {
	route.Host =  d.config.NomadAddr
	logger.Debug("Begin Setup api server", "addr", d.config.ApiAddr)
	router := httprouter.New()
	router.GET("/v1/job/:NodeId/:path",route.JobRequest)
	router.POST("/v1/jobs",route.UpdupJob)
	router.GET("/v1/jobs",route.JobListRequest)
	router.GET("/v1/allocations",route.AllocsRequest)
	router.GET("/v1/allocation/:allocID", route.AllocSpecificRequest)
	router.GET("/v1/evaluations",route.EvalsRequest)
	router.GET("/v1/evaluation/:evalID/:type",route.EvalRequest)
	router.GET("/v1/agent/allocation/:tokens",route.ClientAllocRequest)
	router.GET("/v1/self",route.AgentSelfRequest)
	router.POST("/v1/join",route.AgentJoinRequest)
	router.POST("/v1/agent/force-leave",route.AgentForceLeaveRequest)
	router.GET("/v1/members",route.AgentMembersRequest)
	router.POST("/v1/managers",route.UpdateServers)
	router.GET("/v1/managers",route.ListServers)
	router.GET("/v1/regions",route.RegionListRequest)
	router.GET("/v1/leader",route.StatusLeaderRequest)
	router.GET("/v1/peers",route.StatusPeersRequest)
	router.POST("/v1/validate/job",route.ValidateJobRequest)
	router.GET("/v1/nodes", route.NodesRequest)
	router.GET("/v1/node/:nodeName/:type",route.NodeRequest)
	//router.POST("/v1/operator/",updupJob)
	/*router.POST("/v1/job/renewal",updupJob)
	router.POST("/v1/job/info",updupJob)
	*/
	go func() {
		err := http.ListenAndServe(d.config.ApiAddr, router)
		if err != nil {
			logger.Error("in SetupApiServer ListenAndServe", "err", err)
			// TODO mark plugin unhealthy
		}
	}()
	logger.Info("Setup api server succeeded", "addr", d.config.ApiAddr)

	d.apiServer = router
	return nil
}


func (d *Driver) PluginInfo() (*base.PluginInfoResponse, error) {
	return pluginInfo, nil
}

func (d *Driver) ConfigSchema() (*hclspec.Spec, error) {
	return configSpec, nil
}

type DriverConfig struct {
	NatsBind      string   `codec:"nats_bind"`
	NatsAdvertise string   `codec:"nats_advertise"`
	ApiAddr       string   `codec:"api_addr"`
	NomadAddr     string   `codec:"nomad_addr"`
	Consul        []string `codec:"consul"`
	DataDir       string   `codec:"data_dir"`
}

func (d *Driver) SetConfig(c *base.Config) (err error) {
	if c != nil && c.AgentConfig != nil {
		d.nomadConfig = c.AgentConfig.Driver
		d.logger.Info("SetConfig 1", "DriverConfig", c.AgentConfig.Driver)
	}

	var dconfig DriverConfig
	if len(c.PluginConfig) != 0 {
		if err := base.MsgPackDecode(c.PluginConfig, &dconfig); err != nil {
			return err
		}
	}

	d.config = &dconfig
	d.logger.Info("SetConfig 2", "config", d.config)

	if d.storeManager != nil {
		// PluginLoader.validatePluginConfig() will call SetConfig() twice.
		// This test avoids extra setup.
		return nil
	} else {
		d.storeManager, err = common.NewStoreManager(d.config.Consul)
		if err != nil {
			return err
		}

		go func() {
			if d.config.ApiAddr == "" {
				d.logger.Info("ApiAddr is empty in config. Will not start compat API server.")
			} else {
				apiErr := d.SetupApiServer(d.logger)
				if apiErr != nil {
					d.logger.Error("error in SetupApiServer", "err", err)
					// TODO mark driver unhealthy
				}
			}

			// Have to put this in a goroutine, or it will fail.
			err := d.SetupNatsServer(d.logger)
			if err != nil {
				d.logger.Error("error in SetupNatsServer", "err", err)
				// TODO mark driver unhealthy
			}

		}()
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
	attrs := map[string]*pstructs.Attribute{}

	health = drivers.HealthStateHealthy
	attrs[driverAttr] = pstructs.NewBoolAttribute(true)
	attrs[driverVersionAttr] = pstructs.NewStringAttribute(g.Version)
	attrs[driverFullVersionAttr] = pstructs.NewStringAttribute(
		fmt.Sprintf("%v-%v-%v", g.Version, g.GitBranch, g.GitCommit))

	return &drivers.Fingerprint{
		Attributes:        attrs,
		Health:            health,
		HealthDescription: drivers.DriverHealthy,
	}
}

func (d *Driver) RecoverTask(handle *drivers.TaskHandle) error {
	// See docker / raw_exec driver. 'Recover' means 'reattach'.
	// Nomad client has crashed but the container/process keeps running.
	// On nomad client restarting, it tries to reattach to running container/process.
	// A dtle task crashes with nomad client, so it is not recoverable.

	if handle == nil {
		return fmt.Errorf("handle cannot be nil")
	}

	// COMPAT(0.10): pre 0.9 upgrade path check
	/*if handle.Version == 0 {
		return d.recoverPre09Task(handle)
	}*/

	cfg := handle.Config
	d.logger.Info("RecoverTask", "ID", cfg.ID)

	// If already attached to handle there's nothing to recover.
	if _, ok := d.tasks.Get(cfg.ID); ok {
		d.logger.Debug("nothing to recover; task already exists", "task_id", cfg.ID, "task_name", cfg.Name)
		return nil
	}

	return fmt.Errorf("dtle task is not recoverable")
}

func (d *Driver) StartTask(cfg *drivers.TaskConfig) (*drivers.TaskHandle, *drivers.DriverNetwork, error) {
	d.logger.Info("StartTask", "ID", cfg.ID, "allocID", cfg.AllocID)

	err := common.ValidateJobName(cfg.JobName)
	if err != nil {
		return nil, nil, err
	}

	if _, ok := d.tasks.Get(cfg.ID); ok {
		return nil, nil, fmt.Errorf("task with ID %q already started", cfg.ID)
	}
	d.logger.Debug("start dtle task one")

	var dtleTaskConfig config.DtleTaskConfig

	if err := cfg.DecodeDriverConfig(&dtleTaskConfig); err != nil {
		return nil, nil, errors.Wrap(err, "DecodeDriverConfig")
	}

	if (dtleTaskConfig.ConnectionConfig == nil && dtleTaskConfig.KafkaConfig == nil) ||
		(dtleTaskConfig.ConnectionConfig != nil && dtleTaskConfig.KafkaConfig != nil) {
		return nil, nil, fmt.Errorf("one and only one of ConnectionConfig or KafkaConfig should be set")
	}

	handle := drivers.NewTaskHandle(taskHandleVersion)
	handle.Config = cfg
	h := newDtleTaskHandle(d.logger, cfg, drivers.TaskStateRunning, time.Now().Round(time.Millisecond))
	driverState := TaskState{
		TaskConfig:     cfg,
		DtleTaskConfig: &dtleTaskConfig,
		StartedAt:      time.Now().Round(time.Millisecond),
	}
	if err := handle.SetDriverState(&driverState); err != nil {
		d.logger.Error("failed to start task, error setting driver state", "error", err)
		return nil, nil, errors.Wrap(err, "SetDriverState")
	}
	d.tasks.Set(cfg.ID, h)
	go h.run(&dtleTaskConfig, d)

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

	select {
	case <-ctx.Done():
		return
	case <-d.ctx.Done():
		return
	case <-handle.ctx.Done():
		result := &drivers.ExitResult{
			ExitCode:  0,
			Signal:    0,
			OOMKilled: false,
			Err:       nil,
		}
		ch <- result
	}
}

func (d *Driver) StopTask(taskID string, timeout time.Duration, signal string) error {
	d.logger.Info("StopTask", "id", taskID, "signal", signal)
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}

	handle.Destroy()
	return nil
}

func (d *Driver) DestroyTask(taskID string, force bool) error {
	d.logger.Info("DestroyTask", "id", taskID)
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}
	if handle.IsRunning() && !force {
		return fmt.Errorf("cannot destroy running task")
	}
	handle.Destroy()

	err := d.storeManager.DestroyJob(handle.taskConfig.JobName)
	if err != nil {
		d.logger.Error("error at storeManager.DestroyJob", "err", err)
	}

	d.tasks.Delete(taskID)

	return nil
}

func (d *Driver) InspectTask(taskID string) (*drivers.TaskStatus, error) {
	d.logger.Info("InspectTask", "taskID", taskID)
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
						RSS:      0,
						Measured: []string{"RSS"},
					},
					CpuStats:    &drivers.CpuStats{
						SystemMode:       0,
						UserMode:         0,
						TotalTicks:       0,
						ThrottledPeriods: 0,
						ThrottledTime:    0,
						Percent:          0,
						Measured:         nil,
					},
					DeviceStats: nil,
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
	d.logger.Info("ExecTask", "id", taskID)
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

func (d *Driver) Shutdown() {
	d.signalShutdown()
}
