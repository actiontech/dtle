package mysql

import (
	"context"
	"fmt"
	dcommon "github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/pkg/errors"
	"runtime"
	"time"

	config "github.com/actiontech/dtle/drivers/mysql/mysql/config"
	"github.com/hashicorp/go-hclog"
	//	"github.com/actiontech/dtle/drivers/mysql/mysql"
	"github.com/hashicorp/nomad/drivers/shared/eventer"
	"github.com/hashicorp/nomad/helper/pluginutils/loader"
	"github.com/hashicorp/nomad/plugins/base"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/hashicorp/nomad/plugins/shared/hclspec"
	pstructs "github.com/hashicorp/nomad/plugins/shared/structs"
	gnatsd "github.com/nats-io/nats-server/v2/server"
	stand "github.com/nats-io/nats-streaming-server/server"
	"net"
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
	configSpec = hclspec.NewObject(map[string]*hclspec.Spec{
		"NatsBind": hclspec.NewDefault(hclspec.NewAttr("NatsBind", "string", false),
			hclspec.NewLiteral(`"0.0.0.0:8193"`)),
		"NatsAdvertise": hclspec.NewDefault(hclspec.NewAttr("NatsAdvertise", "string", false),
			hclspec.NewLiteral(`"127.0.0.1:8193"`)),
		"consul": hclspec.NewAttr("consul", "list(string)", true),
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
		"MsgBytesLimit":hclspec.NewAttr("MsgBytesLimit", "number", false),
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
		"NatsAddr":hclspec.NewAttr("NatsAddr", "string", false),
		"ParallelWorkers":hclspec.NewAttr("ParallelWorkers", "number", false),
		"SqlMode":hclspec.NewAttr("SqlMode", "string", false),
		"MySQLVersion":hclspec.NewAttr("MySQLVersion", "string", false),
		"SkipCreateDbTable":hclspec.NewAttr("SkipCreateDbTable", "bool", false),

		"SkipPrivilegeCheck":hclspec.NewAttr("SkipPrivilegeCheck", "bool", false),
		"SkipIncrementalCopy":hclspec.NewAttr("SkipIncrementalCopy", "bool", false),
		"ApproveHeterogeneous":hclspec.NewAttr("ApproveHeterogeneous", "bool", false),
		// TODO remove
		"type":       hclspec.NewAttr("type", "string", true),
		"ConnectionConfig": hclspec.NewBlock("ConnectionConfig", true, hclspec.NewObject(map[string]*hclspec.Spec{
			"Host": hclspec.NewAttr("Host", "string", true),
			"Port": hclspec.NewAttr("Port", "number", true),
			"User": hclspec.NewAttr("User", "string", true),
			"Password": hclspec.NewAttr("Password", "string", true),
			"Charset": hclspec.NewAttr("Charset", "string", false),
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
	TaskConfig     *drivers.TaskConfig
	DtleTaskConfig *DtleTaskConfig
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

	config *DriverConfig

	storeManager *dcommon.StoreManager
}

type DtleTaskConfig struct {
	ReplicateDoDb         []*config.DataSource `codec:"ReplicateDoDb"`
	ReplicateIgnoreDb     []*config.DataSource`codec:"ReplicateIgnoreDb"`
	DropTableIfExists     bool`codec:"DropTableIfExists"`
	ExpandSyntaxSupport   bool`codec:"ExpandSyntaxSupport"`
	ReplChanBufferSize    int64`codec:"ReplChanBufferSize"`
	MsgBytesLimit         int`codec:"MsgBytesLimit"`
	TrafficAgainstLimits  int`codec:"TrafficAgainstLimits"`
	MaxRetries            int64`codec:"MaxRetries"`
	ChunkSize             int64`codec:"ChunkSize"`
	SqlFilter             []string`codec:"SqlFilter"`
	GroupMaxSize          int`codec:"GroupMaxSize"`
	GroupTimeout          int `codec:"GroupTimeout"`
	Gtid              string`codec:"Gtid"`
	BinlogFile        string`codec:"BinlogFile"`
	BinlogPos         int64`codec:"BinlogPos"`
	GtidStart         string`codec:"GtidStart"`
	AutoGtid          bool`codec:"AutoGtid"`
	BinlogRelay       bool`codec:"BinlogRelay"`
	NatsAddr          string`codec:"NatsAddr"`
	ParallelWorkers   int`codec:"ParallelWorkers"`
	SystemVariables   map[string]string`codec:"SystemVariables"`
	HasSuperPrivilege bool`codec:"HasSuperPrivilege"`

	SqlMode           string`codec:"SqlMode"`
	MySQLVersion      string`codec:"MySQLVersion"`
	ApproveHeterogeneous bool`codec:"ApproveHeterogeneous"`
	SkipCreateDbTable    bool`codec:"SkipCreateDbTable"`
	SkipPrivilegeCheck  bool`codec:"SkipPrivilegeCheck"`
	SkipIncrementalCopy bool`codec:"SkipIncrementalCopy"`
	Type      string   `codec:"type"`
	ConnectionConfig *config.ConnectionConfig `codec:"ConnectionConfig"`
}

func NewDriver(logger hclog.Logger) drivers.DriverPlugin {
	logger = logger.Named(pluginName)
	logger.Info("mysql NewDriver")

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

type DriverConfig struct {
	NatsBind string `codec:"NatsBind"`
	NatsAdvertise string `codec:"NatsAdvertise"`
	Consul []string `codec:"consul"`
}

func (d *Driver) SetConfig(c *base.Config) (err error) {
	if c != nil && c.AgentConfig != nil {
		d.nomadConfig = c.AgentConfig.Driver
		d.logger.Info("SetConfig", "DriverConfig", c.AgentConfig.Driver)
	}

	var dconfig DriverConfig
	if len(c.PluginConfig) != 0 {
		if err := base.MsgPackDecode(c.PluginConfig, &dconfig); err != nil {
			return err
		}
	}

	d.config = &dconfig
	d.logger.Info("SetConfig", "config", d.config)

	d.storeManager, err = dcommon.NewStoreManager(d.config.Consul)
	if err != nil {
		return err
	}

	go func() {
		// Have to put this in a goroutine, or it will fail.
		err := d.SetupNatsServer(d.logger)
		if err != nil {
			d.logger.Error("error in SetupNatsServer", "err", err)
			// TODO mark driver unhealthy
		}
	}()

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

	health = drivers.HealthStateHealthy
	desc = "ready"
	attrs["driver.mysql"] = pstructs.NewBoolAttribute(true)
	attrs["driver.mysql.version"] = pstructs.NewStringAttribute("12")

	return &drivers.Fingerprint{
		Attributes:        attrs,
		Health:            health,
		HealthDescription: desc,
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

	if _, ok := d.tasks.Get(cfg.ID); ok {
		return nil, nil, fmt.Errorf("task with ID %q already started", cfg.ID)
	}
	d.logger.Debug("start dtle task one")

	var dtleTaskConfig DtleTaskConfig

	if err := cfg.DecodeDriverConfig(&dtleTaskConfig); err != nil {
		return nil, nil, errors.Wrap(err, "DecodeDriverConfig")
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
func InitConfig(taskConfig *DtleTaskConfig) (mysqlConfig *config.MySQLDriverConfig, err error) {
	mysqlConfig = &config.MySQLDriverConfig{}

	//var driverConfig config.MySQLDriverConfig
	mysqlConfig.ConnectionConfig = taskConfig.ConnectionConfig
	//mysqlConfig = taskConfig
	//driverConfig.ExpandSyntaxSupport =false
	if taskConfig.ReplChanBufferSize ==0{
		mysqlConfig.ReplChanBufferSize=600
	}
	mysqlConfig.ReplicateDoDb=taskConfig.ReplicateDoDb

	mysqlConfig.DropTableIfExists = taskConfig.DropTableIfExists
	mysqlConfig.SkipCreateDbTable = taskConfig.SkipCreateDbTable
	if  taskConfig.MySQLVersion==""{
		mysqlConfig.MySQLVersion = "5.7"
	}else{
		mysqlConfig.MySQLVersion = taskConfig.MySQLVersion
	}

	mysqlConfig.SkipCreateDbTable =  taskConfig.SkipPrivilegeCheck
    mysqlConfig.AutoGtid = taskConfig.AutoGtid
	mysqlConfig.SkipIncrementalCopy =  taskConfig.SkipIncrementalCopy
	mysqlConfig.Gtid = taskConfig.Gtid
	mysqlConfig.GtidStart = taskConfig.GtidStart
	mysqlConfig.BinlogRelay = taskConfig.BinlogRelay
	mysqlConfig.BinlogFile = taskConfig.BinlogFile
	mysqlConfig.BinlogPos = taskConfig.BinlogPos
	if taskConfig.ParallelWorkers==0{
		mysqlConfig.ParallelWorkers=1
	}else{
		mysqlConfig.ParallelWorkers = taskConfig.ParallelWorkers
	}
	if taskConfig.ChunkSize == 0{
		mysqlConfig.ChunkSize = 2000
	}else{
		mysqlConfig.ChunkSize = taskConfig.ChunkSize
	}

	mysqlConfig.ExpandSyntaxSupport = taskConfig.ExpandSyntaxSupport
	if taskConfig.MsgBytesLimit ==0{
		mysqlConfig.MsgBytesLimit = 20480
	}else{
		mysqlConfig.MsgBytesLimit = taskConfig.MsgBytesLimit
	}
	if taskConfig.GroupMaxSize	 ==0{
		mysqlConfig.GroupMaxSize =1
	}else{
		mysqlConfig.GroupMaxSize = taskConfig.GroupMaxSize
	}
	if taskConfig.GroupTimeout	 ==0{
		mysqlConfig.GroupTimeout =1
	}else{
		mysqlConfig.GroupTimeout = taskConfig.GroupTimeout
	}
	mysqlConfig.SqlFilter = taskConfig.SqlFilter
	mysqlConfig.SqlMode = taskConfig.SqlMode

	return mysqlConfig,nil
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
