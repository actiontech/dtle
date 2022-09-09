package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/actiontech/dtle/driver/common"
	"github.com/actiontech/dtle/g"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/drivers/shared/eventer"
	"github.com/hashicorp/nomad/plugins/base"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/hashicorp/nomad/plugins/shared/hclspec"
	pstructs "github.com/hashicorp/nomad/plugins/shared/structs"
	"github.com/julienschmidt/httprouter"
	gnatsd "github.com/nats-io/nats-server/v2/server"
	stand "github.com/nats-io/nats-streaming-server/server"
	"github.com/pkg/errors"

	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	// fingerprintPeriod is the interval at which the driver will send fingerprint responses
	fingerprintPeriod = 30 * time.Second

	driverAttr            = "driver.dtle"
	driverVersionAttr     = "driver.dtle.version"
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
		"consul": hclspec.NewDefault(hclspec.NewAttr("consul", "string", false),
			hclspec.NewLiteral(`"127.0.0.1:8500"`)),
		"data_dir": hclspec.NewDefault(hclspec.NewAttr("data_dir", "string", false),
			hclspec.NewLiteral(`"/var/lib/nomad"`)),
		"stats_collection_interval": hclspec.NewDefault(hclspec.NewAttr("stats_collection_interval", "number", false),
			hclspec.NewLiteral(`15`)),
		"publish_metrics": hclspec.NewDefault(hclspec.NewAttr("publish_metrics", "bool", false),
			hclspec.NewLiteral(`false`)),
		"log_level": hclspec.NewDefault(hclspec.NewAttr("log_level", "string", false),
			hclspec.NewLiteral(`"Info"`)),
		"ui_dir": hclspec.NewDefault(hclspec.NewAttr("ui_dir", "string", false),
			hclspec.NewLiteral(`""`)),
		"rsa_private_key_path": hclspec.NewDefault(hclspec.NewAttr("rsa_private_key_path", "string", false),
			hclspec.NewLiteral(`""`)),
		"cert_file_path": hclspec.NewDefault(hclspec.NewAttr("cert_file_path", "string", false),
			hclspec.NewLiteral(`""`)),
		"key_file_path": hclspec.NewDefault(hclspec.NewAttr("key_file_path", "string", false),
			hclspec.NewLiteral(`""`)),
		"memory":          hclspec.NewAttr("memory", "string", false),
		"big_tx_max_jobs": hclspec.NewAttr("big_tx_max_jobs", "number", false),
		"log_file": hclspec.NewDefault(hclspec.NewAttr("log_file", "string", false),
			hclspec.NewLiteral(`"/var/log/dtle"`)),
	})

	// taskConfigSpec is the hcl specification for the driver config section of
	// a taskConfig within a job. It is returned in the TaskConfigSchema RPC
	taskConfigSpec = hclspec.NewObject(map[string]*hclspec.Spec{
		"ReplicateDoDb": hclspec.NewBlockList("ReplicateDoDb", hclspec.NewObject(map[string]*hclspec.Spec{
			"TableSchema":            hclspec.NewAttr("TableSchema", "string", false),
			"TableSchemaRegex":       hclspec.NewAttr("TableSchemaRegex", "string", false),
			"TableSchemaRenameRegex": hclspec.NewAttr("TableSchemaRenameRegex", "string", false),
			"TableSchemaRename":      hclspec.NewAttr("TableSchemaRename", "string", false),
			"TableSchemaScope":       hclspec.NewAttr("TableSchemaScope", "string", false),
			"Tables": hclspec.NewBlockList("Tables", hclspec.NewObject(map[string]*hclspec.Spec{
				"TableName":         hclspec.NewAttr("TableName", "string", false),
				"TableRegex":        hclspec.NewAttr("TableRegex", "string", false),
				"TableRename":       hclspec.NewAttr("TableRename", "string", false),
				"TableRenameRegex":  hclspec.NewAttr("TableRenameRegex", "string", false),
				"TableSchema":       hclspec.NewAttr("TableSchema", "string", false),
				"TableSchemaRename": hclspec.NewAttr("TableSchemaRename", "string", false),
				"Where":             hclspec.NewAttr("Where", "string", false),
				"ColumnMapFrom":     hclspec.NewAttr("ColumnMapFrom", "list(string)", false),
				"ColumnMapTo":       hclspec.NewAttr("ColumnMapTo", "list(string)", false),
			})),
		})),
		"ReplicateIgnoreDb": hclspec.NewBlockList("ReplicateIgnoreDb", hclspec.NewObject(map[string]*hclspec.Spec{
			"TableSchema": hclspec.NewAttr("TableSchema", "string", false),
			"Tables": hclspec.NewBlockList("Tables", hclspec.NewObject(map[string]*hclspec.Spec{
				"TableName":   hclspec.NewAttr("TableName", "string", false),
				"TableSchema": hclspec.NewAttr("TableSchema", "string", false),
			})),
		})),
		"DropTableIfExists":    hclspec.NewAttr("DropTableIfExists", "bool", false),
		"ExpandSyntaxSupport":  hclspec.NewAttr("ExpandSyntaxSupport", "bool", false),
		"ReplChanBufferSize":   hclspec.NewAttr("ReplChanBufferSize", "number", false),
		"TrafficAgainstLimits": hclspec.NewAttr("TrafficAgainstLimits", "number", false),
		"MaxRetries":           hclspec.NewAttr("MaxRetries", "number", false),
		"ChunkSize":            hclspec.NewAttr("ChunkSize", "number", false),
		"SqlFilter":            hclspec.NewAttr("SqlFilter", "list(string)", false),
		"GroupMaxSize":         hclspec.NewAttr("GroupMaxSize", "number", false),
		"GroupTimeout":         hclspec.NewAttr("GroupTimeout", "number", false),
		"Gtid":                 hclspec.NewAttr("Gtid", "string", false),
		"BinlogFile":           hclspec.NewAttr("BinlogFile", "string", false),
		"BinlogPos":            hclspec.NewAttr("BinlogPos", "number", false),
		"GtidStart":            hclspec.NewAttr("GtidStart", "string", false),
		"AutoGtid":             hclspec.NewAttr("AutoGtid", "bool", false),
		"BinlogRelay":          hclspec.NewAttr("BinlogRelay", "bool", false),
		"ParallelWorkers":      hclspec.NewAttr("ParallelWorkers", "number", false),
		"SkipCreateDbTable":    hclspec.NewAttr("SkipCreateDbTable", "bool", false),
		"SkipPrivilegeCheck":   hclspec.NewAttr("SkipPrivilegeCheck", "bool", false),
		"SkipIncrementalCopy":  hclspec.NewAttr("SkipIncrementalCopy", "bool", false),
		"SlaveNetWriteTimeout": hclspec.NewDefault(hclspec.NewAttr("SlaveNetWriteTimeout", "number", false),
			hclspec.NewLiteral(`28800`)), // 8 hours
		"ConnectionConfig": hclspec.NewBlock("ConnectionConfig", false, hclspec.NewObject(map[string]*hclspec.Spec{
			"Host":     hclspec.NewAttr("Host", "string", true),
			"Port":     hclspec.NewAttr("Port", "number", true),
			"User":     hclspec.NewAttr("User", "string", true),
			"Password": hclspec.NewAttr("Password", "string", true),
			"Charset": hclspec.NewDefault(hclspec.NewAttr("Charset", "string", false),
				hclspec.NewLiteral(`"utf8mb4"`)),
		})),
		"WaitOnJob": hclspec.NewAttr("WaitOnJob", "string", false),
		"BulkInsert1": hclspec.NewDefault(hclspec.NewAttr("BulkInsert1", "number", false),
			hclspec.NewLiteral(`4`)),
		"BulkInsert2": hclspec.NewDefault(hclspec.NewAttr("BulkInsert2", "number", false),
			hclspec.NewLiteral(`8`)),
		"BulkInsert3": hclspec.NewDefault(hclspec.NewAttr("BulkInsert3", "number", false),
			hclspec.NewLiteral(`64`)),
		"BigTxSrcQueue": hclspec.NewDefault(hclspec.NewAttr("BigTxSrcQueue", "number", false),
			hclspec.NewLiteral(`3`)),
		"KafkaConfig": hclspec.NewBlock("KafkaConfig", false, hclspec.NewObject(map[string]*hclspec.Spec{
			"Topic":   hclspec.NewAttr("Topic", "string", true),
			"Brokers": hclspec.NewAttr("Brokers", "list(string)", true),
			"Converter": hclspec.NewDefault(hclspec.NewAttr("Converter", "string", false),
				hclspec.NewLiteral(`"json"`)),
			"TimeZone": hclspec.NewDefault(hclspec.NewAttr("TimeZone", "string", false),
				hclspec.NewLiteral(`"UTC"`)),
			"MessageGroupMaxSize": hclspec.NewAttr("MessageGroupMaxSize", "number", false),
			"MessageGroupTimeout": hclspec.NewAttr("MessageGroupTimeout", "number", false),
			"TopicWithSchemaTable": hclspec.NewDefault(hclspec.NewAttr("TopicWithSchemaTable", "bool", false),
				hclspec.NewLiteral(`true`)),
			"SchemaChangeTopic": hclspec.NewAttr("SchemaChangeTopic", "string", false),
			"User":              hclspec.NewAttr("User", "string", false),
			"Password":          hclspec.NewAttr("SchemaChangeTopic", "string", false),
		})),
		// Since each job has its own history, this should be smaller than MySQL default (25000).
		"DependencyHistorySize": hclspec.NewDefault(hclspec.NewAttr("DependencyHistorySize", "number", false),
			hclspec.NewLiteral(`2500`)),
		"UseMySQLDependency": hclspec.NewDefault(hclspec.NewAttr("UseMySQLDependency", "bool", false),
			hclspec.NewLiteral(`true`)),
		"ForeignKeyChecks": hclspec.NewDefault(hclspec.NewAttr("ForeignKeyChecks", "bool", false),
			hclspec.NewLiteral(`true`)),
		"DumpEntryLimit": hclspec.NewDefault(hclspec.NewAttr("DumpEntryLimit", "number", false),
			hclspec.NewLiteral(`67108864`)),
		"SetGtidNext": hclspec.NewDefault(hclspec.NewAttr("SetGtidNext", "bool", false),
			hclspec.NewLiteral(`false`)),
		"OracleConfig": hclspec.NewBlock("OracleConfig", false, hclspec.NewObject(map[string]*hclspec.Spec{
			"ServiceName": hclspec.NewAttr("ServiceName", "string", true),
			"Host":        hclspec.NewAttr("Host", "string", true),
			"Port":        hclspec.NewAttr("Port", "number", true),
			"User":        hclspec.NewAttr("User", "string", true),
			"Password":    hclspec.NewAttr("Password", "string", true),
			"Scn":         hclspec.NewAttr("Scn", "number", true),
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
	DtleTaskConfig *common.DtleTaskConfig
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
	logger g.LoggerType

	stand            *stand.StanServer
	apiServer        *httprouter.Router
	setupApiServerFn func(logger g.LoggerType, driverConfig *DriverConfig) error

	config *DriverConfig

	storeManager *common.StoreManager
}

func NewDriver(logger g.LoggerType) *Driver {
	logger = logger.Named(g.PluginName)
	logger.Info("dtle NewDriver")

	ctx, cancel := context.WithCancel(context.Background())
	AllocIdTaskNameToTaskHandler = newTaskStoreForApi()

	go g.FreeMemoryWorker()
	go g.MemoryMonitor(logger)

	return &Driver{
		eventer:        eventer.NewEventer(ctx, logger),
		tasks:          newTaskStore(),
		ctx:            ctx,
		signalShutdown: cancel,
		logger:         logger,
	}
}

func (d *Driver) SetupNatsServer(logger g.LoggerType) (err error) {
	natsAddr, err := net.ResolveTCPAddr("tcp", d.config.NatsBind)
	if err != nil {
		return fmt.Errorf("failed to parse Nats address. addr %v err %v",
			d.config.NatsBind, err)
	}
	nOpts := gnatsd.Options{
		Host:       natsAddr.IP.String(),
		Port:       natsAddr.Port,
		MaxPayload: g.NatsMaxPayload,
		//HTTPPort:   8199,
		LogFile: "/opt/log",
		Debug:   true,
	}
	//logger.Debug("Starting nats streaming server", "addr", natsAddr)
	sOpts := stand.GetDefaultOptions()
	sOpts.ID = common.DefaultClusterID
	s, err := stand.RunServerWithOpts(sOpts, &nOpts)
	if err != nil {
		return err
	}

	logger.Info("Setup nats server", "addr", d.config.NatsBind)

	d.stand = s
	return nil
}

func (d *Driver) PluginInfo() (*base.PluginInfoResponse, error) {
	return pluginInfo, nil
}

func (d *Driver) ConfigSchema() (*hclspec.Spec, error) {
	return configSpec, nil
}

type DriverConfig struct {
	NatsBind                string `codec:"nats_bind"`
	NatsAdvertise           string `codec:"nats_advertise"`
	BigTxMaxJobs            int32  `codec:"big_tx_max_jobs"`
	ApiAddr                 string `codec:"api_addr"`
	NomadAddr               string `codec:"nomad_addr"`
	Consul                  string `codec:"consul"`
	DataDir                 string `codec:"data_dir"`
	StatsCollectionInterval int    `codec:"stats_collection_interval"`
	PublishMetrics          bool   `codec:"publish_metrics"`
	LogLevel                string `codec:"log_level"`
	LogFile                 string `codec:"log_file"`
	UiDir                   string `codec:"ui_dir"`
	RsaPrivateKeyPath       string `codec:"rsa_private_key_path"`
	CertFilePath            string `codec:"cert_file_path"`
	KeyFilePath             string `codec:"key_file_path"`
	Memory                  string `codec:"memory"`
}

func (d *Driver) setupLogger() (err error) {
	// set log level for nomad logger
	err = d.SetLogLevel(d.config.LogLevel)
	if err != nil {
		return err
	}

	if d.config.LogFile == "" {
		d.logger.Info("use nomad logger", "level", d.config.LogLevel)
	} else {
		err = os.MkdirAll(filepath.Dir(d.config.LogFile), 0755)
		if err != nil {
			return err
		}

		logFileName := d.config.LogFile
		if strings.HasSuffix(logFileName, "/") {
			logFileName += "dtle.log"
		}

		rotateFile := &lumberjack.Logger{
			Filename: logFileName,
			MaxSize:  512, // MB
			Compress: true,
		}

		d.logger.Info("switching to dtle logger", "file", d.config.LogFile, "level", d.config.LogLevel)

		d.logger = hclog.New(&hclog.LoggerOptions{
			Name:   "",
			Level:  hclog.Info,
			Output: rotateFile,
		})
		g.Logger = d.logger

		err = d.SetLogLevel(d.config.LogLevel)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Driver) SetConfig(c *base.Config) (err error) {
	if c != nil && c.AgentConfig != nil {
		d.nomadConfig = c.AgentConfig.Driver
		d.logger.Info("SetConfig 1", "DriverConfig", c.AgentConfig.Driver)
	}

	d.config = new(DriverConfig)
	if len(c.PluginConfig) != 0 {
		if err := base.MsgPackDecode(c.PluginConfig, d.config); err != nil {
			return err
		}
	}

	d.logger.Info("SetConfig 2", "config", d.config)

	err = d.setupLogger()
	if err != nil {
		return errors.Wrap(err, "setupLogger")
	}

	//if dconfig.Memory != "" {
	//	g.MemAvailable = TODO
	//}
	if d.config.BigTxMaxJobs != 0 {
		g.BigTxMaxJobs = d.config.BigTxMaxJobs
	}
	d.logger.Info("BigTxMaxJobs is set", "value", g.BigTxMaxJobs)

	if "" != d.config.RsaPrivateKeyPath {
		b, err := ioutil.ReadFile(d.config.RsaPrivateKeyPath)
		if nil != err {
			return fmt.Errorf("read rsa private key file failed: %v", err)
		}
		g.RsaPrivateKey = string(b)
	}

	if d.storeManager != nil {
		// PluginLoader.validatePluginConfig() will call SetConfig() twice.
		// This test avoids extra setup.
		return nil
	} else {
		d.storeManager, err = common.NewStoreManager([]string{d.config.Consul}, d.logger)
		if err != nil {
			return err
		}

		go func() {
			if d.config.ApiAddr == "" {
				d.logger.Info("ApiAddr is empty in config. Will not start api_compat server.")
			} else {
				if d.config.NomadAddr == "" {
					d.logger.Info("NomadAddr is empty in config. Will not handle api_compat request.")
					go func() {
						// for pprof
						err := http.ListenAndServe(d.config.ApiAddr, http.DefaultServeMux)
						if err != nil {
							d.logger.Error("ListenAndServe error", "err", err, "addr", d.config.ApiAddr)
						}
					}()
				} else {
					apiErr := d.setupApiServerFn(d.logger, d.config)
					if apiErr != nil {
						d.logger.Error("error in SetupApiServer", "err", err,
							"apiAddr", d.config.ApiAddr, "nomadAddr", d.config.NomadAddr)
						// TODO mark driver unhealthy
					}
				}
			}

			// Have to put this in a goroutine, or it will fail.
			err := d.SetupNatsServer(d.logger)
			if err != nil {
				d.logger.Error("error in SetupNatsServer", "err", err, "natsAddr", d.config.NatsBind)
				// TODO mark driver unhealthy
			}
			// clean relay binlog data
			go d.loopCleanRelayDir()
		}()
	}
	return nil
}

func (d *Driver) loopCleanRelayDir() {
	cleanDataDir := func() {
		files, err := ioutil.ReadDir(path.Join(d.config.DataDir, "binlog"))
		if err != nil {
			d.logger.Error("read dir failed", "dataDir", d.config.DataDir, "err", err)
			return
		}

		for _, file := range files {
			_, exist, err := d.storeManager.GetNatsIfExist(file.Name())
			if exist || err != nil {
				continue
			}
			if err := os.RemoveAll(path.Join(d.config.DataDir, "binlog", file.Name())); err != nil {
				d.logger.Warn("remove dir failed", "err", err)
			}
		}
	}

	cleanDuration := 12 * time.Hour
	cleanDelay := time.NewTimer(cleanDuration)
	defer cleanDelay.Stop()
	for {
		cleanDelay.Reset(cleanDuration)
		<-cleanDelay.C
		cleanDataDir()
	}
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

	return fmt.Errorf("dtle task is not recoverable. intended. NOT an error")
}

func (d *Driver) StartTask(cfg *drivers.TaskConfig) (*drivers.TaskHandle, *drivers.DriverNetwork, error) {
	d.logger.Info("StartTask", "ID", cfg.ID, "allocID", cfg.AllocID, "jobName", cfg.JobName, "taskName", cfg.Name)

	err := common.ValidateJobName(cfg.JobName)
	if err != nil {
		return nil, nil, err
	}

	if _, ok := d.tasks.Get(cfg.ID); ok {
		return nil, nil, fmt.Errorf("task with ID %q already started", cfg.ID)
	}
	d.logger.Debug("start dtle task 1")
	var dtleTaskConfig common.DtleTaskConfig

	if err := cfg.DecodeDriverConfig(&dtleTaskConfig); err != nil {
		d.logger.Debug("start dtle task err")
		return nil, nil, errors.Wrap(err, "DecodeDriverConfig")
	}

	if err := d.verifyDriverConfig(dtleTaskConfig); nil != err {
		return nil, nil, fmt.Errorf("invalide driver config, errors: %v", err)
	}
	dtleTaskConfig.SetDefaultForEmpty()

	handle := drivers.NewTaskHandle(taskHandleVersion)
	handle.Config = cfg
	driverState := TaskState{
		TaskConfig:     cfg,
		DtleTaskConfig: &dtleTaskConfig,
		StartedAt:      time.Now().Round(time.Millisecond),
	}
	if err := handle.SetDriverState(&driverState); err != nil {
		d.logger.Error("failed to start task, error setting driver state", "error", err)
		return nil, nil, errors.Wrap(err, "SetDriverState")
	}

	h := newDtleTaskHandle(d.ctx, d.logger, cfg, drivers.TaskStateRunning, time.Now().Round(time.Millisecond))
	h.driverConfig = &common.MySQLDriverConfig{DtleTaskConfig: dtleTaskConfig}
	d.tasks.Set(cfg.ID, h)
	AllocIdTaskNameToTaskHandler.Set(cfg.AllocID, cfg.Name, cfg.ID, h)

	go h.run(d)

	return handle, nil, nil
}

func (d *Driver) verifyDriverConfig(config common.DtleTaskConfig) error {
	errMsgs := []string{}
	addErrMsgs := func(msg string) {
		errMsgs = append(errMsgs, fmt.Sprintf("	* %v", msg))
	}

	if !(1 <= config.BulkInsert1 && config.BulkInsert1 <= config.BulkInsert2) {
		return fmt.Errorf("expect 1 <= BulkInsert1 <= BulkInsert2. %v %v", config.BulkInsert1, config.BulkInsert2)
	}

	if config.ConnectionConfig != nil && config.OracleConfig != nil {
		addErrMsgs("only one src connection config should be set")
	}
	if config.ConnectionConfig != nil && config.KafkaConfig != nil {
		addErrMsgs("only one dest connection config should be set")
	}

	for _, doDb := range config.ReplicateDoDb {
		if doDb.TableSchema == "" && doDb.TableSchemaRegex == "" {
			addErrMsgs("TableSchema and TableSchemaRegex in ReplicateDoDb cannot both be blank")
		}
		if doDb.TableSchema != "" && doDb.TableSchemaRegex != "" {
			addErrMsgs(fmt.Sprintf("TableSchema and TableSchemaRegex in ReplicateDoDb cannot both be used. TableSchema=%v, TableSchemaRegex=%v", doDb.TableSchema, doDb.TableSchemaRegex))
		}
		if doDb.TableSchemaRegex != "" && doDb.TableSchemaRename == "" {
			addErrMsgs(fmt.Sprintf("TableSchemaRename in ReplicateDoDb is required while using TableSchemaRegex in ReplicateDoDb. TableSchemaRegex=%v", doDb.TableSchemaRegex))
		}

		for _, doTb := range doDb.Tables {
			if len(doTb.ColumnMapTo) != 0 {
				if len(doTb.ColumnMapTo) != len(doTb.ColumnMapFrom) {
					addErrMsgs("ColumnMapTo should be either empty or the same cardinality as ColumnMapFrom")
				}
			}
			if doTb.TableName == "" && doTb.TableRegex == "" {
				addErrMsgs("TableName and TableRegex in ReplicateDoDb cannot both be empty")
			}
			if doTb.TableName != "" && doTb.TableRegex != "" {
				addErrMsgs(fmt.Sprintf("TableName and TableRegex in ReplicateDoDb cannot both be used. TableSchema=%v, TableName=%v, TableRegex=%v", doDb.TableSchema, doTb.TableName, doTb.TableRegex))
			}
			if doTb.TableRegex != "" && doTb.TableRename == "" {
				addErrMsgs(fmt.Sprintf("TableRename in ReplicateDoDb is required while using TableRegex in ReplicateDoDb. TableSchema=%v, TableRegex=%v", doDb.TableSchema, doTb.TableRegex))
			}
		}
	}

	for _, db := range config.ReplicateIgnoreDb {
		if db.TableSchema == "" {
			addErrMsgs("TableSchema in ReplicateIgnoreDb should not be empty")
		}
		for _, tb := range db.Tables {
			if tb.TableName == "" {
				addErrMsgs(fmt.Sprintf("TableName in ReplicateIgnoreDb should not be empty. TableSchema=%v", db.TableSchema))
			}
		}
	}

	if len(errMsgs) > 0 {
		return fmt.Errorf("\n%v", strings.Join(errMsgs, "\n"))
	} else {
		return nil
	}
}

func (d *Driver) WaitTask(ctx context.Context, taskID string) (<-chan *drivers.ExitResult, error) {
	d.logger.Info("WaitTask", "taskID", taskID)
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
		ch <- handle.GetExitResult()
	}
}

// StopTask will not be called if the task has already exited (e.g. onError).
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

	d.tasks.Delete(taskID)
	AllocIdTaskNameToTaskHandler.Delete(taskID)

	return nil
}

func (d *Driver) InspectTask(taskID string) (*drivers.TaskStatus, error) {
	d.logger.Info("InspectTask", "taskID", taskID)
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}

	return handle.TaskStatus()
}

func (d *Driver) TaskStats(ctx context.Context, taskID string, interval time.Duration) (<-chan *drivers.TaskResourceUsage, error) {
	ch := make(chan *drivers.TaskResourceUsage)
	go d.handleStats(ctx, interval, ch)
	return ch, nil
}

func (d *Driver) handleStats(ctx context.Context, interval time.Duration, ch chan *drivers.TaskResourceUsage) {
	defer close(ch)
	timer := time.NewTimer(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			timer.Reset(interval)
		}

		s := &drivers.TaskResourceUsage{
			ResourceUsage: &drivers.ResourceUsage{
				MemoryStats: &drivers.MemoryStats{
					RSS:      0,
					Measured: []string{"RSS"},
				},
				CpuStats: &drivers.CpuStats{
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
		}
	}
}

func (d *Driver) TaskEvents(ctx context.Context) (<-chan *drivers.TaskEvent, error) {
	return d.eventer.TaskEvents(ctx)
}

func (d *Driver) SignalTask(taskID string, signal string) error {
	d.logger.Debug("SignalTask", "taskID", taskID, "signal", signal)
	// SignalTask: driver=dtle @module=dtle pid=72685 signal=SIGKILL taskID=37c60a2d-b7c0-37ff-787b-fdb98e921e92/Src/51b71e84

	h, ok := d.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}

	err := d.eventer.EmitEvent(&drivers.TaskEvent{
		TaskID:      taskID,
		TaskName:    h.taskConfig.Name,
		AllocID:     h.taskConfig.AllocID,
		Timestamp:   time.Now(),
		Message:     fmt.Sprintf("Task receive signal %v", signal),
		Annotations: nil,
		Err:         nil,
	})
	if err != nil {
		d.logger.Error("error at sending task event", "err", err, "signal", signal)
	}
	//if h.exitResult == nil {
	//	return nil
	//}

	switch signal {
	case "stats":
		if h.stats != nil {
			bs, err := json.Marshal(h.stats)
			if err != nil {
				return err
			}
			return errors.New(string(bs))
		}
	case "finish":
		if h.runner == nil {
			return fmt.Errorf("h.runner is nil")
		}
		return h.runner.Finish1()
	case "pause":
		d.logger.Info("pause a task", "taskID", taskID)
		if h.runner == nil {
			return fmt.Errorf("h.runner is nil")
		}
		err := h.runner.Shutdown()
		if err != nil {
			d.logger.Error("error when pausing a task", "taskID", taskID, "err", err)
		}
		// Keep old runner for stats()
		//h.runner = nil
		return nil
	case "resume":
		d.logger.Info("resume a task", "taskID", taskID)
		err := h.resumeTask(d)
		if err != nil {
			d.logger.Error("error when resuming a task", "taskID", taskID, "err", err)
			h.onError(err)
		}
		return nil
	default:
		return nil
	}

	return fmt.Errorf("DTLE_BUG SignalTask. should not reach here")
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
	d.logger.Info("Driver.Shutdown")
	d.signalShutdown()
}

func (d *Driver) SetLogLevel(level string) error {
	logLevel := hclog.LevelFromString(level)
	if logLevel == hclog.NoLevel {
		return fmt.Errorf("log level should be TRACE, DEBUG or INFO, got %v", level)
	}
	d.logger.SetLevel(logLevel)

	return nil
}

func (d *Driver) SetSetupApiServerFn(fn func(logger g.LoggerType, driverConfig *DriverConfig) (err error)) {
	d.setupApiServerFn = fn
}
