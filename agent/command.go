package agent

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/armon/go-metrics"
	"github.com/armon/go-metrics/datadog"
	"github.com/hashicorp/logutils"
	"github.com/mitchellh/cli"
)

// gracefulTimeout controls how long we wait before forcefully terminating
const gracefulTimeout = 5 * time.Second

// Command is a Command implementation that runs a Udup agent.
// The command will not end unless a shutdown message is sent on the
// ShutdownCh. If two messages are sent on the ShutdownCh it will forcibly
// exit.
type Command struct {
	Version    string
	Ui         cli.Ui
	ShutdownCh <-chan struct{}

	args           []string
	agent          *Agent
	httpServer     *HTTPServer
	logger         *log.Logger
	logFilter      *logutils.LevelFilter
	logOutput      io.Writer
	retryJoinErrCh chan struct{}
}

func (c *Command) readConfig() *Config {
	var configPath []string
	var servers string

	// Make a new, empty config.
	cmdConfig := &Config{
		Client: &ClientConfig{},
		Ports:  &Ports{},
		Server: &ServerConfig{},
	}

	flags := flag.NewFlagSet("agent", flag.ContinueOnError)
	flags.Usage = func() { c.Ui.Error(c.Help()) }

	// Role options
	flags.BoolVar(&cmdConfig.Server.Enabled, "server", true, "")
	flags.BoolVar(&cmdConfig.Client.Enabled, "client", true, "")

	// Server-only options
	flags.IntVar(&cmdConfig.Server.BootstrapExpect, "bootstrap-expect", 0, "")
	flags.Var((*StringFlag)(&cmdConfig.Server.StartJoin), "join", "")
	flags.Var((*StringFlag)(&cmdConfig.Server.RetryJoin), "retry-join", "")
	flags.IntVar(&cmdConfig.Server.RetryMaxAttempts, "retry-max", 0, "")
	flags.StringVar(&cmdConfig.Server.RetryInterval, "retry-interval", "", "")

	// Client-only options
	flags.StringVar(&cmdConfig.Client.StateDir, "state-dir", "", "")
	flags.StringVar(&servers, "servers", "", "")

	// General options
	flags.Var((*StringFlag)(&configPath), "config", "config")
	flags.StringVar(&cmdConfig.BindAddr, "bind", "", "")
	flags.StringVar(&cmdConfig.Region, "region", "", "")
	flags.StringVar(&cmdConfig.DataDir, "data-dir", "", "")
	flags.StringVar(&cmdConfig.Datacenter, "dc", "", "")
	flags.StringVar(&cmdConfig.LogLevel, "log-level", "", "")
	flags.StringVar(&cmdConfig.NodeName, "node", "", "")

	if err := flags.Parse(c.args); err != nil {
		return nil
	}

	// Split the servers.
	if servers != "" {
		cmdConfig.Client.Servers = strings.Split(servers, ",")
	}

	// Load the configuration
	var config *Config
	config = DefaultConfig()
	for _, path := range configPath {
		current, err := LoadConfig(path)
		if err != nil {
			c.logger.Printf("[ERR] Error loading configuration from %s: %s", path, err)
			return nil
		}

		if config == nil {
			config = current
		} else {
			config = config.Merge(current)
		}
	}

	// Ensure the sub-structs at least exist
	if config.Client == nil {
		config.Client = &ClientConfig{}
	}
	if config.Server == nil {
		config.Server = &ServerConfig{}
	}

	// Merge any CLI options over config file options
	config = config.Merge(cmdConfig)

	// Set the version info
	config.Version = c.Version

	// Normalize binds, ports, addresses, and advertise
	if err := config.normalizeAddrs(); err != nil {
		log.Fatal(err.Error())
		return nil
	}

	// Parse the RetryInterval.
	dur, err := time.ParseDuration(config.Server.RetryInterval)
	if err != nil {
		c.logger.Printf("[ERR] Error parsing retry interval: %s", err)
		return nil
	}
	config.Server.retryInterval = dur

	// Check that the server is running in at least one mode.
	if !(config.Server.Enabled || config.Client.Enabled) {
		c.logger.Printf("[ERR] Must specify either server, client or dev mode for the agent.")
		return nil
	}

	// Verify the paths are absolute.
	dirs := map[string]string{
		"data-dir":  config.DataDir,
		"state-dir": config.Client.StateDir,
	}
	for k, dir := range dirs {
		if dir == "" {
			continue
		}

		if !filepath.IsAbs(dir) {
			c.logger.Printf("[ERR] %s must be given as an absolute path: got %v", k, dir)
			return nil
		}
	}

	// Ensure that we have the directories we neet to run.
	if config.Server.Enabled && config.DataDir == "" {
		c.logger.Printf("[ERR] Must specify data directory")
		return nil
	}

	// The config is valid if the top-level data-dir is set or if both
	// alloc-dir and store-dir are set.
	if config.Client.Enabled && config.DataDir == "" {
		if config.Client.StateDir == "" {
			c.logger.Printf("[ERR] Must specify the state dir if data-dir is omitted.")
			return nil
		}
	}

	// Check the bootstrap flags
	if config.Server.BootstrapExpect > 0 && !config.Server.Enabled {
		c.logger.Printf("[ERR] Bootstrap requires server mode to be enabled")
		return nil
	}
	if config.Server.BootstrapExpect == 1 {
		c.logger.Printf("[WARN] Bootstrap mode enabled! Potentially unsafe operation.")
	}

	return config
}

// StringFlag implements the flag.Value interface and allows multiple
// calls to the same variable to append a list.
type StringFlag []string

func (s *StringFlag) String() string {
	return strings.Join(*s, ",")
}

func (s *StringFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// setupLoggers is used to setup the logGate, logWriter, and our logOutput
func (c *Command) setupLoggers(config *Config) io.Writer {
	c.logFilter = LevelFilter()
	c.logFilter.MinLevel = logutils.LogLevel(strings.ToUpper(config.LogLevel))
	if !ValidateLevelFilter(c.logFilter.MinLevel, c.logFilter) {
		c.logger.Printf("[ERR] Invalid log level: %s. Valid log levels are: %v",
			c.logFilter.MinLevel, c.logFilter.Levels)
		return nil
	}

	var oFile *os.File
	if config.LogFile != "" {
		if _, err := os.Stat(config.LogFile); os.IsNotExist(err) {
			if oFile, err = os.Create(config.LogFile); err != nil {
				c.logger.Printf("[ERR] Unable to create %s (%s), using stderr",
					config.LogFile, err)
				oFile = os.Stderr
			}
		} else {
			if oFile, err = os.OpenFile(config.LogFile, os.O_APPEND|os.O_WRONLY, os.ModeAppend); err != nil {
				c.logger.Printf("[ERR] Unable to append to %s (%s), using stderr",
					config.LogFile, err)
				oFile = os.Stderr
			}
		}
	} else {
		oFile = os.Stderr
	}

	c.logOutput = oFile
	c.logger = log.New(c.logOutput, "", log.LstdFlags|log.Lmicroseconds)
	log.SetOutput(oFile)
	return oFile
}

// setupAgent is used to start the agent and various interfaces
func (c *Command) setupAgent(config *Config, logOutput io.Writer) error {
	c.logger.Printf("[INFO] Starting Udup agent...")
	agent, err := NewAgent(config, logOutput)
	if err != nil {
		c.logger.Printf("[ERR] Error starting agent: %s", err)
		return err
	}
	c.agent = agent

	// Setup the HTTP server
	http, err := NewHTTPServer(agent, config, logOutput)
	if err != nil {
		agent.Shutdown()
		c.logger.Printf("[ERR] Error starting http server: %s", err)
		return err
	}
	c.httpServer = http

	return nil
}

func (c *Command) Run(args []string) int {
	c.logger = log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	// Parse our configs
	c.args = args
	config := c.readConfig()
	if config == nil {
		return 1
	}

	// Setup the log outputs
	logOutput := c.setupLoggers(config)

	// Log config files
	if len(config.Files) > 0 {
		c.logger.Printf("[INFO] Loaded configuration from %s", strings.Join(config.Files, ", "))
	} else {
		c.logger.Printf("[INFO] No configuration files loaded")
	}

	// Initialize the metric
	if err := c.setupMetric(config); err != nil {
		c.logger.Printf("[ERR] Error initializing metric: %s", err)
		return 1
	}

	// Create the agent
	if err := c.setupAgent(config, logOutput); err != nil {
		return 1
	}
	defer c.agent.Shutdown()

	defer func() {
		if c.httpServer != nil {
			c.httpServer.Shutdown()
		}
	}()

	// Join startup nodes if specified
	if err := c.startupJoin(config); err != nil {
		c.logger.Printf("[ERR] %v", err.Error())
		return 1
	}

	// Compile agent information for output later
	info := make(map[string]string)
	info["version"] = config.Version
	info["client"] = strconv.FormatBool(config.Client.Enabled)
	info["log level"] = config.LogLevel
	info["server"] = strconv.FormatBool(config.Server.Enabled)
	info["region"] = fmt.Sprintf("%s (DC: %s)", config.Region, config.Datacenter)

	// Sort the keys for output
	infoKeys := make([]string, 0, len(info))
	for key := range info {
		infoKeys = append(infoKeys, key)
	}
	sort.Strings(infoKeys)

	// Agent configuration output
	padding := 18
	c.logger.Printf("[INFO] Udup agent configuration:\n")
	for _, k := range infoKeys {
		c.logger.Printf(fmt.Sprintf(
			"[INFO] %s%s: %s",
			strings.Repeat(" ", padding-len(k)),
			strings.Title(k),
			info[k]))
	}
	// Output the header that the server has started
	c.logger.Printf("[INFO] Udup agent started! Log data will stream in below:\n")

	// Start retry join process
	c.retryJoinErrCh = make(chan struct{})
	go c.retryJoin(config)

	// Wait for exit
	return c.handleSignals(config)
}

// handleSignals blocks until we get an exit-causing signal
func (c *Command) handleSignals(config *Config) int {
	signalCh := make(chan os.Signal, 4)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGPIPE)

	// Wait for a signal
WAIT:
	var sig os.Signal
	select {
	case s := <-signalCh:
		sig = s
	case <-c.ShutdownCh:
		sig = os.Interrupt
	case <-c.retryJoinErrCh:
		return 1
	}
	c.logger.Printf("[INFO] Caught signal: %v", sig)

	// Skip any SIGPIPE signal (See issue #1798)
	if sig == syscall.SIGPIPE {
		goto WAIT
	}

	// Check if this is a SIGHUP
	if sig == syscall.SIGHUP {
		if conf := c.handleReload(config); conf != nil {
			*config = *conf
		}
		goto WAIT
	}

	// Check if we should do a graceful leave
	graceful := false
	if sig == os.Interrupt && config.LeaveOnInt {
		graceful = true
	} else if sig == syscall.SIGTERM && config.LeaveOnTerm {
		graceful = true
	}

	// Bail fast if not doing a graceful leave
	if !graceful {
		return 1
	}

	// Attempt a graceful leave
	gracefulCh := make(chan struct{})
	c.logger.Printf("[INFO] Gracefully shutting down agent...")
	go func() {
		if err := c.agent.Leave(); err != nil {
			c.logger.Printf("[ERR] %s", err)
			return
		}
		close(gracefulCh)
	}()

	// Wait for leave or another signal
	select {
	case <-signalCh:
		return 1
	case <-time.After(gracefulTimeout):
		return 1
	case <-gracefulCh:
		return 0
	}
}

// handleReload is invoked when we should reload our configs, e.g. SIGHUP
func (c *Command) handleReload(config *Config) *Config {
	c.logger.Printf("[INFO] Reloading configuration...")
	newConf := c.readConfig()
	if newConf == nil {
		c.logger.Printf("[ERR] Failed to reload configs")
		return config
	}

	// Change the log level
	minLevel := logutils.LogLevel(strings.ToUpper(newConf.LogLevel))
	if ValidateLevelFilter(minLevel, c.logFilter) {
		c.logFilter.SetMinLevel(minLevel)
	} else {
		c.logger.Printf("[INFO] Invalid log level: %s. Valid log levels are: %v",
			minLevel, c.logFilter.Levels)

		// Keep the current log level
		newConf.LogLevel = config.LogLevel
	}

	if s := c.agent.Server(); s != nil {
		_, err := convertServerConfig(newConf, c.logOutput)
		if err != nil {
			c.logger.Printf("[ERR] agent: failed to convert server config: %v", err)
		}
	}

	return newConf
}

// setupMetric is used ot setup the metric sub-systems
func (c *Command) setupMetric(config *Config) error {
	/* Setup metric
	Aggregate on 10 second intervals for 1 minute. Expose the
	metrics over stderr when there is a SIGUSR1 received.
	*/
	inm := metrics.NewInmemSink(10*time.Second, time.Minute)
	metrics.DefaultInmemSignal(inm)

	var telConfig *Metric
	if config.Metric == nil {
		telConfig = &Metric{}
	} else {
		telConfig = config.Metric
	}

	metricsConf := metrics.DefaultConfig("server")
	metricsConf.EnableHostname = !telConfig.DisableHostname
	if telConfig.UseNodeName {
		metricsConf.HostName = config.NodeName
		metricsConf.EnableHostname = true
	}

	// Configure the statsite sink
	var fanout metrics.FanoutSink
	if telConfig.StatsiteAddr != "" {
		sink, err := metrics.NewStatsiteSink(telConfig.StatsiteAddr)
		if err != nil {
			return err
		}
		fanout = append(fanout, sink)
	}

	// Configure the statsd sink
	if telConfig.StatsdAddr != "" {
		sink, err := metrics.NewStatsdSink(telConfig.StatsdAddr)
		if err != nil {
			return err
		}
		fanout = append(fanout, sink)
	}

	// Configure the datadog sink
	if telConfig.DataDogAddr != "" {
		sink, err := datadog.NewDogStatsdSink(telConfig.DataDogAddr, config.NodeName)
		if err != nil {
			return err
		}
		fanout = append(fanout, sink)
	}

	// Initialize the global sink
	if len(fanout) > 0 {
		fanout = append(fanout, inm)
		metrics.NewGlobal(metricsConf, fanout)
	} else {
		metricsConf.EnableHostname = false
		metrics.NewGlobal(metricsConf, inm)
	}
	return nil
}

func (c *Command) startupJoin(config *Config) error {
	if len(config.Server.StartJoin) == 0 || !config.Server.Enabled {
		return nil
	}

	c.logger.Printf("[INFO] Joining cluster...")
	n, err := c.agent.server.Join(config.Server.StartJoin)
	if err != nil {
		return err
	}

	c.logger.Printf("[INFO] Join completed. Synced with %d initial agents", n)
	return nil
}

// retryJoin is used to handle retrying a join until it succeeds or all retries
// are exhausted.
func (c *Command) retryJoin(config *Config) {
	if len(config.Server.RetryJoin) == 0 || !config.Server.Enabled {
		return
	}

	c.logger.Printf("[INFO] agent: Joining cluster...")

	attempt := 0
	for {
		n, err := c.agent.server.Join(config.Server.RetryJoin)
		if err == nil {
			c.logger.Printf("[INFO] agent: Join completed. Synced with %d initial agents", n)
			return
		}

		attempt++
		if config.Server.RetryMaxAttempts > 0 && attempt > config.Server.RetryMaxAttempts {
			c.logger.Printf("[ERR] agent: max join retry exhausted, exiting")
			close(c.retryJoinErrCh)
			return
		}

		c.logger.Printf("[WARN] agent: Join failed: %v, retrying in %v", err,
			config.Server.RetryInterval)
		time.Sleep(config.Server.retryInterval)
	}
}

func (c *Command) Synopsis() string {
	return "Runs a Udup agent"
}

func (c *Command) Help() string {
	helpText := `
Usage: server agent [options]

  Starts the Udup agent and runs until an interrupt is received.
  The agent may be a client and/or server.

  The Udup agent's configuration primarily comes from the config
  files used, but a subset of the options may also be passed directly
  as CLI arguments, listed below.

General Options (clients and servers):

  -bind=<addr>
    The address the agent will bind to for all of its various network
    services. The individual services that run bind to individual
    ports on this address. Defaults to the loopback 127.0.0.1.

  -config=<path>
    The path to either a single config file or a directory of config
    files to use for configuring the Udup agent. This option may be
    specified multiple times. If multiple config files are used, the
    values from each will be merged together. During merging, values
    from files found later in the list are merged over values from
    previously parsed files.

  -data-dir=<path>
    The data directory used to store store and other persistent data.
    On client machines this is used to house allocation data such as
    downloaded artifacts used by drivers. On server nodes, the data
    dir is also used to store the replicated log.

  -dc=<datacenter>
    The name of the datacenter this Udup agent is a member of. By
    default this is set to "dc1".

  -log-level=<level>
    Specify the verbosity level of Udup's logs. Valid values include
    DEBUG, INFO, and WARN, in decreasing order of verbosity. The
    default is INFO.

  -node=<name>
    The name of the local agent. This name is used to identify the node
    in the cluster. The name must be unique per region. The default is
    the current hostname of the machine.

  -region=<region>
    Name of the region the Udup agent will be a member of. By default
    this value is set to "global".

Server Options:

  -server
    Enable server mode for the agent. Agents in server mode are
    clustered together and handle the additional responsibility of
    leader election, data replication, and scheduling work onto
    eligible client nodes.

  -bootstrap-expect=<num>
    Configures the expected number of servers nodes to wait for before
    bootstrapping the cluster. Once <num> servers have joined eachother,
    Udup initiates the bootstrap process.

  -join=<address>
    Address of an agent to join at start time. Can be specified
    multiple times.

  -retry-join=<address>
    Address of an agent to join at start time with retries enabled.
    Can be specified multiple times.

  -retry-max=<num>
    Maximum number of join attempts. Defaults to 0, which will retry
    indefinitely.

  -retry-interval=<dur>
    Time to wait between join attempts.

  -rejoin
    Ignore a previous leave and attempts to rejoin the cluster.

Client Options:

  -client
    Enable client mode for the agent. Client mode enables a given node to be
    evaluated for allocations. If client mode is not enabled, no work will be
    scheduled to the agent.

  -store-dir
    The directory used to store store and other persistent data. If not
    specified a subdirectory under the "-data-dir" will be used.

  -servers
    A list of known server addresses to connect to given as "host:port" and
    delimited by commas.
 `
	return strings.TrimSpace(helpText)
}
