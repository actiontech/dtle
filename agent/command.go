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
	"github.com/mitchellh/cli"

	ulog "udup/internal/logger"
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
	logger         *ulog.Logger
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

	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	flags.Usage = func() { c.Ui.Error(c.Help()) }

	// Role options
	flags.BoolVar(&cmdConfig.Server.Enabled, "manager", false, "")
	flags.BoolVar(&cmdConfig.Client.Enabled, "agent", false, "")

	// Server-only options
	flags.IntVar(&cmdConfig.Server.BootstrapExpect, "bootstrap-expect", 0, "")
	flags.Var((*StringFlag)(&cmdConfig.Server.StartJoin), "join", "")
	flags.IntVar(&cmdConfig.Server.RetryMaxAttempts, "retry-max", 0, "")
	flags.StringVar(&cmdConfig.Server.RetryInterval, "retry-interval", "", "")

	// Client-only options
	flags.StringVar(&servers, "managers", "", "")

	// General options
	flags.Var((*StringFlag)(&configPath), "config", "config")
	flags.StringVar(&cmdConfig.BindAddr, "bind", "", "")
	flags.StringVar(&cmdConfig.Region, "region", "", "")
	flags.StringVar(&cmdConfig.DataDir, "data-dir", "", "")
	flags.StringVar(&cmdConfig.Datacenter, "dc", "", "")
	flags.StringVar(&cmdConfig.LogLevel, "log-level", "", "")
	flags.StringVar(&cmdConfig.PidFile, "pid-file", "", "")
	flags.StringVar(&cmdConfig.NodeName, "node", "", "")

	if err := flags.Parse(c.args); err != nil {
		return nil
	}

	if cmdConfig.PidFile != "" {
		f, err := os.Create(cmdConfig.PidFile)
		if err != nil {
			c.Ui.Error(fmt.Sprintf("Unable to create pidfile: %s", err))
		}

		fmt.Fprintf(f, "%d\n", os.Getpid())

		f.Close()
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
			c.Ui.Error(fmt.Sprintf("Error loading configuration from %s: %s", path, err))
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
		c.Ui.Error(fmt.Sprintf("Error normalizes Addresses: %s", err))
		return nil
	}

	// Parse the RetryInterval.
	dur, err := time.ParseDuration(config.Server.RetryInterval)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Error parsing retry interval: %s", err))
		return nil
	}
	config.Server.retryInterval = dur

	// Check that the server is running in at least one mode.
	if !(config.Server.Enabled || config.Client.Enabled) {
		c.Ui.Error("Must specify either manager or agent mode for the server.")
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
			c.Ui.Error(fmt.Sprintf("%s must be given as an absolute path: got %v", k, dir))
			return nil
		}
	}

	// Ensure that we have the directories we neet to run.
	if config.Server.Enabled && config.DataDir == "" {
		c.Ui.Error("Must specify data directory")
		return nil
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
func (c *Command) setupLoggers(config *Config) (io.Writer, error) {
	var oFile *os.File
	if config.LogFile != "" {
		if _, err := os.Stat(config.LogFile); os.IsNotExist(err) {
			if oFile, err = os.Create(config.LogFile); err != nil {
				oFile = os.Stderr
				return nil, fmt.Errorf("Unable to create %s (%s), using stderr",
					config.LogFile, err)
			}
		} else {
			if oFile, err = os.OpenFile(config.LogFile, os.O_APPEND|os.O_WRONLY, os.ModeAppend); err != nil {
				c.logger.Errorf("Unable to append to %s (%s), using stderr",
					config.LogFile, err)
				oFile = os.Stderr
				return nil, fmt.Errorf("Unable to append to %s (%s), using stderr",
					config.LogFile, err)
			}
		}
	} else {
		oFile = os.Stderr
	}

	c.logOutput = oFile
	c.logger = ulog.New(oFile, ulog.ParseLevel(config.LogLevel))
	log.SetOutput(oFile)
	return oFile, nil
}

// setupAgent is used to start the agent and various interfaces
func (c *Command) setupAgent(config *Config, logOutput io.Writer) error {
	c.logger.Printf("Starting Udup server...")
	agent, err := NewAgent(config, logOutput, c.logger)
	if err != nil {
		c.logger.Errorf("Error starting server: %s", err)
		return err
	}
	c.agent = agent

	// Setup the HTTP server
	http, err := NewHTTPServer(agent, config, logOutput)
	if err != nil {
		agent.Shutdown()
		c.logger.Errorf("Error starting http server: %s", err)
		return err
	}
	c.httpServer = http

	return nil
}

func (c *Command) Run(args []string) int {
	// Parse our configs
	c.args = args
	config := c.readConfig()
	if config == nil {
		return 1
	}

	// Setup the log outputs
	logOutput, err := c.setupLoggers(config)
	if err != nil {
		c.logger.Errorf("Error setup logger: %s", err)
		return 1
	}

	// Log config files
	if len(config.Files) > 0 {
		c.logger.Printf("Loaded configuration from %s", strings.Join(config.Files, ", "))
	} else {
		c.logger.Printf("No configuration files loaded")
	}

	// Initialize the metric
	if err := c.setupMetric(config); err != nil {
		c.logger.Errorf("Error initializing metric: %s", err)
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
		c.logger.Errorf("%v", err.Error())
		return 1
	}

	// Compile agent information for output later
	info := make(map[string]string)
	info["version"] = config.Version
	info["agent"] = strconv.FormatBool(config.Client.Enabled)
	info["log level"] = config.LogLevel
	info["manager"] = strconv.FormatBool(config.Server.Enabled)
	info["region"] = fmt.Sprintf("%s (DC: %s)", config.Region, config.Datacenter)

	// Sort the keys for output
	infoKeys := make([]string, 0, len(info))
	for key := range info {
		infoKeys = append(infoKeys, key)
	}
	sort.Strings(infoKeys)

	// Agent configuration output
	padding := 18
	c.logger.Printf("Udup server configuration:\n")
	for _, k := range infoKeys {
		c.logger.Printf(fmt.Sprintf(
			" %s%s: %s",
			strings.Repeat(" ", padding-len(k)),
			strings.Title(k),
			info[k]))
	}
	// Output the header that the server has started
	c.logger.Printf("Udup server started! Log data will stream in below:\n")

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
	c.logger.Printf("Caught signal: %v", sig)

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
	c.logger.Printf("Gracefully shutting down agent...")
	go func() {
		if err := c.agent.Leave(); err != nil {
			c.logger.Errorf("%s", err)
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
	c.logger.Printf("Reloading configuration...")
	newConf := c.readConfig()
	if newConf == nil {
		c.logger.Errorf("Failed to reload configs")
		return config
	}

	if s := c.agent.Server(); s != nil {
		_, err := convertServerConfig(newConf, c.logOutput)
		if err != nil {
			c.logger.Errorf("server: failed to convert server config: %v", err)
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

	metricsConf := metrics.DefaultConfig("udup")
	metricsConf.EnableHostname = !telConfig.DisableHostname
	if telConfig.UseNodeName {
		metricsConf.HostName = config.NodeName
		metricsConf.EnableHostname = true
	}

	// Configure the prometheus sink
	var fanout metrics.FanoutSink
	if telConfig.PrometheusAddr != "" {
		sink, err := NewPrometheusSink(telConfig.PrometheusAddr, telConfig.collectionInterval, c.logger)
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

	c.logger.Printf("Joining cluster...")
	n, err := c.agent.server.Join(config.Server.StartJoin)
	if err != nil {
		return err
	}

	c.logger.Printf("Join completed. Synced with %d initial agents", n)
	return nil
}

// retryJoin is used to handle retrying a join until it succeeds or all retries
// are exhausted.
func (c *Command) retryJoin(config *Config) {
	if len(config.Server.StartJoin) == 0 || !config.Server.Enabled {
		return
	}

	c.logger.Printf("server: Joining cluster...")

	attempt := 0
	for {
		n, err := c.agent.server.Join(config.Server.StartJoin)
		if err == nil {
			c.logger.Printf("server: Join completed. Synced with %d initial agents", n)
			return
		}

		attempt++
		if config.Server.RetryMaxAttempts > 0 && attempt > config.Server.RetryMaxAttempts {
			c.logger.Errorf("server: max join retry exhausted, exiting")
			close(c.retryJoinErrCh)
			return
		}

		c.logger.Warnf("server: Join failed: %v, retrying in %v", err,
			config.Server.RetryInterval)
		time.Sleep(config.Server.retryInterval)
	}
}

func (c *Command) Synopsis() string {
	return "Runs a Udup server"
}

func (c *Command) Help() string {
	helpText := `
Usage: udup server [options]

  Starts the Udup server and runs until an interrupt is received.
  The server may be a agent and/or manager.

  The Udup server's configuration primarily comes from the config
  files used, but a subset of the options may also be passed directly
  as CLI arguments, listed below.

General Options (agents and managers):

  -bind=<addr>
    The address the server will bind to for all of its various network
    services. The individual services that run bind to individual
    ports on this address. Defaults to the loopback 127.0.0.1.

  -config=<path>
    The path to either a single config file or a directory of config
    files to use for configuring the Udup server. This option may be
    specified multiple times. If multiple config files are used, the
    values from each will be merged together. During merging, values
    from files found later in the list are merged over values from
    previously parsed files.

  -data-dir=<path>
    The data directory used to store store and other persistent data.
    On agent machines this is used to house allocation data such as
    downloaded artifacts used by drivers. On manager nodes, the data
    dir is also used to store the replicated log.

  -dc=<datacenter>
    The name of the datacenter this Udup server is a member of. By
    default this is set to "dc1".

  -log-level=<level>
    Specify the verbosity level of Udup's logs. Valid values include
    DEBUG, INFO, and WARN, in decreasing order of verbosity. The
    default is INFO.

  -node=<name>
    The name of the local server. This name is used to identify the node
    in the cluster. The name must be unique per region. The default is
    the current hostname of the machine.

  -region=<region>
    Name of the region the Udup server will be a member of. By default
    this value is set to "global".

Manager Options:

  -manager
    Enable manager mode for the udup. Servers in manager mode are
    clustered together and handle the additional responsibility of
    leader election, data replication, and scheduling work onto
    eligible agent nodes.

  -join=<address>
    Address of an server to join at start time. Can be specified
    multiple times.

Agent Options:

  -agent
    Enable agent mode for the server. Agent mode enables a given node to be
    evaluated for allocations. If agent mode is not enabled, no work will be
    scheduled to the server.

  -managers
    A list of known server addresses to connect to given as "host:port" and
    delimited by commas.
 `
	return strings.TrimSpace(helpText)
}
