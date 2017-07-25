package agent

import (
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	ucli "udup/internal/client"
	uconf "udup/internal/config"
	ulog "udup/internal/logger"
	umodel "udup/internal/models"
	usrv "udup/internal/server"
)

// Agent is a long running daemon that is used to run both
// clients and servers. Servers are responsible for managing
// state and making scheduling decisions. Clients can be
// scheduled to, and are responsible for interfacing with
// servers to run allocations.
type Agent struct {
	config    *Config
	logger    *ulog.Logger
	logOutput io.Writer

	client *ucli.Client

	server *usrv.Server

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// NewAgent is used to create a new agent with the given configuration
func NewAgent(config *Config, logOutput io.Writer, log *ulog.Logger) (*Agent, error) {
	a := &Agent{
		config:     config,
		logger:     log,
		logOutput:  logOutput,
		shutdownCh: make(chan struct{}),
	}
	if err := a.setupServer(); err != nil {
		return nil, err
	}
	if err := a.setupClient(); err != nil {
		return nil, err
	}
	if a.client == nil && a.server == nil {
		return nil, fmt.Errorf("must have at least client or server mode enabled")
	}

	return a, nil
}

// convertServerConfig takes an agent config and log output and returns a Udup Config.
func convertServerConfig(agentConfig *Config, logOutput io.Writer) (*uconf.ServerConfig, error) {
	conf := agentConfig.UdupConfig
	if conf == nil {
		conf = uconf.DefaultServerConfig()
	}
	conf.LogOutput = logOutput
	conf.Build = agentConfig.Version
	if agentConfig.Region != "" {
		conf.Region = agentConfig.Region
	}
	if agentConfig.Datacenter != "" {
		conf.Datacenter = agentConfig.Datacenter
	}
	if agentConfig.NodeName != "" {
		conf.NodeName = agentConfig.NodeName
	}
	if agentConfig.Server.BootstrapExpect > 0 {
		if agentConfig.Server.BootstrapExpect == 1 {
			conf.Bootstrap = true
		} else {
			atomic.StoreInt32(&conf.BootstrapExpect, int32(agentConfig.Server.BootstrapExpect))
		}
	}
	if agentConfig.DataDir != "" {
		conf.DataDir = filepath.Join(agentConfig.DataDir, "server")
	}
	if agentConfig.Server.DataDir != "" {
		conf.DataDir = agentConfig.Server.DataDir
	}
	if agentConfig.Server.NumSchedulers != 0 {
		conf.NumSchedulers = agentConfig.Server.NumSchedulers
	}
	if len(agentConfig.Server.EnabledSchedulers) != 0 {
		conf.EnabledSchedulers = agentConfig.Server.EnabledSchedulers
	}

	// Set up the bind addresses
	rpcAddr, err := net.ResolveTCPAddr("tcp", agentConfig.normalizedAddrs.RPC)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse RPC address %q: %v", agentConfig.normalizedAddrs.RPC, err)
	}
	serfAddr, err := net.ResolveTCPAddr("tcp", agentConfig.normalizedAddrs.Serf)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse Serf address %q: %v", agentConfig.normalizedAddrs.Serf, err)
	}
	conf.RPCAddr.Port = rpcAddr.Port
	conf.RPCAddr.IP = rpcAddr.IP
	conf.SerfConfig.MemberlistConfig.BindPort = serfAddr.Port
	conf.SerfConfig.MemberlistConfig.BindAddr = serfAddr.IP.String()

	// Set up the advertise addresses
	rpcAddr, err = net.ResolveTCPAddr("tcp", agentConfig.AdvertiseAddrs.RPC)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse RPC advertise address %q: %v", agentConfig.AdvertiseAddrs.RPC, err)
	}
	serfAddr, err = net.ResolveTCPAddr("tcp", agentConfig.AdvertiseAddrs.Serf)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse Serf advertise address %q: %v", agentConfig.AdvertiseAddrs.Serf, err)
	}
	conf.RPCAdvertise = rpcAddr
	conf.SerfConfig.MemberlistConfig.AdvertiseAddr = serfAddr.IP.String()
	conf.SerfConfig.MemberlistConfig.AdvertisePort = serfAddr.Port

	if heartbeatGrace := agentConfig.Server.HeartbeatGrace; heartbeatGrace != "" {
		dur, err := time.ParseDuration(heartbeatGrace)
		if err != nil {
			return nil, err
		}
		conf.HeartbeatGrace = dur
	}

	if *agentConfig.Consul.AutoAdvertise && agentConfig.Consul.ServerServiceName == "" {
		return nil, fmt.Errorf("server_service_name must be set when auto_advertise is enabled")
	}

	// Add the Consul config
	conf.ConsulConfig = agentConfig.Consul

	return conf, nil
}

// serverConfig is used to generate a new server configuration struct
// for initializing a server server.
func (a *Agent) serverConfig() (*uconf.ServerConfig, error) {
	return convertServerConfig(a.config, a.logOutput)
}

// clientConfig is used to generate a new client configuration struct
// for initializing a Udup client.
func (a *Agent) clientConfig() (*uconf.ClientConfig, error) {
	// Setup the configuration
	conf := a.config.ClientConfig
	if conf == nil {
		conf = uconf.DefaultClientConfig()
	}
	if a.server != nil {
		conf.RPCHandler = a.server
	}
	conf.LogOutput = a.logOutput
	conf.LogFile = a.config.LogFile
	conf.LogLevel = a.config.LogLevel
	if a.config.Region != "" {
		conf.Region = a.config.Region
	}
	if a.config.DataDir != "" {
		conf.StateDir = filepath.Join(a.config.DataDir, "client")
	}
	conf.Servers = a.config.Client.Servers

	// Setup the node
	conf.Node = new(umodel.Node)
	conf.Node.Datacenter = a.config.Datacenter
	conf.Node.Name = a.config.NodeName

	// Set up the HTTP advertise address
	conf.Node.HTTPAddr = a.config.AdvertiseAddrs.HTTP
	conf.Version = a.config.Version

	if *a.config.Consul.AutoAdvertise && a.config.Consul.ClientServiceName == "" {
		return nil, fmt.Errorf("client_service_name must be set when auto_advertise is enabled")
	}

	conf.ConsulConfig = a.config.Consul
	conf.NatsAddr = a.config.AdvertiseAddrs.Nats
	conf.MaxPayload = a.config.MaxPayload
	conf.StatsCollectionInterval = a.config.Metric.collectionInterval
	conf.PublishNodeMetrics = a.config.Metric.PublishNodeMetrics
	conf.PublishAllocationMetrics = a.config.Metric.PublishAllocationMetrics

	conf.NoHostUUID = a.config.Client.NoHostUUID

	return conf, nil
}

// setupServer is used to setup the server if enabled
func (a *Agent) setupServer() error {
	if !a.config.Server.Enabled {
		return nil
	}

	// Setup the configuration
	conf, err := a.serverConfig()
	if err != nil {
		return fmt.Errorf("server config setup failed: %s", err)
	}

	// Create the server
	server, err := usrv.NewServer(conf, a.logger)
	if err != nil {
		return fmt.Errorf("server setup failed: %v", err)
	}
	a.server = server

	return nil
}

// setupClient is used to setup the client if enabled
func (a *Agent) setupClient() error {
	if !a.config.Client.Enabled {
		return nil
	}

	// Setup the configuration
	conf, err := a.clientConfig()
	if err != nil {
		return fmt.Errorf("client setup failed: %v", err)
	}

	// Create the client
	client, err := ucli.NewClient(conf, a.logger)
	if err != nil {
		return fmt.Errorf("client setup failed: %v", err)
	}
	a.client = client

	return nil
}

// Leave is used gracefully exit. Clients will inform servers
// of their departure so that allocations can be rescheduled.
func (a *Agent) Leave() error {
	if a.client != nil {
		if err := a.client.Leave(); err != nil {
			a.logger.Errorf("agent: client leave failed: %v", err)
		}
	}
	if a.server != nil {
		if err := a.server.Leave(); err != nil {
			a.logger.Errorf("agent: server leave failed: %v", err)
		}
	}
	return nil
}

// Shutdown is used to terminate the agent.
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}

	a.logger.Println("[INFO] agent: requesting shutdown")
	if a.client != nil {
		if err := a.client.Shutdown(); err != nil {
			a.logger.Errorf("agent: client shutdown failed: %v", err)
		}
	}
	if a.server != nil {
		if err := a.server.Shutdown(); err != nil {
			a.logger.Errorf("agent: server shutdown failed: %v", err)
		}
	}

	a.logger.Println("[INFO] agent: shutdown complete")
	a.shutdown = true
	close(a.shutdownCh)
	return nil
}

// RPC is used to make an RPC call to the Udup servers
func (a *Agent) RPC(method string, args interface{}, reply interface{}) error {
	if a.server != nil {
		return a.server.RPC(method, args, reply)
	}
	return a.client.RPC(method, args, reply)
}

// Client returns the configured client or nil
func (a *Agent) Client() *ucli.Client {
	return a.client
}

// Server returns the configured server or nil
func (a *Agent) Server() *usrv.Server {
	return a.server
}

// Stats is used to return statistics for debugging and insight
// for various sub-systems
func (a *Agent) Stats() map[string]map[string]string {
	stats := make(map[string]map[string]string)
	if a.server != nil {
		subStat := a.server.Stats()
		for k, v := range subStat {
			stats[k] = v
		}
	}
	if a.client != nil {
		subStat := a.client.Stats()
		for k, v := range subStat {
			stats[k] = v
		}
	}
	return stats
}
