package agent

import (
	"fmt"
	"io"
	"log"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"udup/client"
	clientconfig "udup/client/config"
	"udup/server"
	"udup/server/structs"
)

const (
	clientHttpCheckInterval = 10 * time.Second
	clientHttpCheckTimeout  = 3 * time.Second
	serverHttpCheckInterval = 10 * time.Second
	serverHttpCheckTimeout  = 6 * time.Second
	serverRpcCheckInterval  = 10 * time.Second
	serverRpcCheckTimeout   = 3 * time.Second
	serverSerfCheckInterval = 10 * time.Second
	serverSerfCheckTimeout  = 3 * time.Second
)

// Agent is a long running daemon that is used to run both
// clients and servers. Servers are responsible for managing
// state and making scheduling decisions. Clients can be
// scheduled to, and are responsible for interfacing with
// servers to run allocations.
type Agent struct {
	config    *Config
	logger    *log.Logger
	logOutput io.Writer

	client         *client.Client
	clientHTTPAddr string
	clientNatsAddr string

	server         *server.Server
	serverHTTPAddr string
	serverRPCAddr  string
	serverSerfAddr string

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// NewAgent is used to create a new agent with the given configuration
func NewAgent(config *Config, logOutput io.Writer) (*Agent, error) {
	a := &Agent{
		config:     config,
		logger:     log.New(logOutput, "", log.LstdFlags|log.Lmicroseconds),
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

// serverConfig is used to generate a new server configuration struct
// for initializing a udup server.
func (a *Agent) serverConfig() (*server.Config, error) {
	conf := a.config.UdupConfig
	if conf == nil {
		conf = server.DefaultConfig()
	}
	conf.LogOutput = a.logOutput
	conf.DevMode = a.config.DevMode
	conf.Build = fmt.Sprintf("%s%s", a.config.Version, a.config.VersionPrerelease)
	if a.config.Region != "" {
		conf.Region = a.config.Region
	}
	if a.config.Datacenter != "" {
		conf.Datacenter = a.config.Datacenter
	}
	if a.config.NodeName != "" {
		conf.NodeName = a.config.NodeName
	}
	if a.config.Server.BootstrapExpect > 0 {
		if a.config.Server.BootstrapExpect == 1 {
			conf.Bootstrap = true
		} else {
			atomic.StoreInt32(&conf.BootstrapExpect, int32(a.config.Server.BootstrapExpect))
		}
	}
	if a.config.DataDir != "" {
		conf.DataDir = filepath.Join(a.config.DataDir, "server")
	}
	if a.config.Server.DataDir != "" {
		conf.DataDir = a.config.Server.DataDir
	}
	if a.config.Server.ProtocolVersion != 0 {
		conf.ProtocolVersion = uint8(a.config.Server.ProtocolVersion)
	}
	if a.config.Server.NumSchedulers != 0 {
		conf.NumSchedulers = a.config.Server.NumSchedulers
	}
	if len(a.config.Server.EnabledSchedulers) != 0 {
		conf.EnabledSchedulers = a.config.Server.EnabledSchedulers
	}

	// Set up the advertise addrs
	if addr := a.config.AdvertiseAddrs.Serf; addr != "" {
		serfAddr, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("error resolving serf advertise address: %s", err)
		}
		conf.SerfConfig.MemberlistConfig.AdvertiseAddr = serfAddr.IP.String()
		conf.SerfConfig.MemberlistConfig.AdvertisePort = serfAddr.Port
	}
	if addr := a.config.AdvertiseAddrs.RPC; addr != "" {
		rpcAddr, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("error resolving rpc advertise address: %s", err)
		}
		conf.RPCAdvertise = rpcAddr
	}

	// Set up the bind addresses
	if addr := a.config.BindAddr; addr != "" {
		conf.RPCAddr.IP = net.ParseIP(addr)
		conf.SerfConfig.MemberlistConfig.BindAddr = addr
	}
	if addr := a.config.Addresses.RPC; addr != "" {
		conf.RPCAddr.IP = net.ParseIP(addr)
	}

	if addr := a.config.Addresses.Serf; addr != "" {
		conf.SerfConfig.MemberlistConfig.BindAddr = addr
	}

	// Set up the ports
	if port := a.config.Ports.RPC; port != 0 {
		conf.RPCAddr.Port = port
	}
	if port := a.config.Ports.Serf; port != 0 {
		conf.SerfConfig.MemberlistConfig.BindPort = port
	}

	// Resolve the Server's HTTP Address
	if a.config.AdvertiseAddrs.HTTP != "" {
		a.serverHTTPAddr = a.config.AdvertiseAddrs.HTTP
	} else if a.config.Addresses.HTTP != "" {
		a.serverHTTPAddr = net.JoinHostPort(a.config.Addresses.HTTP, strconv.Itoa(a.config.Ports.HTTP))
	} else if a.config.BindAddr != "" {
		a.serverHTTPAddr = net.JoinHostPort(a.config.BindAddr, strconv.Itoa(a.config.Ports.HTTP))
	} else {
		a.serverHTTPAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(a.config.Ports.HTTP))
	}
	addr, err := net.ResolveTCPAddr("tcp", a.serverHTTPAddr)
	if err != nil {
		return nil, fmt.Errorf("error resolving HTTP addr %+q: %v", a.serverHTTPAddr, err)
	}
	a.serverHTTPAddr = net.JoinHostPort(addr.IP.String(), strconv.Itoa(addr.Port))

	// Resolve the Server's RPC Address
	if a.config.AdvertiseAddrs.RPC != "" {
		a.serverRPCAddr = a.config.AdvertiseAddrs.RPC
	} else if a.config.Addresses.RPC != "" {
		a.serverRPCAddr = net.JoinHostPort(a.config.Addresses.RPC, strconv.Itoa(a.config.Ports.RPC))
	} else if a.config.BindAddr != "" {
		a.serverRPCAddr = net.JoinHostPort(a.config.BindAddr, strconv.Itoa(a.config.Ports.RPC))
	} else {
		a.serverRPCAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(a.config.Ports.RPC))
	}
	addr, err = net.ResolveTCPAddr("tcp", a.serverRPCAddr)
	if err != nil {
		return nil, fmt.Errorf("error resolving RPC addr %+q: %v", a.serverRPCAddr, err)
	}
	a.serverRPCAddr = net.JoinHostPort(addr.IP.String(), strconv.Itoa(addr.Port))

	// Resolve the Server's Serf Address
	if a.config.AdvertiseAddrs.Serf != "" {
		a.serverSerfAddr = a.config.AdvertiseAddrs.Serf
	} else if a.config.Addresses.Serf != "" {
		a.serverSerfAddr = net.JoinHostPort(a.config.Addresses.Serf, strconv.Itoa(a.config.Ports.Serf))
	} else if a.config.BindAddr != "" {
		a.serverSerfAddr = net.JoinHostPort(a.config.BindAddr, strconv.Itoa(a.config.Ports.Serf))
	} else {
		a.serverSerfAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(a.config.Ports.Serf))
	}
	addr, err = net.ResolveTCPAddr("tcp", a.serverSerfAddr)
	if err != nil {
		return nil, fmt.Errorf("error resolving Serf addr %+q: %v", a.serverSerfAddr, err)
	}
	a.serverSerfAddr = net.JoinHostPort(addr.IP.String(), strconv.Itoa(addr.Port))

	if gcThreshold := a.config.Server.NodeGCThreshold; gcThreshold != "" {
		dur, err := time.ParseDuration(gcThreshold)
		if err != nil {
			return nil, err
		}
		conf.NodeGCThreshold = dur
	}

	if heartbeatGrace := a.config.Server.HeartbeatGrace; heartbeatGrace != "" {
		dur, err := time.ParseDuration(heartbeatGrace)
		if err != nil {
			return nil, err
		}
		conf.HeartbeatGrace = dur
	}

	return conf, nil
}

// clientConfig is used to generate a new client configuration struct
// for initializing a Udup client.
func (a *Agent) clientConfig() (*clientconfig.Config, error) {
	// Setup the configuration
	conf := a.config.ClientConfig
	if conf == nil {
		conf = clientconfig.DefaultConfig()
	}
	if a.server != nil {
		conf.RPCHandler = a.server
	}
	conf.LogOutput = a.logOutput
	conf.DevMode = a.config.DevMode
	if a.config.Region != "" {
		conf.Region = a.config.Region
	}
	if a.config.DataDir != "" {
		conf.StateDir = filepath.Join(a.config.DataDir, "client")
		conf.AllocDir = filepath.Join(a.config.DataDir, "alloc")
	}
	if a.config.Client.StateDir != "" {
		conf.StateDir = a.config.Client.StateDir
	}
	if a.config.Client.AllocDir != "" {
		conf.AllocDir = a.config.Client.AllocDir
	}
	conf.Servers = a.config.Client.Servers
	if a.config.Client.NetworkInterface != "" {
		conf.NetworkInterface = a.config.Client.NetworkInterface
	}
	conf.ChrootEnv = a.config.Client.ChrootEnv

	if a.config.Client.NetworkSpeed != 0 {
		conf.NetworkSpeed = a.config.Client.NetworkSpeed
	}
	if a.config.Client.MaxKillTimeout != "" {
		dur, err := time.ParseDuration(a.config.Client.MaxKillTimeout)
		if err != nil {
			return nil, fmt.Errorf("Error parsing retry interval: %s", err)
		}
		conf.MaxKillTimeout = dur
	}

	// Set up the bind addresses
	if addr := a.config.BindAddr; addr != "" {
		conf.NatsAddr.IP = net.ParseIP(addr)
	}
	if addr := a.config.Addresses.Nats; addr != "" {
		conf.NatsAddr.IP = net.ParseIP(addr)
	}

	// Set up the ports
	if port := a.config.Ports.Nats; port != 0 {
		conf.NatsAddr.Port = port
	}

	// Setup the node
	conf.Node = new(structs.Node)
	conf.Node.Datacenter = a.config.Datacenter
	conf.Node.Name = a.config.NodeName
	conf.Node.Meta = a.config.Client.Meta
	conf.Node.NodeClass = a.config.Client.NodeClass

	// Resolve the Client's HTTP address
	if a.config.AdvertiseAddrs.HTTP != "" {
		a.clientHTTPAddr = a.config.AdvertiseAddrs.HTTP
	} else if a.config.Addresses.HTTP != "" {
		a.clientHTTPAddr = net.JoinHostPort(a.config.Addresses.HTTP, strconv.Itoa(a.config.Ports.HTTP))
	} else if a.config.BindAddr != "" {
		a.clientHTTPAddr = net.JoinHostPort(a.config.BindAddr, strconv.Itoa(a.config.Ports.HTTP))
	} else {
		a.clientHTTPAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(a.config.Ports.HTTP))
	}
	addr, err := net.ResolveTCPAddr("tcp", a.clientHTTPAddr)
	if err != nil {
		return nil, fmt.Errorf("error resolving HTTP addr %+q: %v", a.clientHTTPAddr, err)
	}
	httpAddr := net.JoinHostPort(addr.IP.String(), strconv.Itoa(addr.Port))

	conf.Node.HTTPAddr = httpAddr
	a.clientHTTPAddr = httpAddr

	// Resolve the Client's Nats address
	if a.config.AdvertiseAddrs.Nats != "" {
		a.clientNatsAddr = a.config.AdvertiseAddrs.Nats
	} else if a.config.Addresses.Nats != "" {
		a.clientNatsAddr = net.JoinHostPort(a.config.Addresses.Nats, strconv.Itoa(a.config.Ports.Nats))
	} else if a.config.BindAddr != "" {
		a.clientNatsAddr = net.JoinHostPort(a.config.BindAddr, strconv.Itoa(a.config.Ports.Nats))
	} else {
		a.clientNatsAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(a.config.Ports.Nats))
	}
	addr, err = net.ResolveTCPAddr("tcp", a.clientNatsAddr)
	if err != nil {
		return nil, fmt.Errorf("error resolving HTTP addr %+q: %v", a.clientNatsAddr, err)
	}
	natsAddr := net.JoinHostPort(addr.IP.String(), strconv.Itoa(addr.Port))

	conf.Node.NatsAddr = natsAddr
	a.clientNatsAddr = natsAddr

	// Reserve resources on the node.
	r := conf.Node.Reserved
	if r == nil {
		r = new(structs.Resources)
		conf.Node.Reserved = r
	}
	r.CPU = a.config.Client.Reserved.CPU
	r.MemoryMB = a.config.Client.Reserved.MemoryMB
	r.DiskMB = a.config.Client.Reserved.DiskMB
	r.IOPS = a.config.Client.Reserved.IOPS
	conf.GloballyReservedPorts = a.config.Client.Reserved.ParsedReservedPorts

	conf.Version = fmt.Sprintf("%s%s", a.config.Version, a.config.VersionPrerelease)
	conf.Revision = a.config.Revision

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
	server, err := server.NewServer(conf, a.logger)
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
	client, err := client.NewClient(conf, a.logger)
	if err != nil {
		return fmt.Errorf("client setup failed: %v", err)
	}
	a.client = client

	return nil
}

// findLoopbackDevice iterates through all the interfaces on a machine and
// returns the ip addr, mask of the loopback device
func (a *Agent) findLoopbackDevice() (string, string, string, error) {
	var ifcs []net.Interface
	var err error
	ifcs, err = net.Interfaces()
	if err != nil {
		return "", "", "", err
	}
	for _, ifc := range ifcs {
		addrs, err := ifc.Addrs()
		if err != nil {
			return "", "", "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip.IsLoopback() {
				if ip.To4() == nil {
					continue
				}
				return ifc.Name, ip.String(), addr.String(), nil
			}
		}
	}

	return "", "", "", fmt.Errorf("no loopback devices with IPV4 addr found")
}

// Leave is used gracefully exit. Clients will inform servers
// of their departure so that allocations can be rescheduled.
func (a *Agent) Leave() error {
	if a.client != nil {
		if err := a.client.Leave(); err != nil {
			a.logger.Printf("[ERR] agent: client leave failed: %v", err)
		}
	}
	if a.server != nil {
		if err := a.server.Leave(); err != nil {
			a.logger.Printf("[ERR] agent: server leave failed: %v", err)
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
			a.logger.Printf("[ERR] agent: client shutdown failed: %v", err)
		}
	}
	if a.server != nil {
		if err := a.server.Shutdown(); err != nil {
			a.logger.Printf("[ERR] agent: server shutdown failed: %v", err)
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
func (a *Agent) Client() *client.Client {
	return a.client
}

// Server returns the configured server or nil
func (a *Agent) Server() *server.Server {
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
