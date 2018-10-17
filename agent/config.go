/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package agent

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	uconf "github.com/actiontech/dtle/internal/config"
)

// This is the default addr to all interfaces.
const (
	DefaultAddr       = "0.0.0.0"
	DefaultMaxPayload = 100 * 1024 * 1024 // 100M
)

// Config is the configuration for the Udup agent.
type Config struct {
	// Region is the region this agent is in. Defaults to global.
	Region string `mapstructure:"region"`

	// Datacenter is the datacenter this agent is in. Defaults to dc1
	Datacenter string `mapstructure:"datacenter"`

	// NodeName is the name we register as. Defaults to hostname.
	NodeName string `mapstructure:"name"`

	// DataDir is the directory to store our store in
	DataDir string `mapstructure:"data_dir"`

	// LogLevel is the level of the logs to putout
	LogLevel string `mapstructure:"log_level"`

	LogToStdout bool `mapstructure:"log_to_stdout"`

	// file to write our pid to
	PidFile string `mapstructure:"pid_file"`

	// Specify the log file name. The empty string means to log to stdout.
	LogFile string `mapstructure:"log_file"`

	// BindAddr is the address on which all of server's services will
	// be bound. If not specified, this defaults to 0.0.0.0 .
	BindAddr string `mapstructure:"bind_addr"`

	// Ports is used to control the network ports we bind to.
	Ports *Ports `mapstructure:"ports"`

	// Addresses is used to override the network addresses we bind to.
	// Use normalizedAddrs if you need the host+port to bind to.
	Addresses *Addresses `mapstructure:"addresses"`

	// normalizedAddr is set to the Address+Port by normalizeAddrs()
	normalizedAddrs *Addresses

	// AdvertiseAddrs is used to control the addresses we advertise.
	AdvertiseAddrs *AdvertiseAddrs `mapstructure:"advertise"`

	// Client has our client related settings
	Client *ClientConfig `mapstructure:"agent"`

	// Server has our server related settings
	Server *ServerConfig `mapstructure:"manager"`

	Metric *Metric `mapstructure:"metric"`

	Network *Network `mapstructure:"network"`

	// Profile is used to select a timing profile for Serf. The supported choices
	// are "wan", "lan", and "local". The default is "lan"
	Profile string `mapstructure:"profile"`

	// LeaveOnInt is used to gracefully leave on the interrupt signal
	LeaveOnInt bool `mapstructure:"leave_on_interrupt"`

	// LeaveOnTerm is used to gracefully leave on the terminate signal
	LeaveOnTerm bool `mapstructure:"leave_on_terminate"`

	// Consul contains the configuration for the Consul Agent and
	// parameters necessary to register services, their checks, and
	// discover the current Udup servers.
	Consul *uconf.ConsulConfig `mapstructure:"consul"`

	// UdupConfig is used to override the default config.
	// This is largly used for testing purposes.
	UdupConfig *uconf.ServerConfig `mapstructure:"-" json:"-"`

	// ClientConfig is used to override the default config.
	// This is largly used for testing purposes.
	ClientConfig *uconf.ClientConfig `mapstructure:"-" json:"-"`

	// Version information is set at compilation time
	Version  string
	Revision string

	// List of config files that have been loaded (in order)
	Files []string `mapstructure:"-"`

	// HTTPAPIResponseHeaders allows users to configure the Udup http agent to
	// set arbritrary headers on API responses
	HTTPAPIResponseHeaders map[string]string `mapstructure:"http_api_response_headers"`

	// EnableUi enables the statically-compiled assets for the Udup web UI and
	// serves them at the default /ui/ endpoint automatically.
	EnableUi bool `mapstructure:"ui"`

	// UiDir is the directory containing the Web UI resources.
	// If provided, the UI endpoints will be enabled.
	UiDir string `mapstructure:"ui_dir"`

	// Schema name for dtle meta info (e.g. gtid_executed).
	// Do not use special characters (which need to be quoted) in schema name.
	DtleSchemaName string `mapstructure:"dtle_schema_name"`
}

// ClientConfig is configuration specific to the client mode
type ClientConfig struct {
	// Enabled controls if we are a client
	Enabled bool `mapstructure:"enabled"`

	// StateDir is the store directory
	StateDir string `mapstructure:"state_dir"`

	// Servers is a list of known server addresses. These are as "host:port"
	Servers []string `mapstructure:"managers"`

	// NoHostUUID disables using the host's UUID and will force generation of a
	// random UUID.
	NoHostUUID bool `mapstructure:"no_host_uuid"`
}

// ServerConfig is configuration specific to the server mode
type ServerConfig struct {
	// Enabled controls if we are a server
	Enabled bool `mapstructure:"enabled"`

	// BootstrapExpect tries to automatically bootstrap the udup cluster,
	// by withholding peers until enough servers join.
	BootstrapExpect int `mapstructure:"bootstrap_expect"`

	// DataDir is the directory to store our store in
	DataDir string `mapstructure:"data_dir"`

	// NumSchedulers is the number of scheduler thread that are run.
	// This can be as many as one per core, or zero to disable this server
	// from doing any scheduling work.
	NumSchedulers int `mapstructure:"num_schedulers"`

	// EnabledSchedulers controls the set of sub-schedulers that are
	// enabled for this server to handle. This will restrict the evaluations
	// that the workers dequeue for processing.
	EnabledSchedulers []string `mapstructure:"enabled_schedulers"`

	// HeartbeatGrace is the grace period beyond the TTL to account for network,
	// processing delays and clock skew before marking a node as "down".
	HeartbeatGrace string `mapstructure:"heartbeat_grace"`

	// StartJoin is a list of addresses to attempt to join when the
	// agent starts. If Serf is unable to communicate with any of these
	// addresses, then the agent will error and exit.
	StartJoin []string `mapstructure:"join"`

	// RetryMaxAttempts specifies the maximum number of times to retry joining a
	// host on startup. This is useful for cases where we know the node will be
	// online eventually.
	RetryMaxAttempts int `mapstructure:"retry_max"`

	// RetryInterval specifies the amount of time to wait in between join
	// attempts on agent start. The minimum allowed value is 1 second and
	// the default is 30s.
	RetryInterval string        `mapstructure:"retry_interval"`
	retryInterval time.Duration `mapstructure:"-"`
}

type Network struct {
	// MAX_PAYLOAD is the maximum allowed payload size. Should be using
	// something different if > 1MB payloads are needed.
	MaxPayload int `mapstructure:"max_payload"`
}

type Metric struct {
	DisableHostname          bool          `mapstructure:"disable_hostname"`
	UseNodeName              bool          `mapstructure:"use_node_name"`
	CollectionInterval       string        `mapstructure:"collection_interval"`
	collectionInterval       time.Duration `mapstructure:"-"`
	PublishAllocationMetrics bool          `mapstructure:"publish_allocation_metrics"`
	PublishNodeMetrics       bool          `mapstructure:"publish_node_metrics"`
}

// Ports encapsulates the various ports we bind to for network services. If any
// are not specified then the defaults are used instead.
type Ports struct {
	HTTP int `mapstructure:"http"`
	RPC  int `mapstructure:"rpc"`
	Serf int `mapstructure:"serf"`
	Nats int `mapstructure:"nats"`
}

// Addresses encapsulates all of the addresses we bind to for various
// network services. Everything is optional and defaults to BindAddr.
type Addresses struct {
	HTTP string `mapstructure:"http"`
	RPC  string `mapstructure:"rpc"`
	Serf string `mapstructure:"serf"`
	Nats string `mapstructure:"nats"`
}

// AdvertiseAddrs is used to control the addresses we advertise out for
// different network services. All are optional and default to BindAddr and
// their default Port.
type AdvertiseAddrs struct {
	HTTP string `mapstructure:"http"`
	RPC  string `mapstructure:"rpc"`
	Serf string `mapstructure:"serf"`
	Nats string `mapstructure:"nats"`
}

type ZTreeData struct {
	Code  string
	Name  string
	Nodes []*Node
}

type Node struct {
	Code string
	Name string
}

// DefaultConfig is a the baseline configuration for Udup
func DefaultConfig() *Config {
	return &Config{
		LogLevel:    "INFO",
		LogFile:     "/var/log/dtle/dtle.log",
		LogToStdout: false,
		PidFile:     "/var/run/dtle/dtle.pid",
		Region:      "global",
		Datacenter:  "dc1",
		BindAddr:    "0.0.0.0",
		Ports: &Ports{
			HTTP: 8190,
			RPC:  8191,
			Serf: 8192,
			Nats: 8193,
		},
		Addresses: &Addresses{
			HTTP: DefaultAddr,
			RPC:  DefaultAddr,
			Serf: DefaultAddr,
			Nats: DefaultAddr,
		},
		AdvertiseAddrs: &AdvertiseAddrs{
			Nats: DefaultAddr,
		},
		Consul: uconf.DefaultConsulConfig(),
		Client: &ClientConfig{
			Enabled:    false,
			NoHostUUID: true,
		},
		Server: &ServerConfig{
			Enabled:          false,
			StartJoin:        []string{},
			RetryInterval:    "15s",
			HeartbeatGrace:   "30s",
			RetryMaxAttempts: 3,
		},
		Metric: &Metric{
			CollectionInterval: "1s",
			collectionInterval: 1 * time.Second,
		},
		Network: &Network{
			MaxPayload: DefaultMaxPayload,
		},
		DtleSchemaName: "dtle",
	}
}

// Listener can be used to get a new listener using a custom bind address.
// If the bind provided address is empty, the BindAddr is used instead.
func (c *Config) Listener(proto, addr string, port int) (net.Listener, error) {
	if addr == "" {
		addr = c.BindAddr
	}

	// Do our own range check to avoid bugs in package net.
	//
	//   golang.org/issue/11715
	//   golang.org/issue/13447
	//
	// Both of the above bugs were fixed by golang.org/cl/12447 which will be
	// included in Go 1.6. The error returned below is the same as what Go 1.6
	// will return.
	if 0 > port || port > 65535 {
		return nil, &net.OpError{
			Op:  "listen",
			Net: proto,
			Err: &net.AddrError{Err: "invalid port", Addr: fmt.Sprint(port)},
		}
	}
	return net.Listen(proto, net.JoinHostPort(addr, strconv.Itoa(port)))
}

// Merge merges two configurations.
func (c *Config) Merge(b *Config) *Config {
	result := *c

	if b.Region != "" {
		result.Region = b.Region
	}
	if b.Datacenter != "" {
		result.Datacenter = b.Datacenter
	}
	if b.NodeName != "" {
		result.NodeName = b.NodeName
	}
	if b.DataDir != "" {
		result.DataDir = b.DataDir
	}
	if b.EnableUi {
		result.EnableUi = b.EnableUi
	}
	if b.UiDir != "" {
		result.UiDir = b.UiDir
	}
	if b.LogLevel != "" {
		result.LogLevel = b.LogLevel
	}
	if b.LogFile != "" {
		result.LogFile = b.LogFile
	}
	if b.LogToStdout {
		result.LogToStdout = b.LogToStdout
	}
	if b.PidFile != "" {
		result.PidFile = b.PidFile
	}
	if b.BindAddr != "" {
		result.BindAddr = b.BindAddr
	}
	if b.Profile != "" {
		result.Profile = b.Profile
	}
	if b.LeaveOnInt {
		result.LeaveOnInt = true
	}
	if b.LeaveOnTerm {
		result.LeaveOnTerm = true
	}

	// Apply the metric config
	if result.Metric == nil && b.Metric != nil {
		metric := *b.Metric
		result.Metric = &metric
	} else if b.Metric != nil {
		result.Metric = result.Metric.Merge(b.Metric)
	}

	// Apply the network config
	if result.Network == nil && b.Network != nil {
		network := *b.Network
		result.Network = &network
	} else if b.Network != nil {
		result.Network = result.Network.Merge(b.Network)
	}

	// Apply the client config
	if result.Client == nil && b.Client != nil {
		client := *b.Client
		result.Client = &client
	} else if b.Client != nil {
		result.Client = result.Client.Merge(b.Client)
	}

	// Apply the server config
	if result.Server == nil && b.Server != nil {
		server := *b.Server
		result.Server = &server
	} else if b.Server != nil {
		result.Server = result.Server.Merge(b.Server)
	}

	// Apply the ports config
	if result.Ports == nil && b.Ports != nil {
		ports := *b.Ports
		result.Ports = &ports
	} else if b.Ports != nil {
		result.Ports = result.Ports.Merge(b.Ports)
	}

	// Apply the address config
	if result.Addresses == nil && b.Addresses != nil {
		addrs := *b.Addresses
		result.Addresses = &addrs
	} else if b.Addresses != nil {
		result.Addresses = result.Addresses.Merge(b.Addresses)
	}

	// Apply the advertise addrs config
	if result.AdvertiseAddrs == nil && b.AdvertiseAddrs != nil {
		advertise := *b.AdvertiseAddrs
		result.AdvertiseAddrs = &advertise
	} else if b.AdvertiseAddrs != nil {
		result.AdvertiseAddrs = result.AdvertiseAddrs.Merge(b.AdvertiseAddrs)
	}

	// Apply the Consul Configuration
	if result.Consul == nil && b.Consul != nil {
		result.Consul = b.Consul.Copy()
	} else if b.Consul != nil {
		result.Consul = result.Consul.Merge(b.Consul)
	}

	// Merge config files lists
	result.Files = append(result.Files, b.Files...)

	// Add the http API response header map values
	if result.HTTPAPIResponseHeaders == nil {
		result.HTTPAPIResponseHeaders = make(map[string]string)
	}
	for k, v := range b.HTTPAPIResponseHeaders {
		result.HTTPAPIResponseHeaders[k] = v
	}

	if b.DtleSchemaName != "" {
		result.DtleSchemaName = b.DtleSchemaName
	}

	return &result
}

// normalizeAddrs normalizes Addresses and AdvertiseAddrs to always be
// initialized and have sane defaults.
func (c *Config) normalizeAddrs() error {
	c.Addresses.HTTP = normalizeBind(c.Addresses.HTTP, c.BindAddr)
	c.Addresses.RPC = normalizeBind(c.Addresses.RPC, c.BindAddr)
	c.Addresses.Serf = normalizeBind(c.Addresses.Serf, c.BindAddr)
	c.Addresses.Nats = normalizeBind(c.Addresses.Nats, c.BindAddr)
	c.normalizedAddrs = &Addresses{
		HTTP: net.JoinHostPort(c.Addresses.HTTP, strconv.Itoa(c.Ports.HTTP)),
		RPC:  net.JoinHostPort(c.Addresses.RPC, strconv.Itoa(c.Ports.RPC)),
		Serf: net.JoinHostPort(c.Addresses.Serf, strconv.Itoa(c.Ports.Serf)),
		Nats: net.JoinHostPort(c.Addresses.Nats, strconv.Itoa(c.Ports.Nats)),
	}

	addr, err := normalizeAdvertise(c.AdvertiseAddrs.HTTP, c.Addresses.HTTP, c.Ports.HTTP)
	if err != nil {
		return fmt.Errorf("Failed to parse HTTP advertise address: %v", err)
	}
	c.AdvertiseAddrs.HTTP = addr

	addr, err = normalizeAdvertise(c.AdvertiseAddrs.RPC, c.Addresses.RPC, c.Ports.RPC)
	if err != nil {
		return fmt.Errorf("Failed to parse RPC advertise address: %v", err)
	}
	c.AdvertiseAddrs.RPC = addr

	addr, err = normalizeAdvertise(c.AdvertiseAddrs.Nats, c.Addresses.Nats, c.Ports.Nats)
	if err != nil {
		return fmt.Errorf("Failed to parse Nats advertise address: %v", err)
	}
	c.AdvertiseAddrs.Nats = addr

	// Skip serf if server is disabled
	if c.Server != nil && c.Server.Enabled {
		addr, err = normalizeAdvertise(c.AdvertiseAddrs.Serf, c.Addresses.Serf, c.Ports.Serf)
		if err != nil {
			return fmt.Errorf("Failed to parse Serf advertise address: %v", err)
		}
		c.AdvertiseAddrs.Serf = addr
	}

	return nil
}

// normalizeBind returns a normalized bind address.
//
// If addr is set it is used, if not the default bind address is used.
func normalizeBind(addr, bind string) string {
	if addr == "" {
		return bind
	}
	return addr
}

// normalizeAdvertise returns a normalized advertise address.
//
// If addr is set, it is used and the default port is appended if no port is
// set.
//
// If addr is not set and bind is a valid address, the returned string is the
// bind+port.
//
// If addr is not set and bind is not a valid advertise address, the hostname
// is resolved and returned with the port.
//
// Loopback is only considered a valid advertise address in dev mode.
func normalizeAdvertise(addr string, bind string, defport int) (string, error) {
	if addr != "" {
		// Default to using manually configured address
		_, _, err := net.SplitHostPort(addr)
		if err != nil {
			if !isMissingPort(err) {
				return "", fmt.Errorf("Error parsing advertise address %q: %v", addr, err)
			}

			// missing port, append the default
			return net.JoinHostPort(addr, strconv.Itoa(defport)), nil
		}
		return addr, nil
	}

	// Fallback to bind address first, and then try resolving the local hostname
	ips, err := net.LookupIP(bind)
	if err != nil {
		return "", fmt.Errorf("Error resolving bind address %q: %v", bind, err)
	}

	// Return the first unicast address
	for _, ip := range ips {
		if ip.IsLinkLocalUnicast() || ip.IsGlobalUnicast() {
			return net.JoinHostPort(ip.String(), strconv.Itoa(defport)), nil
		}
		if ip.IsLoopback() {
			// loopback is fine for dev mode
			return net.JoinHostPort(ip.String(), strconv.Itoa(defport)), nil
		}
	}

	// As a last resort resolve the hostname and use it if it's not
	// localhost (as localhost is never a sensible default)
	host, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("Unable to get hostname to set advertise address: %v", err)
	}

	ips, err = net.LookupIP(host)
	if err != nil {
		return "", fmt.Errorf("Error resolving hostname %q for advertise address: %v", host, err)
	}

	// Return the first unicast address
	for _, ip := range ips {
		if ip.IsLinkLocalUnicast() || ip.IsGlobalUnicast() {
			return net.JoinHostPort(ip.String(), strconv.Itoa(defport)), nil
		}
		if ip.IsLoopback() {
			// loopback is fine for dev mode
			return net.JoinHostPort(ip.String(), strconv.Itoa(defport)), nil
		}
	}
	return "", fmt.Errorf("No valid advertise addresses, please set `advertise` manually")
}

// isMissingPort returns true if an error is a "missing port" error from
// net.SplitHostPort.
func isMissingPort(err error) bool {
	// matches error const in net/ipsock.go
	const missingPort = "missing port in address"
	return err != nil && strings.Contains(err.Error(), missingPort)
}

// Merge is used to merge two server configs together
func (a *ServerConfig) Merge(b *ServerConfig) *ServerConfig {
	result := *a

	if b.Enabled {
		result.Enabled = true
	}
	if b.DataDir != "" {
		result.DataDir = b.DataDir
	}
	if b.NumSchedulers != 0 {
		result.NumSchedulers = b.NumSchedulers
	}
	if b.HeartbeatGrace != "" {
		result.HeartbeatGrace = b.HeartbeatGrace
	}
	if b.RetryMaxAttempts != 0 {
		result.RetryMaxAttempts = b.RetryMaxAttempts
	}
	if b.RetryInterval != "" {
		result.RetryInterval = b.RetryInterval
		result.retryInterval = b.retryInterval
	}
	// Add the schedulers
	result.EnabledSchedulers = append(result.EnabledSchedulers, b.EnabledSchedulers...)

	// Copy the start join addresses
	result.StartJoin = make([]string, 0, len(a.StartJoin)+len(b.StartJoin))
	result.StartJoin = append(result.StartJoin, a.StartJoin...)
	result.StartJoin = append(result.StartJoin, b.StartJoin...)

	if b.BootstrapExpect > 0 {
		result.BootstrapExpect = b.BootstrapExpect
	} else {
		result.BootstrapExpect = len(result.StartJoin)
	}

	return &result
}

// Merge is used to merge two client configs together
func (a *ClientConfig) Merge(b *ClientConfig) *ClientConfig {
	result := *a

	if b.Enabled {
		result.Enabled = true
	}
	if b.StateDir != "" {
		result.StateDir = b.StateDir
	}
	if b.NoHostUUID {
		result.NoHostUUID = b.NoHostUUID
	}

	// Add the servers
	result.Servers = append(result.Servers, b.Servers...)

	return &result
}

func (a *Network) Merge(b *Network) *Network {
	result := *a

	if b.MaxPayload != 0 {
		result.MaxPayload = b.MaxPayload
	}
	return &result
}

// Merge is used to merge two metric configs together
func (a *Metric) Merge(b *Metric) *Metric {
	result := *a

	if b.DisableHostname {
		result.DisableHostname = true
	}
	if b.CollectionInterval != "" {
		result.CollectionInterval = b.CollectionInterval
	}
	if b.collectionInterval != 0 {
		result.collectionInterval = b.collectionInterval
	}
	if b.PublishNodeMetrics {
		result.PublishNodeMetrics = true
	}
	if b.PublishAllocationMetrics {
		result.PublishAllocationMetrics = true
	}
	return &result
}

// Merge is used to merge two port configurations.
func (a *Ports) Merge(b *Ports) *Ports {
	result := *a

	if b.HTTP != 0 {
		result.HTTP = b.HTTP
	}
	if b.RPC != 0 {
		result.RPC = b.RPC
	}
	if b.Serf != 0 {
		result.Serf = b.Serf
	}
	if b.Nats != 0 {
		result.Nats = b.Nats
	}
	return &result
}

// Merge is used to merge two address configs together.
func (a *Addresses) Merge(b *Addresses) *Addresses {
	result := *a

	if b.HTTP != "" {
		result.HTTP = b.HTTP
	}
	if b.RPC != "" {
		result.RPC = b.RPC
	}
	if b.Serf != "" {
		result.Serf = b.Serf
	}
	if b.Nats != "" {
		result.Nats = b.Nats
	}
	return &result
}

// Merge merges two advertise addrs configs together.
func (a *AdvertiseAddrs) Merge(b *AdvertiseAddrs) *AdvertiseAddrs {
	result := *a

	if b.RPC != "" {
		result.RPC = b.RPC
	}
	if b.Serf != "" {
		result.Serf = b.Serf
	}
	if b.HTTP != "" {
		result.HTTP = b.HTTP
	}
	if b.Nats != "" {
		result.Nats = b.Nats
	}
	return &result
}

// LoadConfig loads the configuration at the given path, regardless if
// its a file or directory.
func LoadConfig(path string) (*Config, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if fi.IsDir() {
		return LoadConfigDir(path)
	}

	cleaned := filepath.Clean(path)
	config, err := ParseConfigFile(cleaned)
	if err != nil {
		return nil, fmt.Errorf("Error loading %s: %s", cleaned, err)
	}

	config.Files = append(config.Files, cleaned)
	return config, nil
}

// LoadConfigDir loads all the configurations in the given directory
// in alphabetical order.
func LoadConfigDir(dir string) (*Config, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf(
			"configuration path must be a directory: %s", dir)
	}

	var files []string
	err = nil
	for err != io.EOF {
		var fis []os.FileInfo
		fis, err = f.Readdir(128)
		if err != nil && err != io.EOF {
			return nil, err
		}

		for _, fi := range fis {
			// Ignore directories
			if fi.IsDir() {
				continue
			}

			// Only care about files that are valid to load.
			name := fi.Name()
			skip := true
			if strings.HasSuffix(name, ".hcl") {
				skip = false
			} else if strings.HasSuffix(name, ".json") {
				skip = false
			}
			if skip || isTemporaryFile(name) {
				continue
			}

			path := filepath.Join(dir, name)
			files = append(files, path)
		}
	}

	// Fast-path if we have no files
	if len(files) == 0 {
		return &Config{}, nil
	}

	sort.Strings(files)

	var result *Config
	for _, f := range files {
		config, err := ParseConfigFile(f)
		if err != nil {
			return nil, fmt.Errorf("Error loading %s: %s", f, err)
		}
		config.Files = append(config.Files, f)

		if result == nil {
			result = config
		} else {
			result = result.Merge(config)
		}
	}

	return result, nil
}

// isTemporaryFile returns true or false depending on whether the
// provided file name is a temporary file for the following editors:
// emacs or vim.
func isTemporaryFile(name string) bool {
	return strings.HasSuffix(name, "~") || // vim
		strings.HasPrefix(name, ".#") || // emacs
		(strings.HasPrefix(name, "#") && strings.HasSuffix(name, "#")) // emacs
}
