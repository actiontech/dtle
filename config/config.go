package config

import (
	"fmt"
	"net"
	"path/filepath"
	"time"
)

// This is the default port that we use for Serf communication
const DefaultBindPort int = 8191

// Config is the configuration for the Udup agent.
type Config struct {
	LogLevel  string `mapstructure:"log_level"`
	LogFile   string `mapstructure:"log_file"`
	LogRotate string `mapstructure:"log_rotate"`
	// Region is the region this agent is in. Defaults to global.
	Region string
	// Datacenter is the datacenter this agent is in. Defaults to dc1
	Datacenter string
	// NodeName is the name we register as. Defaults to hostname.
	NodeName string `mapstructure:"name"`
	// BindAddr is the address on which all of nomad's services will
	// be bound. If not specified, this defaults to 127.0.0.1.
	BindAddr              string `mapstructure:"bind_addr"`
	HTTPAddr              string `mapstructure:"http_addr"`
	Interface             string
	ReconnectInterval     time.Duration `mapstructure:"reconnect_interval"`
	ReconnectTimeout      time.Duration `mapstructure:"reconnect_timeout"`
	TombstoneTimeout      time.Duration `mapstructure:"tombstone_timeout"`
	DisableNameResolution bool
	RejoinAfterLeave      bool `mapstructure:"rejoin"`
	Server                bool
	// StartJoin is a list of addresses to attempt to join when the
	// agent starts. If Serf is unable to communicate with any of these
	// addresses, then the agent will error and exit.
	StartJoin []string `mapstructure:"start_join"`
	Version   string

	Consul 	*ConsulConfig `mapstructure:"consul"`

	// config file that have been loaded (in order)
	PidFile    string `mapstructure:"pid_file"`
	File       string `mapstructure:"-"`
	PanicAbort chan error
}

type ConsulConfig struct {
	Addresses []string            `mapstructure:"addresses"`
}

// ConnectionConfig is the DB configuration.
type ConnectionConfig struct {
	Host string `mapstructure:"host"`

	User string `mapstructure:"user"`

	Password string `mapstructure:"password"`

	Port int `mapstructure:"port"`
}

// TableName is the table configuration
// slave restrict replication to a given table
type TableName struct {
	Schema string `mapstructure:"db_name"`
	Name   string `mapstructure:"tbl_name"`
}

// DefaultConfig is a the baseline configuration for Udup
func DefaultConfig() *Config {
	return &Config{
		File:       "udup.conf",
		LogLevel:   "INFO",
		PanicAbort: make(chan error),
	}
}

// Merge merges two configurations.
func (c *Config) Merge(b *Config) *Config {
	result := *c

	if b.NodeName != "" {
		result.NodeName = b.NodeName
	}

	if b.LogLevel != "" {
		result.LogLevel = b.LogLevel
	}

	if b.LogFile != "" {
		result.LogFile = b.LogFile
	}

	if b.LogRotate != "" {
		result.LogRotate = b.LogRotate
	}

	if b.Region != "" {
		result.Region = b.Region
	}

	if b.Datacenter != "" {
		result.Datacenter = b.Datacenter
	}

	if b.BindAddr != "" {
		result.BindAddr = b.BindAddr
	}

	if b.HTTPAddr != "" {
		result.HTTPAddr = b.HTTPAddr
	}

	result.StartJoin = append(result.StartJoin, b.StartJoin...)

	if b.Server {
		result.Server = true
	}

	if result.Consul == nil && b.Consul != nil {
		consul := *b.Consul
		result.Consul = &consul
	} else if b.Consul != nil {
		result.Consul = result.Consul.Merge(b.Consul)
	}

	if b.PidFile != "" {
		result.PidFile = b.PidFile
	}

	if b.File != "" {
		result.File = b.File
	}

	return &result
}

// Merge merges two Atlas configurations together.
func (a *ConsulConfig) Merge(b *ConsulConfig) *ConsulConfig {
	result := *a

	result.Addresses = append(result.Addresses, b.Addresses...)
	return &result
}

// Merge merges two Atlas configurations together.
func (a *ConnectionConfig) Merge(b *ConnectionConfig) *ConnectionConfig {
	result := *a

	if b.Host != "" {
		result.Host = b.Host
	}
	if b.Port != 0 {
		result.Port = b.Port
	}
	if b.User != "" {
		result.User = b.User
	}
	if b.Password != "" {
		result.Password = b.Password
	}
	return &result
}

// LoadConfig loads the configuration at the given path, regardless if
// its a file or directory.
func LoadConfig(path string) (*Config, error) {
	cleaned := filepath.Clean(path)
	config, err := ParseConfigFile(cleaned)
	if err != nil {
		return nil, fmt.Errorf("Error loading %s: %s", cleaned, err)
	}

	config.File = cleaned
	return config, nil
}

// AddrParts returns the parts of the BindAddr that should be
// used to configure Serf.
func (c *Config) AddrParts(address string) (string, int, error) {
	checkAddr := address

START:
	_, _, err := net.SplitHostPort(checkAddr)
	if ae, ok := err.(*net.AddrError); ok && ae.Err == "missing port in address" {
		checkAddr = fmt.Sprintf("%s:%d", checkAddr, DefaultBindPort)
		goto START
	}
	if err != nil {
		return "", 0, err
	}

	// Get the address
	addr, err := net.ResolveTCPAddr("tcp", checkAddr)
	if err != nil {
		return "", 0, err
	}

	return addr.IP.String(), addr.Port, nil
}

// Networkinterface is used to get the associated network
// interface from the configured value
func (c *Config) NetworkInterface() (*net.Interface, error) {
	if c.Interface == "" {
		return nil, nil
	}
	return net.InterfaceByName(c.Interface)
}
