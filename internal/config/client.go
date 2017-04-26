package config

import (
	"fmt"
	"io"
	"os"
	"time"

	"udup/internal"
	"udup/internal/models"
)

// This is the default port that we use for Serf communication
const (
	DefaultBindPort  int = 8191
	DefaultClusterID     = "udup-cluster"
)

// RPCHandler can be provided to the Client if there is a local server
// to avoid going over the network. If not provided, the Client will
// maintain a connection pool to the servers
type RPCHandler interface {
	RPC(method string, args interface{}, reply interface{}) error
}

// Config is used to parameterize and configure the behavior of the client
type ClientConfig struct {
	// StateDir is where we store our state
	StateDir string

	// AllocDir is where we store data for allocations
	AllocDir string

	// LogOutput is the destination for logs
	LogOutput io.Writer

	// Region is the clients region
	Region string

	// Servers is a list of known server addresses. These are as "host:port"
	Servers []string

	// RPCHandler can be provided to avoid network traffic if the
	// server is running locally.
	RPCHandler RPCHandler

	// Node provides the base node
	Node *models.Node

	// Version is the version of the Udup client
	Version string

	// Revision is the commit number of the Udup client
	Revision string

	// ConsulConfig is this Agent's Consul configuration
	ConsulConfig *ConsulConfig

	NatsConfig *NatsConfig

	// StatsCollectionInterval is the interval at which the Udup client
	// collects resource usage stats
	StatsCollectionInterval time.Duration

	// PublishNodeMetrics determines whether server is going to publish node
	// level metrics to remote Metric sinks
	PublishNodeMetrics bool

	// PublishAllocationMetrics determines whether server is going to publish
	// allocation metrics to remote Metric sinks
	PublishAllocationMetrics bool

	// LogLevel is the level of the logs to putout
	LogLevel string

	// NoHostUUID disables using the host's UUID and will force generation of a
	// random UUID.
	NoHostUUID bool
}

func (c *ClientConfig) Copy() *ClientConfig {
	nc := new(ClientConfig)
	*nc = *c
	nc.Node = nc.Node.Copy()
	nc.Servers = internal.CopySliceString(nc.Servers)
	nc.ConsulConfig = c.ConsulConfig.Copy()
	return nc
}

type DriverCtx struct {
	DriverConfig *MySQLDriverConfig
}

type NatsConfig struct {
	Addr         string `mapstructure:"address"`
	StoreType    string `mapstructure:"store_type"`
	FilestoreDir string `mapstructure:"file_store_dir"`
}

type MySQLDriverConfig struct {
	//Ref:http://dev.mysql.com/doc/refman/5.7/en/replication-options-slave.html#option_mysqld_replicate-do-table
	ReplicateDoDb []TableName
	Gtid          string
	NatsAddr      string
	WorkerCount   int
	Dsn           *Dsn
}

type Dsn struct {
	Host     string
	User     string
	Password string
	Port     int
}

func (c *Dsn) String() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// TableName is the table configuration
// slave restrict replication to a given table
type TableName struct {
	Schema string
	Table  string
}

// DefaultConfig returns the default configuration
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		NatsConfig:              DefaultNatsConfig(),
		ConsulConfig:            DefaultConsulConfig(),
		LogOutput:               os.Stderr,
		Region:                  "global",
		StatsCollectionInterval: 1 * time.Second,
		LogLevel:                "DEBUG",
	}
}

// DefaultNatsConfig() returns the canonical defaults for the Udup
// `nats` configuration.
func DefaultNatsConfig() *NatsConfig {
	return &NatsConfig{
		Addr:         "127.0.0.1:8193",
		StoreType:    "memory",
		FilestoreDir: "",
	}
}

// Merge merges two Consul Configurations together.
func (a *NatsConfig) Merge(b *NatsConfig) *NatsConfig {
	result := *a

	if b.Addr != "" {
		result.Addr = b.Addr
	}
	if b.StoreType != "" {
		result.StoreType = b.StoreType
	}
	if b.FilestoreDir != "" {
		result.FilestoreDir = b.FilestoreDir
	}
	return &result
}
