package config

import (
	"fmt"
	"path/filepath"
)

// Config is the configuration for the Udup agent.
type Config struct {
	LogLevel  string `mapstructure:"log_level"`
	LogFile   string `mapstructure:"log_file"`
	LogRotate string `mapstructure:"log_rotate"`

	//Ref:http://dev.mysql.com/doc/refman/5.7/en/replication-options-slave.html#option_mysqld_replicate-do-table
	ReplicateDoTable []TableName `mapstructure:"replicate_do_table"`
	ReplicateDoDb    []string    `mapstructure:"replicate_do_db"`
	// extract related settings
	Extract *ExtractorConfig `mapstructure:"extract"`
	// apply related settings
	Apply *ApplierConfig `mapstructure:"apply"`

	// config file that have been loaded (in order)
	PidFile    string `mapstructure:"pid_file"`
	File       string `mapstructure:"-"`
	PanicAbort chan error
}

type ExtractorConfig struct {
	// Enabled controls if we are a Extract
	Enabled  bool              `mapstructure:"enabled"`
	NatsAddr string            `mapstructure:"nats_addr"`
	ServerID int               `mapstructure:"server_id"`
	ConnCfg  *ConnectionConfig `mapstructure:"conn_cfg"`
}

type ApplierConfig struct {
	// Enabled controls if we are a Apply
	Enabled      bool              `mapstructure:"enabled"`
	NatsAddr     string            `mapstructure:"nats_addr"`
	StoreType    string            `mapstructure:"nats_store_type"`
	FilestoreDir string            `mapstructure:"nats_file_store_dir"`
	WorkerCount  int               `mapstructure:"worker_count"`
	Batch        int               `mapstructure:"batch"`
	ConnCfg      *ConnectionConfig `mapstructure:"conn_cfg"`
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
		File:     "udup.conf",
		LogLevel: "INFO",
		Extract: &ExtractorConfig{
			Enabled:  false,
			ServerID: 100,
			ConnCfg: &ConnectionConfig{
				Host:     "127.0.0.1",
				Port:     3306,
				User:     "mysql",
				Password: "pwd",
			},
		},
		Apply: &ApplierConfig{
			Enabled:     false,
			WorkerCount: 1,
			Batch:       1,
			ConnCfg: &ConnectionConfig{
				Host:     "127.0.0.1",
				Port:     3307,
				User:     "mysql",
				Password: "pwd",
			},
		},
		PanicAbort: make(chan error),
	}
}

// Merge merges two configurations.
func (c *Config) Merge(b *Config) *Config {
	result := *c

	if b.LogLevel != "" {
		result.LogLevel = b.LogLevel
	}

	if b.LogFile != "" {
		result.LogFile = b.LogFile
	}

	if b.LogRotate != "" {
		result.LogRotate = b.LogRotate
	}

	// Add the DoDBs
	result.ReplicateDoDb = append(result.ReplicateDoDb, b.ReplicateDoDb...)

	// Add the DoTables
	result.ReplicateDoTable = append(result.ReplicateDoTable, b.ReplicateDoTable...)

	// Apply the extract config
	if result.Extract == nil && b.Extract != nil {
		extractor := *b.Extract
		result.Extract = &extractor
	} else if b.Extract != nil {
		result.Extract = result.Extract.Merge(b.Extract)
	}

	if result.Apply == nil && b.Apply != nil {
		applier := *b.Apply
		result.Apply = &applier
	} else if b.Apply != nil {
		result.Apply = result.Apply.Merge(b.Apply)
	}

	if b.PidFile != "" {
		result.PidFile = b.PidFile
	}

	if b.File != "" {
		result.File = b.File
	}

	return &result
}

// Merge is used to merge two server configs together
func (a *ExtractorConfig) Merge(b *ExtractorConfig) *ExtractorConfig {
	result := *a

	if b.Enabled {
		result.Enabled = true
	}
	if b.NatsAddr != "" {
		result.NatsAddr = b.NatsAddr
	}

	if b.ServerID != 0 {
		result.ServerID = b.ServerID
	}

	if result.ConnCfg == nil && b.ConnCfg != nil {
		cfg := *b.ConnCfg
		result.ConnCfg = &cfg
	} else if b.ConnCfg != nil {
		result.ConnCfg = result.ConnCfg.Merge(b.ConnCfg)
	}

	return &result
}

// Merge is used to merge two server configs together
func (a *ApplierConfig) Merge(b *ApplierConfig) *ApplierConfig {
	result := *a

	if b.Enabled {
		result.Enabled = true
	}
	if b.NatsAddr != "" {
		result.NatsAddr = b.NatsAddr
	}

	if b.StoreType != "" {
		result.StoreType = b.StoreType
	}

	if b.FilestoreDir != "" {
		result.FilestoreDir = b.FilestoreDir
	}

	if b.WorkerCount != 0 {
		result.WorkerCount = b.WorkerCount
	}

	if b.Batch != 0 {
		result.Batch = b.Batch
	}
	if result.ConnCfg == nil && b.ConnCfg != nil {
		cfg := *b.ConnCfg
		result.ConnCfg = &cfg
	} else if b.ConnCfg != nil {
		result.ConnCfg = result.ConnCfg.Merge(b.ConnCfg)
	}

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
