// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	column "github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb-tools/pkg/table-router"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

// task modes
const (
	ModeAll       = "all"
	ModeFull      = "full"
	ModeIncrement = "incremental"
)

var (
	defaultMaxAllowedPacket = 64 * 1024 * 1024 // 64MiB, equal to TiDB's default
)

// DBConfig is the DB configuration.
type DBConfig struct {
	Host             string `toml:"host" json:"host" yaml:"host"`
	Port             int    `toml:"port" json:"port" yaml:"port"`
	User             string `toml:"user" json:"user" yaml:"user"`
	Password         string `toml:"password" json:"-" yaml:"password"` // omit it for privacy
	MaxAllowedPacket *int   `toml:"max-allowed-packet" json:"max-allowed-packet" yaml:"max-allowed-packet"`
}

// Toml returns TOML format representation of config
func (db *DBConfig) Toml() (string, error) {
	var b bytes.Buffer
	enc := toml.NewEncoder(&b)
	err := enc.Encode(db)
	if err != nil {
		return "", errors.Trace(err)
	}
	return b.String(), nil
}

// Decode loads config from file data
func (db *DBConfig) Decode(data string) error {
	_, err := toml.Decode(data, db)
	return errors.Trace(err)
}

// Adjust adjusts the config.
func (db *DBConfig) Adjust() {
	if db.MaxAllowedPacket == nil {
		cloneV := defaultMaxAllowedPacket
		db.MaxAllowedPacket = &cloneV
	}
}

// SubTaskConfig is the configuration for SubTask
type SubTaskConfig struct {
	// BurntSushi/toml seems have a bug for flag "-"
	// when doing encoding, if we use `toml:"-"`, it still try to encode it
	// and it will panic because of unsupported type (reflect.Func)
	// so we should not export flagSet
	flagSet *flag.FlagSet

	// when in sharding, multi dm-workers do one task
	IsSharding      bool   `toml:"is-sharding" json:"is-sharding"`
	OnlineDDLScheme string `toml:"online-ddl-scheme" json:"online-ddl-scheme"`

	// handle schema/table name mode, and only for schema/table name/pattern
	// if case insensitive, we would convert schema/table name/pattern to lower case
	CaseSensitive bool `toml:"case-sensitive" json:"case-sensitive"`

	Name string `toml:"name" json:"name"`
	Mode string `toml:"mode" json:"mode"`
	//  treat it as hidden configuration
	IgnoreCheckingItems []string `toml:"ignore-checking-items" json:"ignore-checking-items"`
	// it represents a MySQL/MariaDB instance or a replica group
	SourceID                string `toml:"source-id" json:"source-id"`
	ServerID                int    `toml:"server-id" json:"server-id"`
	Flavor                  string `toml:"flavor" json:"flavor"`
	MetaSchema              string `toml:"meta-schema" json:"meta-schema"`
	RemoveMeta              bool   `toml:"remove-meta" json:"remove-meta"`
	DisableHeartbeat        bool   `toml:"disable-heartbeat" json:"disable-heartbeat"` //  deprecated, use !enable-heartbeat instead
	HeartbeatUpdateInterval int    `toml:"heartbeat-update-interval" json:"heartbeat-update-interval"`
	HeartbeatReportInterval int    `toml:"heartbeat-report-interval" json:"heartbeat-report-interval"`
	EnableHeartbeat         bool   `toml:"enable-heartbeat" json:"enable-heartbeat"`
	Meta                    *Meta  `toml:"meta" json:"meta"`
	Timezone                string `toml:"timezone" josn:"timezone"`

	BinlogType string `toml:"binlog-type" json:"binlog-type"`
	// RelayDir get value from dm-worker config
	RelayDir string   `toml:"relay-dir" json:"relay-dir"`
	From     DBConfig `toml:"from" json:"from"`
	To       DBConfig `toml:"to" json:"to"`

	RouteRules         []*router.TableRule   `toml:"route-rules" json:"route-rules"`
	FilterRules        []*bf.BinlogEventRule `toml:"filter-rules" json:"filter-rules"`
	ColumnMappingRules []*column.Rule        `toml:"mapping-rule" json:"mapping-rule"`
	BWList             *filter.Rules         `toml:"black-white-list" json:"black-white-list"`

	MydumperConfig // Mydumper configuration
	LoaderConfig   // Loader configuration
	SyncerConfig   // Syncer configuration

	// compatible with standalone dm unit
	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	PprofAddr  string `toml:"pprof-addr" json:"pprof-addr"`
	StatusAddr string `toml:"status-addr" json:"status-addr"`

	ConfigFile string `toml:"-" json:"config-file"`

	// still needed by Syncer / Loader bin
	printVersion bool
}

// NewSubTaskConfig creates a new SubTaskConfig
func NewSubTaskConfig() *SubTaskConfig {
	cfg := &SubTaskConfig{}
	return cfg
}

// String returns the config's json string
func (c *SubTaskConfig) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.Errorf("[config] marshal sub task dm config to json error %v", err)
	}
	return string(cfg)
}

// Toml returns TOML format representation of config
func (c *SubTaskConfig) Toml() (string, error) {
	var b bytes.Buffer
	enc := toml.NewEncoder(&b)
	err := enc.Encode(c)
	if err != nil {
		return "", errors.Trace(err)
	}
	return b.String(), nil
}

// DecodeFile loads and decodes config from file
func (c *SubTaskConfig) DecodeFile(fpath string) error {
	_, err := toml.DecodeFile(fpath, c)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(c.Adjust())
}

// Decode loads config from file data
func (c *SubTaskConfig) Decode(data string) error {
	_, err := toml.Decode(data, c)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(c.Adjust())
}

// Adjust adjusts configs
func (c *SubTaskConfig) Adjust() error {
	if c.Name == "" {
		return errors.New("task name should not be empty")
	}

	if c.SourceID == "" {
		return errors.NotValidf("empty source-id")
	}
	if len(c.SourceID) > MaxSourceIDLength {
		return errors.NotValidf("too long source-id")
	}

	//if c.Flavor != mysql.MySQLFlavor && c.Flavor != mysql.MariaDBFlavor {
	//	return errors.Errorf("please specify right mysql version, support mysql, mariadb now")
	//}

	if c.OnlineDDLScheme != "" && c.OnlineDDLScheme != PT && c.OnlineDDLScheme != GHOST {
		return errors.NotSupportedf("online scheme %s", c.OnlineDDLScheme)
	}

	if c.MetaSchema == "" {
		c.MetaSchema = defaultMetaSchema
	}

	if c.MaxRetry == 0 {
		c.MaxRetry = 1
	}

	if !c.DisableHeartbeat {
		c.EnableHeartbeat = true
	}

	if c.Timezone != "" {
		_, err := time.LoadLocation(c.Timezone)
		if err != nil {
			return errors.Annotatef(err, "invalid timezone string: %s", c.Timezone)
		}
	}

	dirSuffix := "." + c.Name
	if !strings.HasSuffix(c.LoaderConfig.Dir, dirSuffix) { // check to support multiple times calling
		// if not ends with the task name, we append the task name to the tail
		c.LoaderConfig.Dir += dirSuffix
	}

	c.From.Adjust()
	c.To.Adjust()

	_, err := c.DecryptPassword()
	return err
}

// Parse parses flag definitions from the argument list.
func (c *SubTaskConfig) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if c.printVersion {
		fmt.Println(utils.GetRawInfo())
		return flag.ErrHelp
	}

	// Load config file if specified.
	if c.ConfigFile != "" {
		err = c.DecodeFile(c.ConfigFile)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options.
	err = c.flagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.flagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.flagSet.Arg(0))
	}

	return errors.Trace(c.Adjust())
}

// DecryptPassword tries to decrypt db password in config
func (c *SubTaskConfig) DecryptPassword() (*SubTaskConfig, error) {
	clone, err := c.Clone()
	if err != nil {
		return nil, errors.Trace(err)
	}

	var (
		pswdTo   string
		pswdFrom string
	)
	if len(clone.To.Password) > 0 {
		pswdTo, err = utils.Decrypt(clone.To.Password)
		if err != nil {
			return nil, errors.Annotatef(err, "downstream DB password %s", clone.To.Password)
		}
	}
	if len(clone.From.Password) > 0 {
		pswdFrom, err = utils.Decrypt(clone.From.Password)
		if err != nil {
			return nil, errors.Annotatef(err, "source DB password %s", clone.From.Password)
		}
	}
	clone.From.Password = pswdFrom
	clone.To.Password = pswdTo

	return clone, nil
}

// Clone returns a replica of SubTaskConfig
func (c *SubTaskConfig) Clone() (*SubTaskConfig, error) {
	content, err := c.Toml()
	if err != nil {
		return nil, errors.Trace(err)
	}

	clone := &SubTaskConfig{}
	_, err = toml.Decode(content, clone)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return clone, nil
}
