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
	"regexp"
	"strings"

	"github.com/BurntSushi/toml"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	lcfg "github.com/pingcap/tidb/br/pkg/lightning/config"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/dumpling"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

// task modes.
const (
	ModeAll       = "all"
	ModeFull      = "full"
	ModeIncrement = "incremental"

	DefaultShadowTableRules = "^_(.+)_(?:new|gho)$"
	DefaultTrashTableRules  = "^_(.+)_(?:ghc|del|old)$"

	ShadowTableRules              = "shadow-table-rules"
	TrashTableRules               = "trash-table-rules"
	TiDBLightningCheckpointPrefix = "tidb_lightning_checkpoint_"
)

var defaultMaxIdleConns = 2

// RawDBConfig contains some low level database config.
type RawDBConfig struct {
	MaxIdleConns int
	ReadTimeout  string
	WriteTimeout string
}

// DefaultRawDBConfig returns a default raw database config.
func DefaultRawDBConfig() *RawDBConfig {
	return &RawDBConfig{
		MaxIdleConns: defaultMaxIdleConns,
	}
}

// SetReadTimeout set readTimeout for raw database config.
func (c *RawDBConfig) SetReadTimeout(readTimeout string) *RawDBConfig {
	c.ReadTimeout = readTimeout
	return c
}

// SetWriteTimeout set writeTimeout for raw database config.
func (c *RawDBConfig) SetWriteTimeout(writeTimeout string) *RawDBConfig {
	c.WriteTimeout = writeTimeout
	return c
}

// SetMaxIdleConns set maxIdleConns for raw database config
// set value <= 0 then no idle connections are retained.
// set value > 0 then `value` idle connections are retained.
func (c *RawDBConfig) SetMaxIdleConns(value int) *RawDBConfig {
	c.MaxIdleConns = value
	return c
}

// DBConfig is the DB configuration.
type DBConfig struct {
	Host     string `toml:"host" json:"host" yaml:"host"`
	Port     int    `toml:"port" json:"port" yaml:"port"`
	User     string `toml:"user" json:"user" yaml:"user"`
	Password string `toml:"password" json:"-" yaml:"password"` // omit it for privacy
	// deprecated, mysql driver could automatically fetch this value
	MaxAllowedPacket *int              `toml:"max-allowed-packet" json:"max-allowed-packet" yaml:"max-allowed-packet"`
	Session          map[string]string `toml:"session" json:"session" yaml:"session"`

	// security config
	Security *Security `toml:"security" json:"security" yaml:"security"`

	RawDBCfg *RawDBConfig `toml:"-" json:"-" yaml:"-"`
}

func (db *DBConfig) String() string {
	cfg, err := json.Marshal(db)
	if err != nil {
		log.L().Error("fail to marshal config to json", log.ShortError(err))
	}
	return string(cfg)
}

// Toml returns TOML format representation of config.
func (db *DBConfig) Toml() (string, error) {
	var b bytes.Buffer
	enc := toml.NewEncoder(&b)
	if err := enc.Encode(db); err != nil {
		return "", terror.ErrConfigTomlTransform.Delegate(err, "encode db config to toml")
	}
	return b.String(), nil
}

// Decode loads config from file data.
func (db *DBConfig) Decode(data string) error {
	_, err := toml.Decode(data, db)
	return terror.ErrConfigTomlTransform.Delegate(err, "decode db config")
}

// Adjust adjusts the config.
func (db *DBConfig) Adjust() {
	// force set session time zone to UTC here.
	AdjustTargetDBTimeZone(db)
	if len(db.Password) > 0 {
		db.Password = utils.DecryptOrPlaintext(db.Password)
	}
}

// Clone returns a deep copy of DBConfig. This function only fixes data race when adjusting Session.
func (db *DBConfig) Clone() *DBConfig {
	if db == nil {
		return nil
	}

	clone := *db

	if db.MaxAllowedPacket != nil {
		packet := *(db.MaxAllowedPacket)
		clone.MaxAllowedPacket = &packet
	}

	if db.Session != nil {
		clone.Session = make(map[string]string, len(db.Session))
		for k, v := range db.Session {
			clone.Session[k] = v
		}
	}

	clone.Security = db.Security.Clone()

	if db.RawDBCfg != nil {
		dbCfg := *(db.RawDBCfg)
		clone.RawDBCfg = &dbCfg
	}

	return &clone
}

// GetDBConfigForTest is a helper function to get db config for unit test .
func GetDBConfigForTest() DBConfig {
	return DBConfig{Host: "localhost", User: "root", Password: "not a real password", Port: 3306}
}

// TiDBExtraConfig is the extra DB configuration only for TiDB.
type TiDBExtraConfig struct {
	StatusPort int    `toml:"status-port" json:"status-port" yaml:"status-port"`
	PdAddr     string `toml:"pd-addr" json:"pd-addr" yaml:"pd-addr"`
	Backend    string `toml:"backend" json:"backend" yaml:"backend"`
}

func (db *TiDBExtraConfig) String() string {
	cfg, err := json.Marshal(db)
	if err != nil {
		log.L().Error("fail to marshal config to json", log.ShortError(err))
	}
	return string(cfg)
}

// Toml returns TOML format representation of config.
func (db *TiDBExtraConfig) Toml() (string, error) {
	var b bytes.Buffer
	enc := toml.NewEncoder(&b)
	if err := enc.Encode(db); err != nil {
		return "", terror.ErrConfigTomlTransform.Delegate(err, "encode db config to toml")
	}
	return b.String(), nil
}

// Decode loads config from file data.
func (db *TiDBExtraConfig) Decode(data string) error {
	_, err := toml.Decode(data, db)
	return terror.ErrConfigTomlTransform.Delegate(err, "decode db config")
}

// SubTaskConfig is the configuration for SubTask.
type SubTaskConfig struct {
	// BurntSushi/toml seems have a bug for flag "-"
	// when doing encoding, if we use `toml:"-"`, it still try to encode it
	// and it will panic because of unsupported type (reflect.Func)
	// so we should not export flagSet
	flagSet *flag.FlagSet

	// when in sharding, multi dm-workers do one task
	IsSharding bool   `toml:"is-sharding" json:"is-sharding"`
	ShardMode  string `toml:"shard-mode" json:"shard-mode"`
	OnlineDDL  bool   `toml:"online-ddl" json:"online-ddl"`

	// pt/gh-ost name rule, support regex
	ShadowTableRules []string `yaml:"shadow-table-rules" toml:"shadow-table-rules" json:"shadow-table-rules"`
	TrashTableRules  []string `yaml:"trash-table-rules" toml:"trash-table-rules" json:"trash-table-rules"`

	// deprecated
	OnlineDDLScheme string `toml:"online-ddl-scheme" json:"online-ddl-scheme"`

	// handle schema/table name mode, and only for schema/table name/pattern
	// if case insensitive, we would convert schema/table name/pattern to lower case
	CaseSensitive bool `toml:"case-sensitive" json:"case-sensitive"`

	Name string `toml:"name" json:"name"`
	Mode string `toml:"mode" json:"mode"`
	//  treat it as hidden configuration
	IgnoreCheckingItems []string `toml:"ignore-checking-items" json:"ignore-checking-items"`
	// it represents a MySQL/MariaDB instance or a replica group
	SourceID   string `toml:"source-id" json:"source-id"`
	ServerID   uint32 `toml:"server-id" json:"server-id"`
	Flavor     string `toml:"flavor" json:"flavor"`
	MetaSchema string `toml:"meta-schema" json:"meta-schema"`
	// deprecated
	HeartbeatUpdateInterval int `toml:"heartbeat-update-interval" json:"heartbeat-update-interval"`
	// deprecated
	HeartbeatReportInterval int `toml:"heartbeat-report-interval" json:"heartbeat-report-interval"`
	// deprecated
	EnableHeartbeat bool `toml:"enable-heartbeat" json:"enable-heartbeat"`
	// deprecated
	Timezone string `toml:"timezone" json:"timezone"`

	Meta *Meta `toml:"meta" json:"meta"`

	// RelayDir get value from dm-worker config
	RelayDir string `toml:"relay-dir" json:"relay-dir"`

	// UseRelay get value from dm-worker's relayEnabled
	UseRelay bool            `toml:"use-relay" json:"use-relay"`
	From     DBConfig        `toml:"from" json:"from"`
	To       DBConfig        `toml:"to" json:"to"`
	TiDB     TiDBExtraConfig `toml:"tidb" json:"tidb"`

	RouteRules         []*router.TableRule   `toml:"route-rules" json:"route-rules"`
	FilterRules        []*bf.BinlogEventRule `toml:"filter-rules" json:"filter-rules"`
	ColumnMappingRules []*column.Rule        `toml:"mapping-rule" json:"mapping-rule"`
	ExprFilter         []*ExpressionFilter   `yaml:"expression-filter" toml:"expression-filter" json:"expression-filter"`

	// black-white-list is deprecated, use block-allow-list instead
	BWList *filter.Rules `toml:"black-white-list" json:"black-white-list"`
	BAList *filter.Rules `toml:"block-allow-list" json:"block-allow-list"`

	MydumperConfig // Mydumper configuration
	LoaderConfig   // Loader configuration
	SyncerConfig   // Syncer configuration

	// compatible with standalone dm unit
	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogFormat string `toml:"log-format" json:"log-format"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	PprofAddr  string `toml:"pprof-addr" json:"pprof-addr"`
	StatusAddr string `toml:"status-addr" json:"status-addr"`

	ConfigFile string `toml:"-" json:"config-file"`

	CleanDumpFile bool `toml:"clean-dump-file" json:"clean-dump-file"`

	// deprecated, will auto discover SQL mode
	EnableANSIQuotes bool `toml:"ansi-quotes" json:"ansi-quotes"`

	// still needed by Syncer / Loader bin
	printVersion bool

	// which DM worker is running the subtask, this will be injected when the real worker starts running the subtask(StartSubTask).
	WorkerName string `toml:"-" json:"-"`
}

// NewSubTaskConfig creates a new SubTaskConfig.
func NewSubTaskConfig() *SubTaskConfig {
	cfg := &SubTaskConfig{}
	return cfg
}

// GetFlagSet provides the pointer of subtask's flag set.
func (c *SubTaskConfig) GetFlagSet() *flag.FlagSet {
	return c.flagSet
}

// SetFlagSet writes back the flag set.
func (c *SubTaskConfig) SetFlagSet(flagSet *flag.FlagSet) {
	c.flagSet = flagSet
}

// String returns the config's json string.
func (c *SubTaskConfig) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.L().Error("marshal subtask config to json", zap.String("task", c.Name), log.ShortError(err))
	}
	return string(cfg)
}

// Toml returns TOML format representation of config.
func (c *SubTaskConfig) Toml() (string, error) {
	var b bytes.Buffer
	enc := toml.NewEncoder(&b)
	if err := enc.Encode(c); err != nil {
		return "", terror.ErrConfigTomlTransform.Delegate(err, "encode subtask config")
	}
	return b.String(), nil
}

// DecodeFile loads and decodes config from file.
func (c *SubTaskConfig) DecodeFile(fpath string, verifyDecryptPassword bool) error {
	_, err := toml.DecodeFile(fpath, c)
	if err != nil {
		return terror.ErrConfigTomlTransform.Delegate(err, "decode subtask config from file")
	}

	return c.Adjust(verifyDecryptPassword)
}

// Decode loads config from file data.
func (c *SubTaskConfig) Decode(data string, verifyDecryptPassword bool) error {
	if _, err := toml.Decode(data, c); err != nil {
		return terror.ErrConfigTomlTransform.Delegate(err, "decode subtask config from data")
	}

	return c.Adjust(verifyDecryptPassword)
}

func adjustOnlineTableRules(ruleType string, rules []string) ([]string, error) {
	adjustedRules := make([]string, 0, len(rules))
	for _, r := range rules {
		if !strings.HasPrefix(r, "^") {
			r = "^" + r
		}

		if !strings.HasSuffix(r, "$") {
			r += "$"
		}

		p, err := regexp.Compile(r)
		if err != nil {
			return rules, terror.ErrConfigOnlineDDLInvalidRegex.Generate(ruleType, r, "fail to compile: "+err.Error())
		}
		if p.NumSubexp() != 1 {
			return rules, terror.ErrConfigOnlineDDLInvalidRegex.Generate(ruleType, r, "rule isn't contains exactly one submatch")
		}
		adjustedRules = append(adjustedRules, r)
	}
	return adjustedRules, nil
}

// Adjust adjusts and verifies configs.
func (c *SubTaskConfig) Adjust(verifyDecryptPassword bool) error {
	if c.Name == "" {
		return terror.ErrConfigTaskNameEmpty.Generate()
	}

	if c.SourceID == "" {
		return terror.ErrConfigEmptySourceID.Generate()
	}
	if len(c.SourceID) > MaxSourceIDLength {
		return terror.ErrConfigTooLongSourceID.Generate()
	}

	if c.ShardMode != "" && c.ShardMode != ShardPessimistic && c.ShardMode != ShardOptimistic {
		return terror.ErrConfigShardModeNotSupport.Generate(c.ShardMode)
	} else if c.ShardMode == "" && c.IsSharding {
		c.ShardMode = ShardPessimistic // use the pessimistic mode as default for back compatible.
	}

	if c.OnlineDDLScheme != "" && c.OnlineDDLScheme != PT && c.OnlineDDLScheme != GHOST {
		return terror.ErrConfigOnlineSchemeNotSupport.Generate(c.OnlineDDLScheme)
	} else if c.OnlineDDLScheme == PT || c.OnlineDDLScheme == GHOST {
		c.OnlineDDL = true
		log.L().Warn("'online-ddl-scheme' will be deprecated soon. Recommend that use online-ddl instead of online-ddl-scheme.")
	}
	if len(c.ShadowTableRules) == 0 {
		c.ShadowTableRules = []string{DefaultShadowTableRules}
	} else {
		shadowTableRule, err := adjustOnlineTableRules(ShadowTableRules, c.ShadowTableRules)
		if err != nil {
			return err
		}
		c.ShadowTableRules = shadowTableRule
	}

	if len(c.TrashTableRules) == 0 {
		c.TrashTableRules = []string{DefaultTrashTableRules}
	} else {
		trashTableRule, err := adjustOnlineTableRules(TrashTableRules, c.TrashTableRules)
		if err != nil {
			return err
		}
		c.TrashTableRules = trashTableRule
	}

	if c.MetaSchema == "" {
		c.MetaSchema = defaultMetaSchema
	}

	if c.Timezone != "" {
		log.L().Warn("'timezone' is deprecated, please remove this field.")
		c.Timezone = ""
	}

	dirSuffix := "." + c.Name
	if !strings.HasSuffix(c.LoaderConfig.Dir, dirSuffix) { // check to support multiple times calling
		// if not ends with the task name, we append the task name to the tail
		c.LoaderConfig.Dir += dirSuffix
	}

	if c.SyncerConfig.QueueSize == 0 {
		c.SyncerConfig.QueueSize = defaultQueueSize
	}
	if c.SyncerConfig.CheckpointFlushInterval == 0 {
		c.SyncerConfig.CheckpointFlushInterval = defaultCheckpointFlushInterval
	}

	c.From.Adjust()
	c.To.Adjust()

	if verifyDecryptPassword {
		_, err1 := c.DecryptPassword()
		if err1 != nil {
			return err1
		}
	}

	// only when block-allow-list is nil use black-white-list
	if c.BAList == nil && c.BWList != nil {
		c.BAList = c.BWList
	}

	if _, err := filter.New(c.CaseSensitive, c.BAList); err != nil {
		return terror.ErrConfigGenBAList.Delegate(err)
	}
	if _, err := router.NewTableRouter(c.CaseSensitive, c.RouteRules); err != nil {
		return terror.ErrConfigGenTableRouter.Delegate(err)
	}
	// NewMapping will fill arguments with the default values.
	if _, err := column.NewMapping(c.CaseSensitive, c.ColumnMappingRules); err != nil {
		return terror.ErrConfigGenColumnMapping.Delegate(err)
	}
	if _, err := dumpling.ParseFileSize(c.MydumperConfig.ChunkFilesize, 0); err != nil {
		return terror.ErrConfigInvalidChunkFileSize.Generate(c.MydumperConfig.ChunkFilesize)
	}

	if c.TiDB.Backend != "" && c.TiDB.Backend != lcfg.BackendLocal && c.TiDB.Backend != lcfg.BackendTiDB {
		return terror.ErrLoadBackendNotSupport.Generate(c.TiDB.Backend)
	}
	if _, err := bf.NewBinlogEvent(c.CaseSensitive, c.FilterRules); err != nil {
		return terror.ErrConfigBinlogEventFilter.Delegate(err)
	}

	// TODO: check every member
	// TODO: since we checked here, we could remove other terror like ErrSyncerUnitGenBAList
	// TODO: or we should check at task config and source config rather than this subtask config, to reduce duplication

	return nil
}

// Parse parses flag definitions from the argument list.
func (c *SubTaskConfig) Parse(arguments []string, verifyDecryptPassword bool) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
	if err != nil {
		return terror.ErrConfigParseFlagSet.Delegate(err)
	}

	if c.printVersion {
		fmt.Println(utils.GetRawInfo())
		return flag.ErrHelp
	}

	// Load config file if specified.
	if c.ConfigFile != "" {
		err = c.DecodeFile(c.ConfigFile, verifyDecryptPassword)
		if err != nil {
			return err
		}
	}

	// Parse again to replace with command line options.
	err = c.flagSet.Parse(arguments)
	if err != nil {
		return terror.ErrConfigParseFlagSet.Delegate(err)
	}

	if len(c.flagSet.Args()) != 0 {
		return terror.ErrConfigParseFlagSet.Generatef("'%s' is an invalid flag", c.flagSet.Arg(0))
	}

	return c.Adjust(verifyDecryptPassword)
}

// DecryptPassword tries to decrypt db password in config.
func (c *SubTaskConfig) DecryptPassword() (*SubTaskConfig, error) {
	clone, err := c.Clone()
	if err != nil {
		return nil, err
	}

	var (
		pswdTo   string
		pswdFrom string
	)
	if len(clone.To.Password) > 0 {
		pswdTo = utils.DecryptOrPlaintext(clone.To.Password)
	}
	if len(clone.From.Password) > 0 {
		pswdFrom = utils.DecryptOrPlaintext(clone.From.Password)
	}
	clone.From.Password = pswdFrom
	clone.To.Password = pswdTo

	return clone, nil
}

// Clone returns a replica of SubTaskConfig.
func (c *SubTaskConfig) Clone() (*SubTaskConfig, error) {
	content, err := c.Toml()
	if err != nil {
		return nil, err
	}

	clone := &SubTaskConfig{}
	_, err = toml.Decode(content, clone)
	if err != nil {
		return nil, terror.ErrConfigTomlTransform.Delegate(err, "decode subtask config from data")
	}

	return clone, nil
}
