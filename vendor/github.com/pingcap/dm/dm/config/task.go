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
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/coreos/go-semver/semver"
	"github.com/dustin/go-humanize"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb/parser"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

// Online DDL Scheme.
const (
	GHOST = "gh-ost"
	PT    = "pt"
)

// shard DDL mode.
const (
	ShardPessimistic  = "pessimistic"
	ShardOptimistic   = "optimistic"
	tidbTxnMode       = "tidb_txn_mode"
	tidbTxnOptimistic = "optimistic"
)

// default config item values.
var (
	// TaskConfig.
	defaultMetaSchema      = "dm_meta"
	defaultEnableHeartbeat = false
	defaultIsSharding      = false
	defaultUpdateInterval  = 1
	defaultReportInterval  = 10
	// MydumperConfig.
	defaultMydumperPath  = "./bin/mydumper"
	defaultThreads       = 4
	defaultChunkFilesize = "64"
	defaultSkipTzUTC     = true
	// LoaderConfig.
	defaultPoolSize = 16
	defaultDir      = "./dumped_data"
	// SyncerConfig.
	defaultWorkerCount             = 16
	defaultBatch                   = 100
	defaultQueueSize               = 1024 // do not give too large default value to avoid OOM
	defaultCheckpointFlushInterval = 30   // in seconds
	// force use UTC time_zone.
	defaultTimeZone = "+00:00"

	// TargetDBConfig.
	defaultSessionCfg = []struct {
		key        string
		val        string
		minVersion *semver.Version
	}{
		{tidbTxnMode, tidbTxnOptimistic, semver.New("3.0.0")},
	}
)

// Meta represents binlog's meta pos
// NOTE: refine to put these config structs into pkgs
// NOTE: now, syncer does not support GTID mode and which is supported by relay.
type Meta struct {
	BinLogName string `toml:"binlog-name" yaml:"binlog-name"`
	BinLogPos  uint32 `toml:"binlog-pos" yaml:"binlog-pos"`
	BinLogGTID string `toml:"binlog-gtid" yaml:"binlog-gtid"`
}

// Verify does verification on configs
// NOTE: we can't decide to verify `binlog-name` or `binlog-gtid` until bound to a source (with `enable-gtid` set).
func (m *Meta) Verify() error {
	if m != nil && len(m.BinLogName) == 0 && len(m.BinLogGTID) == 0 {
		return terror.ErrConfigMetaInvalid.Generate()
	}

	return nil
}

// MySQLInstance represents a sync config of a MySQL instance.
type MySQLInstance struct {
	// it represents a MySQL/MariaDB instance or a replica group
	SourceID           string   `yaml:"source-id"`
	Meta               *Meta    `yaml:"meta"`
	FilterRules        []string `yaml:"filter-rules"`
	ColumnMappingRules []string `yaml:"column-mapping-rules"`
	RouteRules         []string `yaml:"route-rules"`
	ExpressionFilters  []string `yaml:"expression-filters"`

	// black-white-list is deprecated, use block-allow-list instead
	BWListName string `yaml:"black-white-list"`
	BAListName string `yaml:"block-allow-list"`

	MydumperConfigName string          `yaml:"mydumper-config-name"`
	Mydumper           *MydumperConfig `yaml:"mydumper"`
	// MydumperThread is alias for Threads in MydumperConfig, and its priority is higher than Threads
	MydumperThread int `yaml:"mydumper-thread"`

	LoaderConfigName string        `yaml:"loader-config-name"`
	Loader           *LoaderConfig `yaml:"loader"`
	// LoaderThread is alias for PoolSize in LoaderConfig, and its priority is higher than PoolSize
	LoaderThread int `yaml:"loader-thread"`

	SyncerConfigName string        `yaml:"syncer-config-name"`
	Syncer           *SyncerConfig `yaml:"syncer"`
	// SyncerThread is alias for WorkerCount in SyncerConfig, and its priority is higher than WorkerCount
	SyncerThread int `yaml:"syncer-thread"`
}

// VerifyAndAdjust does verification on configs, and adjust some configs.
func (m *MySQLInstance) VerifyAndAdjust() error {
	if m == nil {
		return terror.ErrConfigMySQLInstNotFound.Generate()
	}

	if m.SourceID == "" {
		return terror.ErrConfigEmptySourceID.Generate()
	}

	if err := m.Meta.Verify(); err != nil {
		return terror.Annotatef(err, "source %s", m.SourceID)
	}

	if len(m.MydumperConfigName) > 0 && m.Mydumper != nil {
		return terror.ErrConfigMydumperCfgConflict.Generate()
	}
	if len(m.LoaderConfigName) > 0 && m.Loader != nil {
		return terror.ErrConfigLoaderCfgConflict.Generate()
	}
	if len(m.SyncerConfigName) > 0 && m.Syncer != nil {
		return terror.ErrConfigSyncerCfgConflict.Generate()
	}

	if len(m.BAListName) == 0 && len(m.BWListName) != 0 {
		m.BAListName = m.BWListName
	}

	return nil
}

// MydumperConfig represents mydumper process unit's specific config.
type MydumperConfig struct {
	MydumperPath  string `yaml:"mydumper-path" toml:"mydumper-path" json:"mydumper-path"`    // mydumper binary path
	Threads       int    `yaml:"threads" toml:"threads" json:"threads"`                      // -t, --threads
	ChunkFilesize string `yaml:"chunk-filesize" toml:"chunk-filesize" json:"chunk-filesize"` // -F, --chunk-filesize
	StatementSize uint64 `yaml:"statement-size" toml:"statement-size" json:"statement-size"` // -S, --statement-size
	Rows          uint64 `yaml:"rows" toml:"rows" json:"rows"`                               // -r, --rows
	Where         string `yaml:"where" toml:"where" json:"where"`                            // --where

	SkipTzUTC bool   `yaml:"skip-tz-utc" toml:"skip-tz-utc" json:"skip-tz-utc"` // --skip-tz-utc
	ExtraArgs string `yaml:"extra-args" toml:"extra-args" json:"extra-args"`    // other extra args
	// NOTE: use LoaderConfig.Dir as --outputdir
	// TODO zxc: combine -B -T --regex with filter rules?
}

// DefaultMydumperConfig return default mydumper config for task.
func DefaultMydumperConfig() MydumperConfig {
	return MydumperConfig{
		MydumperPath:  defaultMydumperPath,
		Threads:       defaultThreads,
		ChunkFilesize: defaultChunkFilesize,
		SkipTzUTC:     defaultSkipTzUTC,
	}
}

// alias to avoid infinite recursion for UnmarshalYAML.
type rawMydumperConfig MydumperConfig

// UnmarshalYAML implements Unmarshaler.UnmarshalYAML.
func (m *MydumperConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	raw := rawMydumperConfig(DefaultMydumperConfig())
	if err := unmarshal(&raw); err != nil {
		return terror.ErrConfigYamlTransform.Delegate(err, "unmarshal mydumper config")
	}
	*m = MydumperConfig(raw) // raw used only internal, so no deep copy
	return nil
}

// LoaderConfig represents loader process unit's specific config.
type LoaderConfig struct {
	PoolSize int    `yaml:"pool-size" toml:"pool-size" json:"pool-size"`
	Dir      string `yaml:"dir" toml:"dir" json:"dir"`
	SQLMode  string `yaml:"-" toml:"-" json:"-"` // wrote by dump unit
}

// DefaultLoaderConfig return default loader config for task.
func DefaultLoaderConfig() LoaderConfig {
	return LoaderConfig{
		PoolSize: defaultPoolSize,
		Dir:      defaultDir,
	}
}

// alias to avoid infinite recursion for UnmarshalYAML.
type rawLoaderConfig LoaderConfig

// UnmarshalYAML implements Unmarshaler.UnmarshalYAML.
func (m *LoaderConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	raw := rawLoaderConfig(DefaultLoaderConfig())
	if err := unmarshal(&raw); err != nil {
		return terror.ErrConfigYamlTransform.Delegate(err, "unmarshal loader config")
	}
	*m = LoaderConfig(raw) // raw used only internal, so no deep copy
	return nil
}

// SyncerConfig represents syncer process unit's specific config.
type SyncerConfig struct {
	MetaFile    string `yaml:"meta-file" toml:"meta-file" json:"meta-file"` // meta filename, used only when load SubConfig directly
	WorkerCount int    `yaml:"worker-count" toml:"worker-count" json:"worker-count"`
	Batch       int    `yaml:"batch" toml:"batch" json:"batch"`
	QueueSize   int    `yaml:"queue-size" toml:"queue-size" json:"queue-size"`
	// checkpoint flush interval in seconds.
	CheckpointFlushInterval int `yaml:"checkpoint-flush-interval" toml:"checkpoint-flush-interval" json:"checkpoint-flush-interval"`

	// deprecated
	MaxRetry int `yaml:"max-retry" toml:"max-retry" json:"max-retry"`

	// refine following configs to top level configs?
	AutoFixGTID bool `yaml:"auto-fix-gtid" toml:"auto-fix-gtid" json:"auto-fix-gtid"`
	EnableGTID  bool `yaml:"enable-gtid" toml:"enable-gtid" json:"enable-gtid"`
	// deprecated
	DisableCausality bool `yaml:"disable-detect" toml:"disable-detect" json:"disable-detect"`
	SafeMode         bool `yaml:"safe-mode" toml:"safe-mode" json:"safe-mode"`
	// deprecated, use `ansi-quotes` in top level config instead
	EnableANSIQuotes bool `yaml:"enable-ansi-quotes" toml:"enable-ansi-quotes" json:"enable-ansi-quotes"`
}

// DefaultSyncerConfig return default syncer config for task.
func DefaultSyncerConfig() SyncerConfig {
	return SyncerConfig{
		WorkerCount:             defaultWorkerCount,
		Batch:                   defaultBatch,
		QueueSize:               defaultQueueSize,
		CheckpointFlushInterval: defaultCheckpointFlushInterval,
	}
}

// alias to avoid infinite recursion for UnmarshalYAML.
type rawSyncerConfig SyncerConfig

// UnmarshalYAML implements Unmarshaler.UnmarshalYAML.
func (m *SyncerConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	raw := rawSyncerConfig(DefaultSyncerConfig())
	if err := unmarshal(&raw); err != nil {
		return terror.ErrConfigYamlTransform.Delegate(err, "unmarshal syncer config")
	}
	*m = SyncerConfig(raw) // raw used only internal, so no deep copy
	return nil
}

// TaskConfig is the configuration for Task.
type TaskConfig struct {
	*flag.FlagSet `yaml:"-" toml:"-" json:"-"`

	Name       string `yaml:"name" toml:"name" json:"name"`
	TaskMode   string `yaml:"task-mode" toml:"task-mode" json:"task-mode"`
	IsSharding bool   `yaml:"is-sharding" toml:"is-sharding" json:"is-sharding"`
	ShardMode  string `yaml:"shard-mode" toml:"shard-mode" json:"shard-mode"` // when `shard-mode` set, we always enable sharding support.
	// treat it as hidden configuration
	IgnoreCheckingItems []string `yaml:"ignore-checking-items" toml:"ignore-checking-items" json:"ignore-checking-items"`
	// we store detail status in meta
	// don't save configuration into it
	MetaSchema string `yaml:"meta-schema" toml:"meta-schema" json:"meta-schema"`
	// deprecated
	EnableHeartbeat bool `yaml:"enable-heartbeat" toml:"enable-heartbeat" json:"enable-heartbeat"`
	// deprecated
	HeartbeatUpdateInterval int `yaml:"heartbeat-update-interval" toml:"heartbeat-update-interval" json:"heartbeat-update-interval"`
	// deprecated
	HeartbeatReportInterval int `yaml:"heartbeat-report-interval" toml:"heartbeat-report-interval" json:"heartbeat-report-interval"`
	// deprecated
	Timezone string `yaml:"timezone" toml:"timezone" json:"timezone"`

	// handle schema/table name mode, and only for schema/table name
	// if case insensitive, we would convert schema/table name to lower case
	CaseSensitive bool `yaml:"case-sensitive" toml:"case-sensitive" json:"case-sensitive"`

	TargetDB *DBConfig `yaml:"target-database" toml:"target-database" json:"target-database"`

	MySQLInstances []*MySQLInstance `yaml:"mysql-instances" toml:"mysql-instances" json:"mysql-instances"`

	OnlineDDL bool `yaml:"online-ddl" toml:"online-ddl" json:"online-ddl"`
	// pt/gh-ost name rule,support regex
	ShadowTableRules []string `yaml:"shadow-table-rules" toml:"shadow-table-rules" json:"shadow-table-rules"`
	TrashTableRules  []string `yaml:"trash-table-rules" toml:"trash-table-rules" json:"trash-table-rules"`

	// deprecated
	OnlineDDLScheme string `yaml:"online-ddl-scheme" toml:"online-ddl-scheme" json:"online-ddl-scheme"`

	Routes         map[string]*router.TableRule   `yaml:"routes" toml:"routes" json:"routes"`
	Filters        map[string]*bf.BinlogEventRule `yaml:"filters" toml:"filters" json:"filters"`
	ColumnMappings map[string]*column.Rule        `yaml:"column-mappings" toml:"column-mappings" json:"column-mappings"`
	ExprFilter     map[string]*ExpressionFilter   `yaml:"expression-filter" toml:"expression-filter" json:"expression-filter"`

	// black-white-list is deprecated, use block-allow-list instead
	BWList map[string]*filter.Rules `yaml:"black-white-list" toml:"black-white-list" json:"black-white-list"`
	BAList map[string]*filter.Rules `yaml:"block-allow-list" toml:"block-allow-list" json:"block-allow-list"`

	Mydumpers map[string]*MydumperConfig `yaml:"mydumpers" toml:"mydumpers" json:"mydumpers"`
	Loaders   map[string]*LoaderConfig   `yaml:"loaders" toml:"loaders" json:"loaders"`
	Syncers   map[string]*SyncerConfig   `yaml:"syncers" toml:"syncers" json:"syncers"`

	CleanDumpFile bool `yaml:"clean-dump-file" toml:"clean-dump-file" json:"clean-dump-file"`
	// deprecated
	EnableANSIQuotes bool `yaml:"ansi-quotes" toml:"ansi-quotes" json:"ansi-quotes"`

	// deprecated, replaced by `start-task --remove-meta`
	RemoveMeta bool `yaml:"remove-meta"`

	// extra config when target db is TiDB
	TiDB *TiDBExtraConfig `yaml:"tidb" toml:"tidb" json:"tidb"`
}

// NewTaskConfig creates a TaskConfig.
func NewTaskConfig() *TaskConfig {
	cfg := &TaskConfig{
		// explicitly set default value
		MetaSchema:              defaultMetaSchema,
		EnableHeartbeat:         defaultEnableHeartbeat,
		HeartbeatUpdateInterval: defaultUpdateInterval,
		HeartbeatReportInterval: defaultReportInterval,
		MySQLInstances:          make([]*MySQLInstance, 0, 5),
		IsSharding:              defaultIsSharding,
		Routes:                  make(map[string]*router.TableRule),
		Filters:                 make(map[string]*bf.BinlogEventRule),
		ColumnMappings:          make(map[string]*column.Rule),
		ExprFilter:              make(map[string]*ExpressionFilter),
		BWList:                  make(map[string]*filter.Rules),
		BAList:                  make(map[string]*filter.Rules),
		Mydumpers:               make(map[string]*MydumperConfig),
		Loaders:                 make(map[string]*LoaderConfig),
		Syncers:                 make(map[string]*SyncerConfig),
		CleanDumpFile:           true,
	}
	cfg.FlagSet = flag.NewFlagSet("task", flag.ContinueOnError)
	return cfg
}

// String returns the config's yaml string.
func (c *TaskConfig) String() string {
	cfg, err := yaml.Marshal(c)
	if err != nil {
		log.L().Error("marshal task config to yaml", zap.String("task", c.Name), log.ShortError(err))
	}
	return string(cfg)
}

// JSON returns the config's json string.
func (c *TaskConfig) JSON() string {
	//nolint:staticcheck
	cfg, err := json.Marshal(c)
	if err != nil {
		log.L().Error("marshal task config to json", zap.String("task", c.Name), log.ShortError(err))
	}
	return string(cfg)
}

// DecodeFile loads and decodes config from file.
func (c *TaskConfig) DecodeFile(fpath string) error {
	bs, err := os.ReadFile(fpath)
	if err != nil {
		return terror.ErrConfigReadCfgFromFile.Delegate(err, fpath)
	}

	err = yaml.UnmarshalStrict(bs, c)
	if err != nil {
		return terror.ErrConfigYamlTransform.Delegate(err)
	}

	return c.adjust()
}

// Decode loads config from file data.
func (c *TaskConfig) Decode(data string) error {
	err := yaml.UnmarshalStrict([]byte(data), c)
	if err != nil {
		return terror.ErrConfigYamlTransform.Delegate(err, "decode task config failed")
	}

	return c.adjust()
}

// RawDecode loads config from file data.
func (c *TaskConfig) RawDecode(data string) error {
	return terror.ErrConfigYamlTransform.Delegate(yaml.UnmarshalStrict([]byte(data), c), "decode task config failed")
}

// find unused items in config.
var configRefPrefixes = []string{"RouteRules", "FilterRules", "ColumnMappingRules", "Mydumper", "Loader", "Syncer", "ExprFilter"}

const (
	routeRulesIdx = iota
	filterRulesIdx
	columnMappingIdx
	mydumperIdx
	loaderIdx
	syncerIdx
	exprFilterIdx
)

// adjust adjusts and verifies config.
func (c *TaskConfig) adjust() error {
	if len(c.Name) == 0 {
		return terror.ErrConfigNeedUniqueTaskName.Generate()
	}
	if c.TaskMode != ModeFull && c.TaskMode != ModeIncrement && c.TaskMode != ModeAll {
		return terror.ErrConfigInvalidTaskMode.Generate()
	}

	if c.ShardMode != "" && c.ShardMode != ShardPessimistic && c.ShardMode != ShardOptimistic {
		return terror.ErrConfigShardModeNotSupport.Generate(c.ShardMode)
	} else if c.ShardMode == "" && c.IsSharding {
		c.ShardMode = ShardPessimistic // use the pessimistic mode as default for back compatible.
	}

	for _, item := range c.IgnoreCheckingItems {
		if err := ValidateCheckingItem(item); err != nil {
			return err
		}
	}

	if c.OnlineDDLScheme != "" && c.OnlineDDLScheme != PT && c.OnlineDDLScheme != GHOST {
		return terror.ErrConfigOnlineSchemeNotSupport.Generate(c.OnlineDDLScheme)
	} else if c.OnlineDDLScheme == PT || c.OnlineDDLScheme == GHOST {
		c.OnlineDDL = true
		log.L().Warn("'online-ddl-scheme' will be deprecated soon. Recommend that use online-ddl instead of online-ddl-scheme.")
	}

	if c.TargetDB == nil {
		return terror.ErrConfigNeedTargetDB.Generate()
	}

	if len(c.MySQLInstances) == 0 {
		return terror.ErrConfigMySQLInstsAtLeastOne.Generate()
	}

	for name, exprFilter := range c.ExprFilter {
		if exprFilter.Schema == "" {
			return terror.ErrConfigExprFilterEmptyName.Generate(name, "schema")
		}
		if exprFilter.Table == "" {
			return terror.ErrConfigExprFilterEmptyName.Generate(name, "table")
		}
		setFields := make([]string, 0, 1)
		if exprFilter.InsertValueExpr != "" {
			if err := checkValidExpr(exprFilter.InsertValueExpr); err != nil {
				return terror.ErrConfigExprFilterWrongGrammar.Generate(name, exprFilter.InsertValueExpr, err)
			}
			setFields = append(setFields, "insert: ["+exprFilter.InsertValueExpr+"]")
		}
		if exprFilter.UpdateOldValueExpr != "" || exprFilter.UpdateNewValueExpr != "" {
			if exprFilter.UpdateOldValueExpr != "" {
				if err := checkValidExpr(exprFilter.UpdateOldValueExpr); err != nil {
					return terror.ErrConfigExprFilterWrongGrammar.Generate(name, exprFilter.UpdateOldValueExpr, err)
				}
			}
			if exprFilter.UpdateNewValueExpr != "" {
				if err := checkValidExpr(exprFilter.UpdateNewValueExpr); err != nil {
					return terror.ErrConfigExprFilterWrongGrammar.Generate(name, exprFilter.UpdateNewValueExpr, err)
				}
			}
			setFields = append(setFields, "update (old value): ["+exprFilter.UpdateOldValueExpr+"] update (new value): ["+exprFilter.UpdateNewValueExpr+"]")
		}
		if exprFilter.DeleteValueExpr != "" {
			if err := checkValidExpr(exprFilter.DeleteValueExpr); err != nil {
				return terror.ErrConfigExprFilterWrongGrammar.Generate(name, exprFilter.DeleteValueExpr, err)
			}
			setFields = append(setFields, "delete: ["+exprFilter.DeleteValueExpr+"]")
		}
		if len(setFields) > 1 {
			return terror.ErrConfigExprFilterManyExpr.Generate(name, setFields)
		}
	}

	instanceIDs := make(map[string]int) // source-id -> instance-index
	globalConfigReferCount := map[string]int{}
	duplicateErrorStrings := make([]string, 0)
	for i, inst := range c.MySQLInstances {
		if err := inst.VerifyAndAdjust(); err != nil {
			return terror.Annotatef(err, "mysql-instance: %s", humanize.Ordinal(i))
		}
		if iid, ok := instanceIDs[inst.SourceID]; ok {
			return terror.ErrConfigMySQLInstSameSourceID.Generate(iid, i, inst.SourceID)
		}
		instanceIDs[inst.SourceID] = i

		switch c.TaskMode {
		case ModeFull, ModeAll:
			if inst.Meta != nil {
				log.L().Warn("metadata will not be used. for Full mode, incremental sync will never occur; for All mode, the meta dumped by MyDumper will be used", zap.Int("mysql instance", i), zap.String("task mode", c.TaskMode))
			}
		case ModeIncrement:
			if inst.Meta == nil {
				return terror.ErrConfigMetadataNotSet.Generate(i, c.TaskMode)
			}
			err := inst.Meta.Verify()
			if err != nil {
				return terror.Annotatef(err, "mysql-instance: %d", i)
			}
		}

		for _, name := range inst.RouteRules {
			if _, ok := c.Routes[name]; !ok {
				return terror.ErrConfigRouteRuleNotFound.Generate(i, name)
			}
			globalConfigReferCount[configRefPrefixes[routeRulesIdx]+name]++
		}
		for _, name := range inst.FilterRules {
			if _, ok := c.Filters[name]; !ok {
				return terror.ErrConfigFilterRuleNotFound.Generate(i, name)
			}
			globalConfigReferCount[configRefPrefixes[filterRulesIdx]+name]++
		}
		for _, name := range inst.ColumnMappingRules {
			if _, ok := c.ColumnMappings[name]; !ok {
				return terror.ErrConfigColumnMappingNotFound.Generate(i, name)
			}
			globalConfigReferCount[configRefPrefixes[columnMappingIdx]+name]++
		}

		// only when BAList is empty use BWList
		if len(c.BAList) == 0 && len(c.BWList) != 0 {
			c.BAList = c.BWList
		}
		if _, ok := c.BAList[inst.BAListName]; len(inst.BAListName) > 0 && !ok {
			return terror.ErrConfigBAListNotFound.Generate(i, inst.BAListName)
		}

		if len(inst.MydumperConfigName) > 0 {
			rule, ok := c.Mydumpers[inst.MydumperConfigName]
			if !ok {
				return terror.ErrConfigMydumperCfgNotFound.Generate(i, inst.MydumperConfigName)
			}
			globalConfigReferCount[configRefPrefixes[mydumperIdx]+inst.MydumperConfigName]++
			inst.Mydumper = new(MydumperConfig)
			*inst.Mydumper = *rule // ref mydumper config
		}
		if inst.Mydumper == nil {
			if len(c.Mydumpers) != 0 {
				log.L().Warn("mysql instance don't refer mydumper's configuration with mydumper-config-name, the default configuration will be used", zap.String("mysql instance", inst.SourceID))
			}
			defaultCfg := DefaultMydumperConfig()
			inst.Mydumper = &defaultCfg
		} else if inst.Mydumper.ChunkFilesize == "" {
			// avoid too big dump file that can't sent concurrently
			inst.Mydumper.ChunkFilesize = defaultChunkFilesize
		}
		if inst.MydumperThread != 0 {
			inst.Mydumper.Threads = inst.MydumperThread
		}

		if (c.TaskMode == ModeFull || c.TaskMode == ModeAll) && len(inst.Mydumper.MydumperPath) == 0 {
			// only verify if set, whether is valid can only be verify when we run it
			return terror.ErrConfigMydumperPathNotValid.Generate(i)
		}

		if len(inst.LoaderConfigName) > 0 {
			rule, ok := c.Loaders[inst.LoaderConfigName]
			if !ok {
				return terror.ErrConfigLoaderCfgNotFound.Generate(i, inst.LoaderConfigName)
			}
			globalConfigReferCount[configRefPrefixes[loaderIdx]+inst.LoaderConfigName]++
			inst.Loader = new(LoaderConfig)
			*inst.Loader = *rule // ref loader config
		}
		if inst.Loader == nil {
			if len(c.Loaders) != 0 {
				log.L().Warn("mysql instance don't refer loader's configuration with loader-config-name, the default configuration will be used", zap.String("mysql instance", inst.SourceID))
			}
			defaultCfg := DefaultLoaderConfig()
			inst.Loader = &defaultCfg
		}
		if inst.LoaderThread != 0 {
			inst.Loader.PoolSize = inst.LoaderThread
		}

		if len(inst.SyncerConfigName) > 0 {
			rule, ok := c.Syncers[inst.SyncerConfigName]
			if !ok {
				return terror.ErrConfigSyncerCfgNotFound.Generate(i, inst.SyncerConfigName)
			}
			globalConfigReferCount[configRefPrefixes[syncerIdx]+inst.SyncerConfigName]++
			inst.Syncer = new(SyncerConfig)
			*inst.Syncer = *rule // ref syncer config
		}
		if inst.Syncer == nil {
			if len(c.Syncers) != 0 {
				log.L().Warn("mysql instance don't refer syncer's configuration with syncer-config-name, the default configuration will be used", zap.String("mysql instance", inst.SourceID))
			}
			defaultCfg := DefaultSyncerConfig()
			inst.Syncer = &defaultCfg
		}
		if inst.SyncerThread != 0 {
			inst.Syncer.WorkerCount = inst.SyncerThread
		}

		// for backward compatible, set global config `ansi-quotes: true` if any syncer is true
		if inst.Syncer.EnableANSIQuotes {
			log.L().Warn("DM could discover proper ANSI_QUOTES, `enable-ansi-quotes` is no longer take effect")
		}
		if inst.Syncer.DisableCausality {
			log.L().Warn("`disable-causality` is no longer take effect")
		}

		for _, name := range inst.ExpressionFilters {
			if _, ok := c.ExprFilter[name]; !ok {
				return terror.ErrConfigExprFilterNotFound.Generate(i, name)
			}
			globalConfigReferCount[configRefPrefixes[exprFilterIdx]+name]++
		}

		if dupeRules := checkDuplicateString(inst.RouteRules); len(dupeRules) > 0 {
			duplicateErrorStrings = append(duplicateErrorStrings, fmt.Sprintf("mysql-instance(%d)'s route-rules: %s", i, strings.Join(dupeRules, ", ")))
		}
		if dupeRules := checkDuplicateString(inst.FilterRules); len(dupeRules) > 0 {
			duplicateErrorStrings = append(duplicateErrorStrings, fmt.Sprintf("mysql-instance(%d)'s filter-rules: %s", i, strings.Join(dupeRules, ", ")))
		}
		if dupeRules := checkDuplicateString(inst.ColumnMappingRules); len(dupeRules) > 0 {
			duplicateErrorStrings = append(duplicateErrorStrings, fmt.Sprintf("mysql-instance(%d)'s column-mapping-rules: %s", i, strings.Join(dupeRules, ", ")))
		}
		if dupeRules := checkDuplicateString(inst.ExpressionFilters); len(dupeRules) > 0 {
			duplicateErrorStrings = append(duplicateErrorStrings, fmt.Sprintf("mysql-instance(%d)'s expression-filters: %s", i, strings.Join(dupeRules, ", ")))
		}
	}
	if len(duplicateErrorStrings) > 0 {
		return terror.ErrConfigDuplicateCfgItem.Generate(strings.Join(duplicateErrorStrings, "\n"))
	}

	var unusedConfigs []string
	for route := range c.Routes {
		if globalConfigReferCount[configRefPrefixes[routeRulesIdx]+route] == 0 {
			unusedConfigs = append(unusedConfigs, route)
		}
	}
	for filter := range c.Filters {
		if globalConfigReferCount[configRefPrefixes[filterRulesIdx]+filter] == 0 {
			unusedConfigs = append(unusedConfigs, filter)
		}
	}
	for columnMapping := range c.ColumnMappings {
		if globalConfigReferCount[configRefPrefixes[columnMappingIdx]+columnMapping] == 0 {
			unusedConfigs = append(unusedConfigs, columnMapping)
		}
	}
	for mydumper := range c.Mydumpers {
		if globalConfigReferCount[configRefPrefixes[mydumperIdx]+mydumper] == 0 {
			unusedConfigs = append(unusedConfigs, mydumper)
		}
	}
	for loader := range c.Loaders {
		if globalConfigReferCount[configRefPrefixes[loaderIdx]+loader] == 0 {
			unusedConfigs = append(unusedConfigs, loader)
		}
	}
	for syncer := range c.Syncers {
		if globalConfigReferCount[configRefPrefixes[syncerIdx]+syncer] == 0 {
			unusedConfigs = append(unusedConfigs, syncer)
		}
	}
	for exprFilter := range c.ExprFilter {
		if globalConfigReferCount[configRefPrefixes[exprFilterIdx]+exprFilter] == 0 {
			unusedConfigs = append(unusedConfigs, exprFilter)
		}
	}

	if len(unusedConfigs) != 0 {
		sort.Strings(unusedConfigs)
		return terror.ErrConfigGlobalConfigsUnused.Generate(unusedConfigs)
	}
	if c.Timezone != "" {
		log.L().Warn("`timezone` is deprecated and useless anymore, please remove it.")
		c.Timezone = ""
	}
	if c.RemoveMeta {
		log.L().Warn("`remove-meta` in task config is deprecated, please use `start-task ... --remove-meta` instead")
	}

	if c.EnableHeartbeat || c.HeartbeatUpdateInterval != defaultUpdateInterval ||
		c.HeartbeatReportInterval != defaultReportInterval {
		c.EnableHeartbeat = false
		log.L().Warn("heartbeat is deprecated, needn't set it anymore.")
	}
	return nil
}

// getGenerateName generates name by rule or gets name from nameMap
// if it's a new name, increase nameIdx
// otherwise return current nameIdx.
func getGenerateName(rule interface{}, nameIdx int, namePrefix string, nameMap map[string]string) (string, int) {
	// use json as key since no DeepEqual for rules now.
	ruleByte, err := json.Marshal(rule)
	if err != nil {
		log.L().Error(fmt.Sprintf("marshal %s rule to json", namePrefix), log.ShortError(err))
		return fmt.Sprintf("%s-%02d", namePrefix, nameIdx), nameIdx + 1
	} else if val, ok := nameMap[string(ruleByte)]; ok {
		return val, nameIdx
	} else {
		ruleName := fmt.Sprintf("%s-%02d", namePrefix, nameIdx+1)
		nameMap[string(ruleByte)] = ruleName
		return ruleName, nameIdx + 1
	}
}

// checkDuplicateString checks whether the given string array has duplicate string item
// if there is duplicate, it will return **all** the duplicate strings.
func checkDuplicateString(ruleNames []string) []string {
	mp := make(map[string]bool, len(ruleNames))
	dupeArray := make([]string, 0)
	for _, name := range ruleNames {
		if added, ok := mp[name]; ok {
			if !added {
				dupeArray = append(dupeArray, name)
				mp[name] = true
			}
		} else {
			mp[name] = false
		}
	}
	return dupeArray
}

// AdjustTargetDBSessionCfg adjust session cfg of TiDB.
func AdjustTargetDBSessionCfg(dbConfig *DBConfig, version *semver.Version) {
	lowerMap := make(map[string]string, len(dbConfig.Session))
	for k, v := range dbConfig.Session {
		lowerMap[strings.ToLower(k)] = v
	}
	// all cfg in defaultSessionCfg should be lower case
	for _, cfg := range defaultSessionCfg {
		if _, ok := lowerMap[cfg.key]; !ok && !version.LessThan(*cfg.minVersion) {
			lowerMap[cfg.key] = cfg.val
		}
	}
	// force set time zone to UTC
	if tz, ok := lowerMap["time_zone"]; ok {
		log.L().Warn("session variable 'time_zone' is overwritten with UTC timezone.",
			zap.String("time_zone", tz))
	}
	lowerMap["time_zone"] = defaultTimeZone
	dbConfig.Session = lowerMap
}

// AdjustTargetDBTimeZone force adjust session `time_zone` to UTC.
func AdjustTargetDBTimeZone(config *DBConfig) {
	for k := range config.Session {
		if strings.ToLower(k) == "time_zone" {
			log.L().Warn("session variable 'time_zone' is overwritten by default UTC timezone.",
				zap.String("time_zone", config.Session[k]))
			config.Session[k] = defaultTimeZone
			return
		}
	}
	if config.Session == nil {
		config.Session = make(map[string]string, 1)
	}
	config.Session["time_zone"] = defaultTimeZone
}

var defaultParser = parser.New()

func checkValidExpr(expr string) error {
	expr = "select " + expr
	_, _, err := defaultParser.Parse(expr, "", "")
	return err
}

// YamlForDowngrade returns YAML format represents of config for downgrade.
func (c *TaskConfig) YamlForDowngrade() (string, error) {
	t := NewTaskConfigForDowngrade(c)

	// encrypt password
	cipher, err := utils.Encrypt(utils.DecryptOrPlaintext(t.TargetDB.Password))
	if err != nil {
		return "", err
	}
	t.TargetDB.Password = cipher

	// omit default values, so we can ignore them for later marshal
	t.omitDefaultVals()

	return t.Yaml()
}

// MySQLInstanceForDowngrade represents a sync config of a MySQL instance for downgrade.
type MySQLInstanceForDowngrade struct {
	SourceID           string          `yaml:"source-id"`
	Meta               *Meta           `yaml:"meta"`
	FilterRules        []string        `yaml:"filter-rules"`
	ColumnMappingRules []string        `yaml:"column-mapping-rules"`
	RouteRules         []string        `yaml:"route-rules"`
	BWListName         string          `yaml:"black-white-list"`
	BAListName         string          `yaml:"block-allow-list"`
	MydumperConfigName string          `yaml:"mydumper-config-name"`
	Mydumper           *MydumperConfig `yaml:"mydumper"`
	MydumperThread     int             `yaml:"mydumper-thread"`
	LoaderConfigName   string          `yaml:"loader-config-name"`
	Loader             *LoaderConfig   `yaml:"loader"`
	LoaderThread       int             `yaml:"loader-thread"`
	SyncerConfigName   string          `yaml:"syncer-config-name"`
	Syncer             *SyncerConfig   `yaml:"syncer"`
	SyncerThread       int             `yaml:"syncer-thread"`
	// new config item
	ExpressionFilters []string `yaml:"expression-filters,omitempty"`
}

// NewMySQLInstancesForDowngrade creates []* MySQLInstanceForDowngrade.
func NewMySQLInstancesForDowngrade(mysqlInstances []*MySQLInstance) []*MySQLInstanceForDowngrade {
	mysqlInstancesForDowngrade := make([]*MySQLInstanceForDowngrade, 0, len(mysqlInstances))
	for _, m := range mysqlInstances {
		newMySQLInstance := &MySQLInstanceForDowngrade{
			SourceID:           m.SourceID,
			Meta:               m.Meta,
			FilterRules:        m.FilterRules,
			ColumnMappingRules: m.ColumnMappingRules,
			RouteRules:         m.RouteRules,
			BWListName:         m.BWListName,
			BAListName:         m.BAListName,
			MydumperConfigName: m.MydumperConfigName,
			Mydumper:           m.Mydumper,
			MydumperThread:     m.MydumperThread,
			LoaderConfigName:   m.LoaderConfigName,
			Loader:             m.Loader,
			LoaderThread:       m.LoaderThread,
			SyncerConfigName:   m.SyncerConfigName,
			Syncer:             m.Syncer,
			SyncerThread:       m.SyncerThread,
			ExpressionFilters:  m.ExpressionFilters,
		}
		mysqlInstancesForDowngrade = append(mysqlInstancesForDowngrade, newMySQLInstance)
	}
	return mysqlInstancesForDowngrade
}

// TaskConfigForDowngrade is the base configuration for task in v2.0.
// This config is used for downgrade(config export) from a higher dmctl version.
// When we add any new config item into SourceConfig, we should update it also.
type TaskConfigForDowngrade struct {
	Name                    string                         `yaml:"name"`
	TaskMode                string                         `yaml:"task-mode"`
	IsSharding              bool                           `yaml:"is-sharding"`
	ShardMode               string                         `yaml:"shard-mode"`
	IgnoreCheckingItems     []string                       `yaml:"ignore-checking-items"`
	MetaSchema              string                         `yaml:"meta-schema"`
	EnableHeartbeat         bool                           `yaml:"enable-heartbeat"`
	HeartbeatUpdateInterval int                            `yaml:"heartbeat-update-interval"`
	HeartbeatReportInterval int                            `yaml:"heartbeat-report-interval"`
	Timezone                string                         `yaml:"timezone"`
	CaseSensitive           bool                           `yaml:"case-sensitive"`
	TargetDB                *DBConfig                      `yaml:"target-database"`
	OnlineDDLScheme         string                         `yaml:"online-ddl-scheme"`
	Routes                  map[string]*router.TableRule   `yaml:"routes"`
	Filters                 map[string]*bf.BinlogEventRule `yaml:"filters"`
	ColumnMappings          map[string]*column.Rule        `yaml:"column-mappings"`
	BWList                  map[string]*filter.Rules       `yaml:"black-white-list"`
	BAList                  map[string]*filter.Rules       `yaml:"block-allow-list"`
	Mydumpers               map[string]*MydumperConfig     `yaml:"mydumpers"`
	Loaders                 map[string]*LoaderConfig       `yaml:"loaders"`
	Syncers                 map[string]*SyncerConfig       `yaml:"syncers"`
	CleanDumpFile           bool                           `yaml:"clean-dump-file"`
	EnableANSIQuotes        bool                           `yaml:"ansi-quotes"`
	RemoveMeta              bool                           `yaml:"remove-meta"`
	// new config item
	MySQLInstances   []*MySQLInstanceForDowngrade `yaml:"mysql-instances"`
	ExprFilter       map[string]*ExpressionFilter `yaml:"expression-filter,omitempty"`
	OnlineDDL        bool                         `yaml:"online-ddl,omitempty"`
	ShadowTableRules []string                     `yaml:"shadow-table-rules,omitempty"`
	TrashTableRules  []string                     `yaml:"trash-table-rules,omitempty"`
}

// NewTaskConfigForDowngrade create new TaskConfigForDowngrade.
func NewTaskConfigForDowngrade(taskConfig *TaskConfig) *TaskConfigForDowngrade {
	return &TaskConfigForDowngrade{
		Name:                    taskConfig.Name,
		TaskMode:                taskConfig.TaskMode,
		IsSharding:              taskConfig.IsSharding,
		ShardMode:               taskConfig.ShardMode,
		IgnoreCheckingItems:     taskConfig.IgnoreCheckingItems,
		MetaSchema:              taskConfig.MetaSchema,
		EnableHeartbeat:         taskConfig.EnableHeartbeat,
		HeartbeatUpdateInterval: taskConfig.HeartbeatUpdateInterval,
		HeartbeatReportInterval: taskConfig.HeartbeatReportInterval,
		Timezone:                taskConfig.Timezone,
		CaseSensitive:           taskConfig.CaseSensitive,
		TargetDB:                taskConfig.TargetDB,
		OnlineDDLScheme:         taskConfig.OnlineDDLScheme,
		Routes:                  taskConfig.Routes,
		Filters:                 taskConfig.Filters,
		ColumnMappings:          taskConfig.ColumnMappings,
		BWList:                  taskConfig.BWList,
		BAList:                  taskConfig.BAList,
		Mydumpers:               taskConfig.Mydumpers,
		Loaders:                 taskConfig.Loaders,
		Syncers:                 taskConfig.Syncers,
		CleanDumpFile:           taskConfig.CleanDumpFile,
		EnableANSIQuotes:        taskConfig.EnableANSIQuotes,
		RemoveMeta:              taskConfig.RemoveMeta,
		MySQLInstances:          NewMySQLInstancesForDowngrade(taskConfig.MySQLInstances),
		ExprFilter:              taskConfig.ExprFilter,
		OnlineDDL:               taskConfig.OnlineDDL,
		ShadowTableRules:        taskConfig.ShadowTableRules,
		TrashTableRules:         taskConfig.TrashTableRules,
	}
}

// omitDefaultVals change default value to empty value for new config item.
// If any default value for new config item is not empty(0 or false or nil),
// we should change it to empty.
func (c *TaskConfigForDowngrade) omitDefaultVals() {
	if len(c.TargetDB.Session) > 0 {
		if timeZone, ok := c.TargetDB.Session["time_zone"]; ok && timeZone == defaultTimeZone {
			delete(c.TargetDB.Session, "time_zone")
		}
	}
	if len(c.ShadowTableRules) == 1 && c.ShadowTableRules[0] == DefaultShadowTableRules {
		c.ShadowTableRules = nil
	}
	if len(c.TrashTableRules) == 1 && c.TrashTableRules[0] == DefaultTrashTableRules {
		c.TrashTableRules = nil
	}
}

// Yaml returns YAML format representation of config.
func (c *TaskConfigForDowngrade) Yaml() (string, error) {
	b, err := yaml.Marshal(c)
	return string(b), err
}
