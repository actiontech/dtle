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

package relay

import (
	"encoding/json"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/relay/retry"
)

// Config is the configuration for Relay.
type Config struct {
	EnableGTID  bool            `toml:"enable-gtid" json:"enable-gtid"`
	AutoFixGTID bool            `toml:"auto-fix-gtid" json:"auto-fix-gtid"`
	RelayDir    string          `toml:"relay-dir" json:"relay-dir"`
	ServerID    uint32          `toml:"server-id" json:"server-id"`
	Flavor      string          `toml:"flavor" json:"flavor"`
	Charset     string          `toml:"charset" json:"charset"`
	From        config.DBConfig `toml:"data-source" json:"data-source"`

	// synchronous start point (if no meta saved before)
	// do not need to specify binlog-pos, because relay will fetch the whole file
	BinLogName string `toml:"binlog-name" json:"binlog-name"`
	BinlogGTID string `toml:"binlog-gtid" json:"binlog-gtid"`
	UUIDSuffix int    `toml:"-" json:"-"`

	// for binlog reader retry
	ReaderRetry retry.ReaderRetryConfig `toml:"reader-retry" json:"reader-retry"`
}

func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.L().Error("fail to marshal relay config to json", log.ShortError(err))
	}
	return string(cfg)
}

// FromSourceCfg gen relay config from source config.
func FromSourceCfg(sourceCfg *config.SourceConfig) *Config {
	clone := sourceCfg.DecryptPassword()
	cfg := &Config{
		EnableGTID:  clone.EnableGTID,
		AutoFixGTID: clone.AutoFixGTID,
		Flavor:      clone.Flavor,
		RelayDir:    clone.RelayDir,
		ServerID:    clone.ServerID,
		Charset:     clone.Charset,
		From:        clone.From,
		BinLogName:  clone.RelayBinLogName,
		BinlogGTID:  clone.RelayBinlogGTID,
		UUIDSuffix:  clone.UUIDSuffix,
		ReaderRetry: retry.ReaderRetryConfig{ // we use config from TaskChecker now
			BackoffRollback: clone.Checker.BackoffRollback.Duration,
			BackoffMax:      clone.Checker.BackoffMax.Duration,
			BackoffMin:      clone.Checker.BackoffMin.Duration,
			BackoffJitter:   clone.Checker.BackoffJitter,
			BackoffFactor:   clone.Checker.BackoffFactor,
		},
	}
	return cfg
}
