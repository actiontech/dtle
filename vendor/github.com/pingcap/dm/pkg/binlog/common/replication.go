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

package common

import (
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

var (
	// MaxBinlogSyncerReconnect is the max reconnection times for binlog syncer in go-mysql.
	MaxBinlogSyncerReconnect = 60
	// SlaveReadTimeout is slave read binlog data timeout, ref: https://dev.mysql.com/doc/refman/8.0/en/replication-options-slave.html#sysvar_slave_net_timeout
	SlaveReadTimeout = 1 * time.Minute
	// MasterHeartbeatPeriod is the master server send heartbeat period, ref: `MASTER_HEARTBEAT_PERIOD` in https://dev.mysql.com/doc/refman/8.0/en/change-master-to.html
	MasterHeartbeatPeriod = 30 * time.Second
)

// SetDefaultReplicationCfg sets some default value for BinlogSyncerConfig
// Note: retryCount should be greater than 0, set retryCount = 1 if you want to disable retry sync.
func SetDefaultReplicationCfg(cfg *replication.BinlogSyncerConfig, retryCount int) {
	// after https://github.com/go-mysql-org/go-mysql/pull/598 we could use `false` to improve performance without
	// losing precision.
	cfg.UseDecimal = false
	cfg.VerifyChecksum = true
	cfg.MaxReconnectAttempts = retryCount
	if retryCount == 1 {
		cfg.DisableRetrySync = true
	}
	cfg.ReadTimeout = SlaveReadTimeout
	cfg.HeartbeatPeriod = MasterHeartbeatPeriod
}
