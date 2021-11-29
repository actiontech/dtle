// Copyright 2021 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/backoff.go
//

// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"context"

	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
)

// Backoffer is a utility for retrying queries.
type Backoffer = retry.Backoffer

// BackoffConfig defines the backoff configuration.
type BackoffConfig = retry.Config

// Maximum total sleep time(in ms) for kv/cop commands.
const (
	gcResolveLockMaxBackoff = 100000
)

// NewBackofferWithVars creates a Backoffer with maximum sleep time(in ms) and kv.Variables.
func NewBackofferWithVars(ctx context.Context, maxSleep int, vars *kv.Variables) *Backoffer {
	return retry.NewBackofferWithVars(ctx, maxSleep, vars)
}

// NewBackoffer creates a Backoffer with maximum sleep time(in ms).
func NewBackoffer(ctx context.Context, maxSleep int) *Backoffer {
	return retry.NewBackoffer(ctx, maxSleep)
}

// TxnStartKey is a key for transaction start_ts info in context.Context.
func TxnStartKey() interface{} {
	return retry.TxnStartKey
}

// BoRegionMiss returns the default backoff config for RegionMiss.
func BoRegionMiss() *BackoffConfig {
	return retry.BoRegionMiss
}

// BoTiFlashRPC returns the default backoff config for TiFlashRPC.
func BoTiFlashRPC() *BackoffConfig {
	return retry.BoTiFlashRPC
}

// BoTxnLock returns the default backoff config for TxnLock.
func BoTxnLock() *BackoffConfig {
	return retry.BoTxnLock
}

// BoPDRPC returns the default backoff config for PDRPC.
func BoPDRPC() *BackoffConfig {
	return retry.BoPDRPC
}

// BoTiKVRPC returns the default backoff config for TiKVRPC.
func BoTiKVRPC() *BackoffConfig {
	return retry.BoTiKVRPC
}

// NewGcResolveLockMaxBackoffer creates a Backoffer for Gc to resolve lock.
func NewGcResolveLockMaxBackoffer(ctx context.Context) *Backoffer {
	return retry.NewBackofferWithVars(ctx, gcResolveLockMaxBackoff, nil)
}

// NewNoopBackoff create a Backoffer do nothing just return error directly
var NewNoopBackoff = retry.NewNoopBackoff
