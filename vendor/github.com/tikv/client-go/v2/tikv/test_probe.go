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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/test_probe.go
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

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	pd "github.com/tikv/pd/client"
)

// StoreProbe wraps KVSTore and exposes internal states for testing purpose.
type StoreProbe struct {
	*KVStore
}

// NewLockResolver creates a new LockResolver instance.
func (s StoreProbe) NewLockResolver() LockResolverProbe {
	txnLockResolver := txnlock.LockResolverProbe{LockResolver: txnlock.NewLockResolver(s.KVStore)}
	return LockResolverProbe{&txnLockResolver}
}

// Begin starts a transaction.
func (s StoreProbe) Begin() (transaction.TxnProbe, error) {
	txn, err := s.KVStore.Begin()
	return transaction.TxnProbe{KVTxn: txn}, err
}

// GetSnapshot returns a snapshot.
func (s StoreProbe) GetSnapshot(ts uint64) txnsnapshot.SnapshotProbe {
	snap := s.KVStore.GetSnapshot(ts)
	return txnsnapshot.SnapshotProbe{KVSnapshot: snap}
}

// SetRegionCachePDClient replaces pd client inside region cache.
func (s StoreProbe) SetRegionCachePDClient(client pd.Client) {
	s.regionCache.SetPDClient(client)
}

// ClearTxnLatches clears store's txn latch scheduler.
func (s StoreProbe) ClearTxnLatches() {
	if s.txnLatches != nil {
		s.txnLatches.Close()
		s.txnLatches = nil
	}
}

// SendTxnHeartbeat renews a txn's ttl.
func (s StoreProbe) SendTxnHeartbeat(ctx context.Context, key []byte, startTS uint64, ttl uint64) (uint64, error) {
	bo := retry.NewBackofferWithVars(ctx, transaction.PrewriteMaxBackoff, nil)
	newTTL, _, err := transaction.SendTxnHeartBeat(bo, s.KVStore, key, startTS, ttl)
	return newTTL, err
}

// LoadSafePoint from safepoint kv.
func (s StoreProbe) LoadSafePoint() (uint64, error) {
	return loadSafePoint(s.GetSafePointKV())
}

// SaveSafePoint saves safepoint to kv.
func (s StoreProbe) SaveSafePoint(v uint64) error {
	return saveSafePoint(s.GetSafePointKV(), v)
}

// SetRegionCacheStore is used to set a store in region cache, for testing only
func (s StoreProbe) SetRegionCacheStore(id uint64, storeType tikvrpc.EndpointType, state uint64, labels []*metapb.StoreLabel) {
	s.regionCache.SetRegionCacheStore(id, storeType, state, labels)
}

// SetSafeTS is used to set safeTS for the store with `storeID`
func (s StoreProbe) SetSafeTS(storeID, safeTS uint64) {
	s.setSafeTS(storeID, safeTS)
}

// LockResolverProbe wraps a LockResolver and exposes internal stats for testing purpose.
type LockResolverProbe struct {
	*txnlock.LockResolverProbe
}

// NewLockResolverProb create a LockResolverProbe from KVStore.
func NewLockResolverProb(r *txnlock.LockResolver) *LockResolverProbe {
	resolver := txnlock.LockResolverProbe{LockResolver: r}
	return &LockResolverProbe{&resolver}
}

// ResolveLock resolves single lock.
func (l LockResolverProbe) ResolveLock(ctx context.Context, lock *txnlock.Lock) error {
	bo := retry.NewBackofferWithVars(ctx, transaction.ConfigProbe{}.GetPessimisticLockMaxBackoff(), nil)
	return l.LockResolverProbe.ResolveLock(bo, lock)
}

// ResolvePessimisticLock resolves single pessimistic lock.
func (l LockResolverProbe) ResolvePessimisticLock(ctx context.Context, lock *txnlock.Lock) error {
	bo := retry.NewBackofferWithVars(ctx, transaction.ConfigProbe{}.GetPessimisticLockMaxBackoff(), nil)
	return l.LockResolverProbe.ResolvePessimisticLock(bo, lock)
}

// ConfigProbe exposes configurations and global variables for testing purpose.
type ConfigProbe struct{}

// GetTxnCommitBatchSize returns the batch size to commit txn.
func (c ConfigProbe) GetTxnCommitBatchSize() uint64 {
	return transaction.ConfigProbe{}.GetTxnCommitBatchSize()
}

// GetBigTxnThreshold returns the txn size to be considered as big txn.
func (c ConfigProbe) GetBigTxnThreshold() int {
	// bigTxnThreshold : transaction involves keys exceed this threshold can be treated as `big transaction`.
	const bigTxnThreshold = 16
	return bigTxnThreshold
}

// GetScanBatchSize returns the batch size to scan ranges.
func (c ConfigProbe) GetScanBatchSize() int {
	return txnsnapshot.ConfigProbe{}.GetScanBatchSize()
}

// GetDefaultLockTTL returns the default lock TTL.
func (c ConfigProbe) GetDefaultLockTTL() uint64 {
	return transaction.ConfigProbe{}.GetDefaultLockTTL()
}

// GetTTLFactor returns the factor to calculate txn TTL.
func (c ConfigProbe) GetTTLFactor() int {
	return transaction.ConfigProbe{}.GetTTLFactor()
}

// GetGetMaxBackoff returns the max sleep for get command.
func (c ConfigProbe) GetGetMaxBackoff() int {
	return txnsnapshot.ConfigProbe{}.GetGetMaxBackoff()
}

// LoadPreSplitDetectThreshold returns presplit detect threshold config.
func (c ConfigProbe) LoadPreSplitDetectThreshold() uint32 {
	return transaction.ConfigProbe{}.LoadPreSplitDetectThreshold()
}

// StorePreSplitDetectThreshold updates presplit detect threshold config.
func (c ConfigProbe) StorePreSplitDetectThreshold(v uint32) {
	transaction.ConfigProbe{}.StorePreSplitDetectThreshold(v)
}

// LoadPreSplitSizeThreshold returns presplit size threshold config.
func (c ConfigProbe) LoadPreSplitSizeThreshold() uint32 {
	return transaction.ConfigProbe{}.LoadPreSplitSizeThreshold()
}

// StorePreSplitSizeThreshold updates presplit size threshold config.
func (c ConfigProbe) StorePreSplitSizeThreshold(v uint32) {
	transaction.ConfigProbe{}.StorePreSplitSizeThreshold(v)
}

// SetOracleUpdateInterval sets the interval of updating cached ts.
func (c ConfigProbe) SetOracleUpdateInterval(v int) {
	oracleUpdateInterval = v
}
