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

package transaction

import (
	"bytes"
	"context"
	"sync/atomic"
	"time"

	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/internal/unionstore"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

// TxnProbe wraps a txn and exports internal states for testing purpose.
type TxnProbe struct {
	*KVTxn
}

// SetStartTS resets the txn's start ts.
func (txn TxnProbe) SetStartTS(ts uint64) {
	txn.startTS = ts
}

// GetCommitTS returns the commit ts.
func (txn TxnProbe) GetCommitTS() uint64 {
	return txn.commitTS
}

// GetUnionStore returns transaction's embedded unionstore.
func (txn TxnProbe) GetUnionStore() *unionstore.KVUnionStore {
	return txn.us
}

// IsAsyncCommit returns if the txn is committed using async commit.
func (txn TxnProbe) IsAsyncCommit() bool {
	return txn.committer.isAsyncCommit()
}

// NewCommitter creates an committer.
func (txn TxnProbe) NewCommitter(sessionID uint64) (CommitterProbe, error) {
	committer, err := newTwoPhaseCommitterWithInit(txn.KVTxn, sessionID)
	return CommitterProbe{twoPhaseCommitter: committer}, err
}

// GetCommitter returns the transaction committer.
func (txn TxnProbe) GetCommitter() CommitterProbe {
	return CommitterProbe{txn.committer}
}

// SetCommitter sets the bind committer of a transaction.
func (txn TxnProbe) SetCommitter(committer CommitterProbe) {
	txn.committer = committer.twoPhaseCommitter
}

// CollectLockedKeys returns all locked keys of a transaction.
func (txn TxnProbe) CollectLockedKeys() [][]byte {
	return txn.collectLockedKeys()
}

// BatchGetSingleRegion gets a batch of keys from a region.
func (txn TxnProbe) BatchGetSingleRegion(bo *retry.Backoffer, region locate.RegionVerID, keys [][]byte, collect func([]byte, []byte)) error {
	snapshot := txnsnapshot.SnapshotProbe{KVSnapshot: txn.GetSnapshot()}

	return snapshot.BatchGetSingleRegion(bo, region, keys, collect)
}

// NewScanner returns a scanner to iterate given key range.
func (txn TxnProbe) NewScanner(start, end []byte, batchSize int, reverse bool) (*txnsnapshot.Scanner, error) {
	snapshot := txnsnapshot.SnapshotProbe{KVSnapshot: txn.GetSnapshot()}
	return snapshot.NewScanner(start, end, batchSize, reverse)
}

// GetStartTime returns the time when txn starts.
func (txn TxnProbe) GetStartTime() time.Time {
	return txn.startTime
}

func newTwoPhaseCommitterWithInit(txn *KVTxn, sessionID uint64) (*twoPhaseCommitter, error) {
	c, err := newTwoPhaseCommitter(txn, sessionID)
	if err != nil {
		return nil, err
	}
	if err = c.initKeysAndMutations(); err != nil {
		return nil, err
	}
	return c, nil
}

// CommitterProbe wraps a 2PC committer and exports internal states for testing purpose.
type CommitterProbe struct {
	*twoPhaseCommitter
}

// InitKeysAndMutations prepares the committer for commit.
func (c CommitterProbe) InitKeysAndMutations() error {
	return c.initKeysAndMutations()
}

// SetPrimaryKey resets the committer's commit ts.
func (c CommitterProbe) SetPrimaryKey(key []byte) {
	c.primaryKey = key
}

// GetPrimaryKey returns primary key of the committer.
func (c CommitterProbe) GetPrimaryKey() []byte {
	return c.primaryKey
}

// GetMutations returns the mutation buffer to commit.
func (c CommitterProbe) GetMutations() CommitterMutations {
	return c.mutations
}

// SetMutations replace the mutation buffer.
func (c CommitterProbe) SetMutations(muts CommitterMutations) {
	c.mutations = muts.(*memBufferMutations)
}

// SetCommitTS resets the committer's commit ts.
func (c CommitterProbe) SetCommitTS(ts uint64) {
	atomic.StoreUint64(&c.commitTS, ts)
}

// GetCommitTS returns the commit ts of the committer.
func (c CommitterProbe) GetCommitTS() uint64 {
	return atomic.LoadUint64(&c.commitTS)
}

// GetMinCommitTS returns the minimal commit ts can be used.
func (c CommitterProbe) GetMinCommitTS() uint64 {
	return c.minCommitTS
}

// SetMinCommitTS sets the minimal commit ts can be used.
func (c CommitterProbe) SetMinCommitTS(ts uint64) {
	c.minCommitTS = ts
}

// SetMaxCommitTS sets the max commit ts can be used.
func (c CommitterProbe) SetMaxCommitTS(ts uint64) {
	c.maxCommitTS = ts
}

// SetSessionID sets the session id of the committer.
func (c CommitterProbe) SetSessionID(id uint64) {
	c.sessionID = id
}

// GetForUpdateTS returns the pessimistic ForUpdate ts.
func (c CommitterProbe) GetForUpdateTS() uint64 {
	return c.forUpdateTS
}

// SetForUpdateTS sets pessimistic ForUpdate ts.
func (c CommitterProbe) SetForUpdateTS(ts uint64) {
	c.forUpdateTS = ts
}

// GetStartTS returns the start ts of the transaction.
func (c CommitterProbe) GetStartTS() uint64 {
	return c.startTS
}

// GetLockTTL returns the lock ttl duration of the transaction.
func (c CommitterProbe) GetLockTTL() uint64 {
	return c.lockTTL
}

// SetLockTTL sets the lock ttl duration.
func (c CommitterProbe) SetLockTTL(ttl uint64) {
	c.lockTTL = ttl
}

// SetLockTTLByTimeAndSize sets the lock ttl duration by time and size.
func (c CommitterProbe) SetLockTTLByTimeAndSize(start time.Time, size int) {
	c.lockTTL = txnLockTTL(start, size)
}

// SetTxnSize resets the txn size of the committer and updates lock TTL.
func (c CommitterProbe) SetTxnSize(sz int) {
	c.txnSize = sz
	c.lockTTL = txnLockTTL(c.txn.startTime, sz)
}

// SetUseAsyncCommit enables async commit feature.
func (c CommitterProbe) SetUseAsyncCommit() {
	c.useAsyncCommit = 1
}

// Execute runs the commit process.
func (c CommitterProbe) Execute(ctx context.Context) error {
	return c.execute(ctx)
}

// PrewriteAllMutations performs the first phase of commit.
func (c CommitterProbe) PrewriteAllMutations(ctx context.Context) error {
	return c.PrewriteMutations(ctx, c.mutations)
}

// PrewriteMutations performs the first phase of commit for given keys.
func (c CommitterProbe) PrewriteMutations(ctx context.Context, mutations CommitterMutations) error {
	return c.prewriteMutations(retry.NewBackofferWithVars(ctx, PrewriteMaxBackoff, nil), mutations)
}

// CommitMutations performs the second phase of commit.
func (c CommitterProbe) CommitMutations(ctx context.Context) error {
	return c.commitMutations(retry.NewBackofferWithVars(ctx, int(atomic.LoadUint64(&CommitMaxBackoff)), nil), c.mutationsOfKeys([][]byte{c.primaryKey}))
}

// MutationsOfKeys returns mutations match the keys.
func (c CommitterProbe) MutationsOfKeys(keys [][]byte) CommitterMutations {
	return c.mutationsOfKeys(keys)
}

// PessimisticRollbackMutations rolls mutations back.
func (c CommitterProbe) PessimisticRollbackMutations(ctx context.Context, muts CommitterMutations) error {
	return c.pessimisticRollbackMutations(retry.NewBackofferWithVars(ctx, pessimisticRollbackMaxBackoff, nil), muts)
}

// Cleanup cleans dirty data of a committer.
func (c CommitterProbe) Cleanup(ctx context.Context) {
	c.cleanup(ctx)
	c.cleanWg.Wait()
}

// WaitCleanup waits for the committer to complete.
func (c CommitterProbe) WaitCleanup() {
	c.cleanWg.Wait()
}

// IsOnePC returns if the committer is using one PC.
func (c CommitterProbe) IsOnePC() bool {
	return c.isOnePC()
}

// BuildPrewriteRequest builds rpc request for mutation.
func (c CommitterProbe) BuildPrewriteRequest(regionID, regionConf, regionVersion uint64, mutations CommitterMutations, txnSize uint64) *tikvrpc.Request {
	var batch batchMutations
	batch.mutations = mutations
	batch.region = locate.NewRegionVerID(regionID, regionConf, regionVersion)
	for _, key := range mutations.GetKeys() {
		if bytes.Equal(key, c.primary()) {
			batch.isPrimary = true
			break
		}
	}
	return c.buildPrewriteRequest(batch, txnSize)
}

// IsAsyncCommit returns if the committer uses async commit.
func (c CommitterProbe) IsAsyncCommit() bool {
	return c.isAsyncCommit()
}

// CheckAsyncCommit returns if async commit is available.
func (c CommitterProbe) CheckAsyncCommit() bool {
	return c.checkAsyncCommit()
}

// GetOnePCCommitTS returns the commit ts of one pc.
func (c CommitterProbe) GetOnePCCommitTS() uint64 {
	return c.onePCCommitTS
}

// IsTTLUninitialized returns if the TTL manager is uninitialized.
func (c CommitterProbe) IsTTLUninitialized() bool {
	state := atomic.LoadUint32((*uint32)(&c.ttlManager.state))
	return state == uint32(stateUninitialized)
}

// IsTTLRunning returns if the TTL manager is running state.
func (c CommitterProbe) IsTTLRunning() bool {
	state := atomic.LoadUint32((*uint32)(&c.ttlManager.state))
	return state == uint32(stateRunning)
}

// CloseTTLManager closes the TTL manager.
func (c CommitterProbe) CloseTTLManager() {
	c.ttlManager.close()
}

// GetUndeterminedErr returns the encountered undetermined error (if any).
func (c CommitterProbe) GetUndeterminedErr() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.undeterminedErr
}

// SetNoFallBack disallows async commit to fall back to normal mode.
func (c CommitterProbe) SetNoFallBack() {
	c.testingKnobs.noFallBack = true
}

// SetPrimaryKeyBlocker is used to block committer after primary is sent.
func (c CommitterProbe) SetPrimaryKeyBlocker(ac, bk chan struct{}) {
	c.testingKnobs.acAfterCommitPrimary = ac
	c.testingKnobs.bkAfterCommitPrimary = bk
}

// CleanupMutations performs the clean up phase.
func (c CommitterProbe) CleanupMutations(ctx context.Context) error {
	bo := retry.NewBackofferWithVars(ctx, cleanupMaxBackoff, nil)
	return c.cleanupMutations(bo, c.mutations)
}

// SendTxnHeartBeat renews a txn's ttl.
func SendTxnHeartBeat(bo *retry.Backoffer, store kvstore, primary []byte, startTS, ttl uint64) (newTTL uint64, stopHeartBeat bool, err error) {
	return sendTxnHeartBeat(bo, store, primary, startTS, ttl)
}

// ConfigProbe exposes configurations and global variables for testing purpose.
type ConfigProbe struct{}

// GetTxnCommitBatchSize returns the batch size to commit txn.
func (c ConfigProbe) GetTxnCommitBatchSize() uint64 {
	return txnCommitBatchSize
}

// GetPessimisticLockMaxBackoff returns pessimisticLockMaxBackoff
func (c ConfigProbe) GetPessimisticLockMaxBackoff() int {
	return pessimisticLockMaxBackoff
}

// GetDefaultLockTTL returns the default lock TTL.
func (c ConfigProbe) GetDefaultLockTTL() uint64 {
	return defaultLockTTL
}

// GetTTLFactor returns the factor to calculate txn TTL.
func (c ConfigProbe) GetTTLFactor() int {
	return ttlFactor
}

// LoadPreSplitDetectThreshold returns presplit detect threshold config.
func (c ConfigProbe) LoadPreSplitDetectThreshold() uint32 {
	return atomic.LoadUint32(&preSplitDetectThreshold)
}

// StorePreSplitDetectThreshold updates presplit detect threshold config.
func (c ConfigProbe) StorePreSplitDetectThreshold(v uint32) {
	atomic.StoreUint32(&preSplitDetectThreshold, v)
}

// LoadPreSplitSizeThreshold returns presplit size threshold config.
func (c ConfigProbe) LoadPreSplitSizeThreshold() uint32 {
	return atomic.LoadUint32(&preSplitSizeThreshold)
}

// StorePreSplitSizeThreshold updates presplit size threshold config.
func (c ConfigProbe) StorePreSplitSizeThreshold(v uint32) {
	atomic.StoreUint32(&preSplitSizeThreshold, v)
}
