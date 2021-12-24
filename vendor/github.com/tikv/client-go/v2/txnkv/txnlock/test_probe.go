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

package txnlock

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/retry"
)

// LockProbe exposes some lock utilities for testing purpose.
type LockProbe struct{}

// NewLockStatus returns a txn state that has been locked.
func (l LockProbe) NewLockStatus(keys [][]byte, useAsyncCommit bool, minCommitTS uint64) TxnStatus {
	return TxnStatus{
		primaryLock: &kvrpcpb.LockInfo{
			Secondaries:    keys,
			UseAsyncCommit: useAsyncCommit,
			MinCommitTs:    minCommitTS,
		},
	}
}

// GetPrimaryKeyFromTxnStatus returns the primary key of the transaction.
func (l LockProbe) GetPrimaryKeyFromTxnStatus(s TxnStatus) []byte {
	return s.primaryLock.Key
}

// LockResolverProbe wraps a LockResolver and exposes internal stats for testing purpose.
type LockResolverProbe struct {
	*LockResolver
}

// ResolveLockAsync tries to resolve a lock using the txn states.
func (l LockResolverProbe) ResolveLockAsync(bo *retry.Backoffer, lock *Lock, status TxnStatus) error {
	return l.resolveLockAsync(bo, lock, status)
}

// ResolveLock resolves single lock.
func (l LockResolverProbe) ResolveLock(bo *retry.Backoffer, lock *Lock) error {
	return l.resolveLock(bo, lock, TxnStatus{}, false, make(map[locate.RegionVerID]struct{}))
}

// ResolvePessimisticLock resolves single pessimistic lock.
func (l LockResolverProbe) ResolvePessimisticLock(bo *retry.Backoffer, lock *Lock) error {
	return l.resolvePessimisticLock(bo, lock, make(map[locate.RegionVerID]struct{}))
}

// GetTxnStatus sends the CheckTxnStatus request to the TiKV server.
func (l LockResolverProbe) GetTxnStatus(bo *retry.Backoffer, txnID uint64, primary []byte,
	callerStartTS, currentTS uint64, rollbackIfNotExist bool, forceSyncCommit bool, lockInfo *Lock) (TxnStatus, error) {
	return l.getTxnStatus(bo, txnID, primary, callerStartTS, currentTS, rollbackIfNotExist, forceSyncCommit, lockInfo)
}

// GetTxnStatusFromLock queries tikv for a txn's status.
func (l LockResolverProbe) GetTxnStatusFromLock(bo *retry.Backoffer, lock *Lock, callerStartTS uint64, forceSyncCommit bool) (TxnStatus, error) {
	return l.getTxnStatusFromLock(bo, lock, callerStartTS, forceSyncCommit)
}

// GetSecondariesFromTxnStatus returns the secondary locks from txn status.
func (l LockResolverProbe) GetSecondariesFromTxnStatus(status TxnStatus) [][]byte {
	return status.primaryLock.GetSecondaries()
}

// SetMeetLockCallback is called whenever it meets locks.
func (l LockResolverProbe) SetMeetLockCallback(f func([]*Lock)) {
	l.testingKnobs.meetLock = f
}

// CheckAllSecondaries checks the secondary locks of an async commit transaction to find out the final
// status of the transaction.
func (l LockResolverProbe) CheckAllSecondaries(bo *retry.Backoffer, lock *Lock, status *TxnStatus) error {
	_, err := l.checkAllSecondaries(bo, lock, status)
	return err
}

// IsErrorNotFound checks if an error is caused by txnNotFoundErr.
func (l LockResolverProbe) IsErrorNotFound(err error) bool {
	_, ok := errors.Cause(err).(txnNotFoundErr)
	return ok
}

// IsNonAsyncCommitLock checks if an error is nonAsyncCommitLock error.
func (l LockResolverProbe) IsNonAsyncCommitLock(err error) bool {
	_, ok := errors.Cause(err).(*nonAsyncCommitLock)
	return ok
}
