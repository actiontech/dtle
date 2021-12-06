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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/pessimistic.go
//

// Copyright 2020 PingCAP, Inc.
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
	"encoding/hex"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/prometheus/client_golang/prometheus"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

type actionPessimisticLock struct {
	*kv.LockCtx
}
type actionPessimisticRollback struct{}

var (
	_ twoPhaseCommitAction = actionPessimisticLock{}
	_ twoPhaseCommitAction = actionPessimisticRollback{}
)

func (actionPessimisticLock) String() string {
	return "pessimistic_lock"
}

func (actionPessimisticLock) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return metrics.TxnRegionsNumHistogramPessimisticLock
}

func (actionPessimisticRollback) String() string {
	return "pessimistic_rollback"
}

func (actionPessimisticRollback) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return metrics.TxnRegionsNumHistogramPessimisticRollback
}

func (action actionPessimisticLock) handleSingleBatch(c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) error {
	m := batch.mutations
	mutations := make([]*kvrpcpb.Mutation, m.Len())
	for i := 0; i < m.Len(); i++ {
		mut := &kvrpcpb.Mutation{
			Op:  kvrpcpb.Op_PessimisticLock,
			Key: m.GetKey(i),
		}
		if c.txn.us.HasPresumeKeyNotExists(m.GetKey(i)) || (c.doingAmend && m.GetOp(i) == kvrpcpb.Op_Insert) {
			mut.Assertion = kvrpcpb.Assertion_NotExist
		}
		mutations[i] = mut
	}
	elapsed := uint64(time.Since(c.txn.startTime) / time.Millisecond)
	ttl := elapsed + atomic.LoadUint64(&ManagedLockTTL)
	if _, err := util.EvalFailpoint("shortPessimisticLockTTL"); err == nil {
		ttl = 1
		keys := make([]string, 0, len(mutations))
		for _, m := range mutations {
			keys = append(keys, hex.EncodeToString(m.Key))
		}
		logutil.BgLogger().Info("[failpoint] injected lock ttl = 1 on pessimistic lock",
			zap.Uint64("txnStartTS", c.startTS), zap.Strings("keys", keys))
	}
	req := tikvrpc.NewRequest(tikvrpc.CmdPessimisticLock, &kvrpcpb.PessimisticLockRequest{
		Mutations:    mutations,
		PrimaryLock:  c.primary(),
		StartVersion: c.startTS,
		ForUpdateTs:  c.forUpdateTS,
		LockTtl:      ttl,
		IsFirstLock:  c.isFirstLock,
		WaitTimeout:  action.LockWaitTime(),
		ReturnValues: action.ReturnValues,
		MinCommitTs:  c.forUpdateTS + 1,
	}, kvrpcpb.Context{Priority: c.priority, SyncLog: c.syncLog, ResourceGroupTag: action.LockCtx.ResourceGroupTag,
		MaxExecutionDurationMs: uint64(client.MaxWriteExecutionTime.Milliseconds())})
	lockWaitStartTime := action.WaitStartTime
	for {
		// if lockWaitTime set, refine the request `WaitTimeout` field based on timeout limit
		if action.LockWaitTime() > 0 && action.LockWaitTime() != kv.LockAlwaysWait {
			timeLeft := action.LockWaitTime() - (time.Since(lockWaitStartTime)).Milliseconds()
			if timeLeft <= 0 {
				req.PessimisticLock().WaitTimeout = kv.LockNoWait
			} else {
				req.PessimisticLock().WaitTimeout = timeLeft
			}
		}
		if _, err := util.EvalFailpoint("PessimisticLockErrWriteConflict"); err == nil {
			time.Sleep(300 * time.Millisecond)
			return errors.WithStack(&tikverr.ErrWriteConflict{WriteConflict: nil})
		}
		startTime := time.Now()
		resp, err := c.store.SendReq(bo, req, batch.region, client.ReadTimeoutShort)
		if action.LockCtx.Stats != nil {
			atomic.AddInt64(&action.LockCtx.Stats.LockRPCTime, int64(time.Since(startTime)))
			atomic.AddInt64(&action.LockCtx.Stats.LockRPCCount, 1)
		}
		if err != nil {
			return err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return err
		}
		if regionErr != nil {
			// For other region error and the fake region error, backoff because
			// there's something wrong.
			// For the real EpochNotMatch error, don't backoff.
			if regionErr.GetEpochNotMatch() == nil || locate.IsFakeRegionError(regionErr) {
				err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
				if err != nil {
					return err
				}
			}
			same, err := batch.relocate(bo, c.store.GetRegionCache())
			if err != nil {
				return err
			}
			if same {
				continue
			}
			err = c.pessimisticLockMutations(bo, action.LockCtx, batch.mutations)
			return err
		}
		if resp.Resp == nil {
			return errors.WithStack(tikverr.ErrBodyMissing)
		}
		lockResp := resp.Resp.(*kvrpcpb.PessimisticLockResponse)
		keyErrs := lockResp.GetErrors()
		if len(keyErrs) == 0 {
			if batch.isPrimary {
				// After locking the primary key, we should protect the primary lock from expiring
				// now in case locking the remaining keys take a long time.
				c.run(c, action.LockCtx)
			}

			if action.ReturnValues {
				action.ValuesLock.Lock()
				for i, mutation := range mutations {
					action.Values[string(mutation.Key)] = kv.ReturnedValue{Value: lockResp.Values[i]}
				}
				action.ValuesLock.Unlock()
			}
			return nil
		}
		var locks []*txnlock.Lock
		for _, keyErr := range keyErrs {
			// Check already exists error
			if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
				e := &tikverr.ErrKeyExist{AlreadyExist: alreadyExist}
				return c.extractKeyExistsErr(e)
			}
			if deadlock := keyErr.Deadlock; deadlock != nil {
				return errors.WithStack(&tikverr.ErrDeadlock{Deadlock: deadlock})
			}

			// Extract lock from key error
			lock, err1 := txnlock.ExtractLockFromKeyErr(keyErr)
			if err1 != nil {
				return err1
			}
			locks = append(locks, lock)
		}
		// Because we already waited on tikv, no need to Backoff here.
		// tikv default will wait 3s(also the maximum wait value) when lock error occurs
		startTime = time.Now()
		msBeforeTxnExpired, _, err := c.store.GetLockResolver().ResolveLocks(bo, 0, locks)
		if err != nil {
			return err
		}
		if action.LockCtx.Stats != nil {
			atomic.AddInt64(&action.LockCtx.Stats.ResolveLockTime, int64(time.Since(startTime)))
		}

		// If msBeforeTxnExpired is not zero, it means there are still locks blocking us acquiring
		// the pessimistic lock. We should return acquire fail with nowait set or timeout error if necessary.
		if msBeforeTxnExpired > 0 {
			if action.LockWaitTime() == kv.LockNoWait {
				return errors.WithStack(tikverr.ErrLockAcquireFailAndNoWaitSet)
			} else if action.LockWaitTime() == kv.LockAlwaysWait {
				// do nothing but keep wait
			} else {
				// the lockWaitTime is set, we should return wait timeout if we are still blocked by a lock
				if time.Since(lockWaitStartTime).Milliseconds() >= action.LockWaitTime() {
					return errors.WithStack(tikverr.ErrLockWaitTimeout)
				}
			}
			if action.LockCtx.PessimisticLockWaited != nil {
				atomic.StoreInt32(action.LockCtx.PessimisticLockWaited, 1)
			}
		}

		// Handle the killed flag when waiting for the pessimistic lock.
		// When a txn runs into LockKeys() and backoff here, it has no chance to call
		// executor.Next() and check the killed flag.
		if action.Killed != nil {
			// Do not reset the killed flag here!
			// actionPessimisticLock runs on each region parallelly, we have to consider that
			// the error may be dropped.
			if atomic.LoadUint32(action.Killed) == 1 {
				return errors.WithStack(tikverr.ErrQueryInterrupted)
			}
		}
	}
}

func (actionPessimisticRollback) handleSingleBatch(c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) error {
	req := tikvrpc.NewRequest(tikvrpc.CmdPessimisticRollback, &kvrpcpb.PessimisticRollbackRequest{
		StartVersion: c.startTS,
		ForUpdateTs:  c.forUpdateTS,
		Keys:         batch.mutations.GetKeys(),
	})
	req.MaxExecutionDurationMs = uint64(client.MaxWriteExecutionTime.Milliseconds())
	resp, err := c.store.SendReq(bo, req, batch.region, client.ReadTimeoutShort)
	if err != nil {
		return err
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return err
	}
	if regionErr != nil {
		err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return err
		}
		return c.pessimisticRollbackMutations(bo, batch.mutations)
	}
	return nil
}

func (c *twoPhaseCommitter) pessimisticLockMutations(bo *retry.Backoffer, lockCtx *kv.LockCtx, mutations CommitterMutations) error {
	if c.sessionID > 0 {
		if val, err := util.EvalFailpoint("beforePessimisticLock"); err == nil {
			// Pass multiple instructions in one string, delimited by commas, to trigger multiple behaviors, like
			// `return("delay,fail")`. Then they will be executed sequentially at once.
			if v, ok := val.(string); ok {
				for _, action := range strings.Split(v, ",") {
					if action == "delay" {
						duration := time.Duration(rand.Int63n(int64(time.Second) * 5))
						logutil.Logger(bo.GetCtx()).Info("[failpoint] injected delay at pessimistic lock",
							zap.Uint64("txnStartTS", c.startTS), zap.Duration("duration", duration))
						time.Sleep(duration)
					} else if action == "fail" {
						logutil.Logger(bo.GetCtx()).Info("[failpoint] injected failure at pessimistic lock",
							zap.Uint64("txnStartTS", c.startTS))
						return errors.New("injected failure at pessimistic lock")
					}
				}
			}
		}
	}
	return c.doActionOnMutations(bo, actionPessimisticLock{lockCtx}, mutations)
}

func (c *twoPhaseCommitter) pessimisticRollbackMutations(bo *retry.Backoffer, mutations CommitterMutations) error {
	return c.doActionOnMutations(bo, actionPessimisticRollback{}, mutations)
}
