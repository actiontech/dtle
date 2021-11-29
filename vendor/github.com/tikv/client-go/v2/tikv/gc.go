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

package tikv

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	zap "go.uber.org/zap"
)

// GC does garbage collection (GC) of the TiKV cluster.
// GC deletes MVCC records whose timestamp is lower than the given `safepoint`. We must guarantee
//  that all transactions started before this timestamp had committed. We can keep an active
// transaction list in application to decide which is the minimal start timestamp of them.
//
// For each key, the last mutation record (unless it's a deletion) before `safepoint` is retained.
//
// GC is performed by:
// 1. resolving all locks with timestamp <= `safepoint`
// 2. updating PD's known safepoint
//
// GC is a simplified version of [GC in TiDB](https://docs.pingcap.com/tidb/stable/garbage-collection-overview).
// We skip the second step "delete ranges" which is an optimization for TiDB.
func (s *KVStore) GC(ctx context.Context, safepoint uint64) (newSafePoint uint64, err error) {
	err = s.resolveLocks(ctx, safepoint, 8)
	if err != nil {
		return
	}

	return s.pdClient.UpdateGCSafePoint(ctx, safepoint)
}

func (s *KVStore) resolveLocks(ctx context.Context, safePoint uint64, concurrency int) error {
	handler := func(ctx context.Context, r kv.KeyRange) (rangetask.TaskStat, error) {
		return s.resolveLocksForRange(ctx, safePoint, r.StartKey, r.EndKey)
	}

	runner := rangetask.NewRangeTaskRunner("resolve-locks-runner", s, concurrency, handler)
	// Run resolve lock on the whole TiKV cluster. Empty keys means the range is unbounded.
	err := runner.RunOnRange(ctx, []byte(""), []byte(""))
	if err != nil {
		return err
	}
	return nil
}

// We don't want gc to sweep out the cached info belong to other processes, like coprocessor.
const gcScanLockLimit = txnlock.ResolvedCacheSize / 2

func (s *KVStore) resolveLocksForRange(ctx context.Context, safePoint uint64, startKey []byte, endKey []byte) (rangetask.TaskStat, error) {
	// for scan lock request, we must return all locks even if they are generated
	// by the same transaction. because gc worker need to make sure all locks have been
	// cleaned.

	var stat rangetask.TaskStat
	key := startKey
	bo := NewGcResolveLockMaxBackoffer(ctx)
	for {
		select {
		case <-ctx.Done():
			return stat, errors.New("[gc worker] gc job canceled")
		default:
		}

		locks, loc, err := s.scanLocksInRegionWithStartKey(bo, key, safePoint, gcScanLockLimit)
		if err != nil {
			return stat, err
		}

		resolvedLocation, err1 := s.batchResolveLocksInARegion(bo, locks, loc)
		if err1 != nil {
			return stat, err1
		}
		// resolve locks failed since the locks are not in one region anymore, need retry.
		if resolvedLocation == nil {
			continue
		}
		if len(locks) < gcScanLockLimit {
			stat.CompletedRegions++
			key = loc.EndKey
			logutil.Logger(ctx).Info("[gc worker] one region finshed ",
				zap.Int("regionID", int(resolvedLocation.Region.GetID())),
				zap.Int("resolvedLocksNum", len(locks)))
		} else {
			logutil.Logger(ctx).Info("[gc worker] region has more than limit locks",
				zap.Int("regionID", int(resolvedLocation.Region.GetID())),
				zap.Int("resolvedLocksNum", len(locks)),
				zap.Int("scan lock limit", gcScanLockLimit))
			key = locks[len(locks)-1].Key
		}

		if len(key) == 0 || (len(endKey) != 0 && bytes.Compare(key, endKey) >= 0) {
			break
		}
		bo = NewGcResolveLockMaxBackoffer(ctx)
	}
	return stat, nil
}

func (s *KVStore) scanLocksInRegionWithStartKey(bo *retry.Backoffer, startKey []byte, maxVersion uint64, limit uint32) (locks []*txnlock.Lock, loc *locate.KeyLocation, err error) {
	for {
		loc, err := s.GetRegionCache().LocateKey(bo, startKey)
		if err != nil {
			return nil, loc, err
		}
		req := tikvrpc.NewRequest(tikvrpc.CmdScanLock, &kvrpcpb.ScanLockRequest{
			MaxVersion: maxVersion,
			Limit:      gcScanLockLimit,
			StartKey:   startKey,
			EndKey:     loc.EndKey,
		})
		resp, err := s.SendReq(bo, req, loc.Region, ReadTimeoutMedium)
		if err != nil {
			return nil, loc, err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, loc, err
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss(), errors.New(regionErr.String()))
			if err != nil {
				return nil, loc, err
			}
			continue
		}
		if resp.Resp == nil {
			return nil, loc, errors.WithStack(tikverr.ErrBodyMissing)
		}
		locksResp := resp.Resp.(*kvrpcpb.ScanLockResponse)
		if locksResp.GetError() != nil {
			return nil, loc, errors.Errorf("unexpected scanlock error: %s", locksResp)
		}
		locksInfo := locksResp.GetLocks()
		locks = make([]*txnlock.Lock, len(locksInfo))
		for i := range locksInfo {
			locks[i] = txnlock.NewLock(locksInfo[i])
		}
		return locks, loc, nil
	}
}

// batchResolveLocksInARegion resolves locks in a region.
// It returns the real location of the resolved locks if resolve locks success.
// It returns error when meet an unretryable error.
// When the locks are not in one region, resolve locks should be failed, it returns with nil resolveLocation and nil err.
// Used it in gcworker only!
func (s *KVStore) batchResolveLocksInARegion(bo *Backoffer, locks []*txnlock.Lock, expectedLoc *locate.KeyLocation) (resolvedLocation *locate.KeyLocation, err error) {
	resolvedLocation = expectedLoc
	for {
		ok, err := s.GetLockResolver().BatchResolveLocks(bo, locks, resolvedLocation.Region)
		if ok {
			return resolvedLocation, nil
		}
		if err != nil {
			return nil, err
		}
		err = bo.Backoff(retry.BoTxnLock, errors.Errorf("remain locks: %d", len(locks)))
		if err != nil {
			return nil, err
		}
		region, err1 := s.GetRegionCache().LocateKey(bo, locks[0].Key)
		if err1 != nil {
			return nil, err1
		}
		if !region.Contains(locks[len(locks)-1].Key) {
			// retry scan since the locks are not in the same region anymore.
			return nil, nil
		}
		resolvedLocation = region
	}
}

const unsafeDestroyRangeTimeout = 5 * time.Minute

// UnsafeDestroyRange Cleans up all keys in a range[startKey,endKey) and quickly free the disk space.
// The range might span over multiple regions, and the `ctx` doesn't indicate region. The request will be done directly
// on RocksDB, bypassing the Raft layer. User must promise that, after calling `UnsafeDestroyRange`,
// the range will never be accessed any more. However, `UnsafeDestroyRange` is allowed to be called
// multiple times on an single range.
func (s *KVStore) UnsafeDestroyRange(ctx context.Context, startKey []byte, endKey []byte) error {
	// Get all stores every time deleting a region. So the store list is less probably to be stale.
	stores, err := s.listStoresForUnsafeDestory(ctx)
	if err != nil {
		metrics.TiKVUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("get_stores").Inc()
		return err
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdUnsafeDestroyRange, &kvrpcpb.UnsafeDestroyRangeRequest{
		StartKey: startKey,
		EndKey:   endKey,
	})

	var wg sync.WaitGroup
	errChan := make(chan error, len(stores))

	for _, store := range stores {
		address := store.Address
		storeID := store.Id
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err1 := s.GetTiKVClient().SendRequest(ctx, address, req, unsafeDestroyRangeTimeout)
			if err1 == nil {
				if resp == nil || resp.Resp == nil {
					err1 = errors.Errorf("[unsafe destroy range] returns nil response from store %v", storeID)
				} else {
					errStr := (resp.Resp.(*kvrpcpb.UnsafeDestroyRangeResponse)).Error
					if len(errStr) > 0 {
						err1 = errors.Errorf("[unsafe destroy range] range failed on store %v: %s", storeID, errStr)
					}
				}
			}

			if err1 != nil {
				metrics.TiKVUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("send").Inc()
			}
			errChan <- err1
		}()
	}

	var errs []string
	for range stores {
		err1 := <-errChan
		if err1 != nil {
			errs = append(errs, err1.Error())
		}
	}

	wg.Wait()

	if len(errs) > 0 {
		return errors.Errorf("[unsafe destroy range] destroy range finished with errors: %v", errs)
	}

	return nil
}

func (s *KVStore) listStoresForUnsafeDestory(ctx context.Context) ([]*metapb.Store, error) {
	stores, err := s.pdClient.GetAllStores(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	upStores := make([]*metapb.Store, 0, len(stores))
	for _, store := range stores {
		if store.State == metapb.StoreState_Tombstone {
			continue
		}
		if tikvrpc.GetStoreTypeByMeta(store) == tikvrpc.TiFlash {
			continue
		}
		upStores = append(upStores, store)
	}
	return upStores, nil
}
