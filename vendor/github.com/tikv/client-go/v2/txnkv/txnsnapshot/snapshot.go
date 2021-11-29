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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/snapshot.go
//

// Copyright 2015 PingCAP, Inc.
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

package txnsnapshot

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/internal/unionstore"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/txnkv/txnutil"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

const (
	defaultScanBatchSize = 256
	batchGetSize         = 5120
	maxTimestamp         = math.MaxUint64
)

// IsoLevel is the transaction's isolation level.
type IsoLevel kvrpcpb.IsolationLevel

const (
	// SI stands for 'snapshot isolation'.
	SI IsoLevel = IsoLevel(kvrpcpb.IsolationLevel_SI)
	// RC stands for 'read committed'.
	RC IsoLevel = IsoLevel(kvrpcpb.IsolationLevel_RC)
)

// ToPB converts isolation level to wire type.
func (l IsoLevel) ToPB() kvrpcpb.IsolationLevel {
	return kvrpcpb.IsolationLevel(l)
}

type kvstore interface {
	CheckVisibility(startTime uint64) error
	// GetRegionCache gets the RegionCache.
	GetRegionCache() *locate.RegionCache
	GetLockResolver() *txnlock.LockResolver
	GetTiKVClient() (client client.Client)

	// SendReq sends a request to TiKV.
	SendReq(bo *retry.Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration) (*tikvrpc.Response, error)
	// GetOracle gets a timestamp oracle client.
	GetOracle() oracle.Oracle
}

// KVSnapshot implements the tidbkv.Snapshot interface.
type KVSnapshot struct {
	store           kvstore
	version         uint64
	isolationLevel  IsoLevel
	priority        txnutil.Priority
	notFillCache    bool
	keyOnly         bool
	vars            *kv.Variables
	replicaReadSeed uint32
	resolvedLocks   util.TSSet
	scanBatchSize   int

	// Cache the result of BatchGet.
	// The invariance is that calling BatchGet multiple times using the same start ts,
	// the result should not change.
	// NOTE: This representation here is different from the BatchGet API.
	// cached use len(value)=0 to represent a key-value entry doesn't exist (a reliable truth from TiKV).
	// In the BatchGet API, it use no key-value entry to represent non-exist.
	// It's OK as long as there are no zero-byte values in the protocol.
	mu struct {
		sync.RWMutex
		hitCnt           int64
		cached           map[string][]byte
		cachedSize       int
		stats            *SnapshotRuntimeStats
		replicaRead      kv.ReplicaReadType
		taskID           uint64
		isStaleness      bool
		readReplicaScope string
		// MatchStoreLabels indicates the labels the store should be matched
		matchStoreLabels []*metapb.StoreLabel
	}
	sampleStep uint32
	// resourceGroupTag is use to set the kv request resource group tag.
	resourceGroupTag []byte
}

// NewTiKVSnapshot creates a snapshot of an TiKV store.
func NewTiKVSnapshot(store kvstore, ts uint64, replicaReadSeed uint32) *KVSnapshot {
	// Sanity check for snapshot version.
	if ts >= math.MaxInt64 && ts != math.MaxUint64 {
		err := errors.Errorf("try to get snapshot with a large ts %d", ts)
		panic(err)
	}
	return &KVSnapshot{
		store:           store,
		version:         ts,
		scanBatchSize:   defaultScanBatchSize,
		priority:        txnutil.PriorityNormal,
		vars:            kv.DefaultVars,
		replicaReadSeed: replicaReadSeed,
	}
}

const batchGetMaxBackoff = 600000 // 10 minutes

// SetSnapshotTS resets the timestamp for reads.
func (s *KVSnapshot) SetSnapshotTS(ts uint64) {
	// Sanity check for snapshot version.
	if ts >= math.MaxInt64 && ts != math.MaxUint64 {
		err := errors.Errorf("try to get snapshot with a large ts %d", ts)
		panic(err)
	}
	// Invalidate cache if the snapshotTS change!
	s.version = ts
	s.mu.Lock()
	s.mu.cached = nil
	s.mu.Unlock()
	// And also remove the minCommitTS pushed information.
	s.resolvedLocks = util.TSSet{}
}

// BatchGet gets all the keys' value from kv-server and returns a map contains key/value pairs.
// The map will not contain nonexistent keys.
// NOTE: Don't modify keys. Some codes rely on the order of keys.
func (s *KVSnapshot) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	// Check the cached value first.
	m := make(map[string][]byte)
	s.mu.RLock()
	if s.mu.cached != nil {
		tmp := make([][]byte, 0, len(keys))
		for _, key := range keys {
			if val, ok := s.mu.cached[string(key)]; ok {
				atomic.AddInt64(&s.mu.hitCnt, 1)
				if len(val) > 0 {
					m[string(key)] = val
				}
			} else {
				tmp = append(tmp, key)
			}
		}
		keys = tmp
	}
	s.mu.RUnlock()

	if len(keys) == 0 {
		return m, nil
	}

	ctx = context.WithValue(ctx, retry.TxnStartKey, s.version)
	bo := retry.NewBackofferWithVars(ctx, batchGetMaxBackoff, s.vars)

	// Create a map to collect key-values from region servers.
	var mu sync.Mutex
	err := s.batchGetKeysByRegions(bo, keys, func(k, v []byte) {
		if len(v) == 0 {
			return
		}

		mu.Lock()
		m[string(k)] = v
		mu.Unlock()
	})
	s.recordBackoffInfo(bo)
	if err != nil {
		return nil, err
	}

	err = s.store.CheckVisibility(s.version)
	if err != nil {
		return nil, err
	}

	// Update the cache.
	s.mu.Lock()
	if s.mu.cached == nil {
		s.mu.cached = make(map[string][]byte, len(m))
	}
	for _, key := range keys {
		val := m[string(key)]
		s.mu.cachedSize += len(key) + len(val)
		s.mu.cached[string(key)] = val
	}

	const cachedSizeLimit = 10 << 30
	if s.mu.cachedSize >= cachedSizeLimit {
		for k, v := range s.mu.cached {
			if _, needed := m[k]; needed {
				continue
			}
			delete(s.mu.cached, k)
			s.mu.cachedSize -= len(k) + len(v)
			if s.mu.cachedSize < cachedSizeLimit {
				break
			}
		}
	}
	s.mu.Unlock()

	return m, nil
}

type batchKeys struct {
	region locate.RegionVerID
	keys   [][]byte
}

func (b *batchKeys) relocate(bo *retry.Backoffer, c *locate.RegionCache) (bool, error) {
	loc, err := c.LocateKey(bo, b.keys[0])
	if err != nil {
		return false, err
	}
	// keys is not in order, so we have to iterate all keys.
	for i := 1; i < len(b.keys); i++ {
		if !loc.Contains(b.keys[i]) {
			return false, nil
		}
	}
	b.region = loc.Region
	return true, nil
}

// appendBatchKeysBySize appends keys to b. It may split the keys to make
// sure each batch's size does not exceed the limit.
func appendBatchKeysBySize(b []batchKeys, region locate.RegionVerID, keys [][]byte, sizeFn func([]byte) int, limit int) []batchKeys {
	var start, end int
	for start = 0; start < len(keys); start = end {
		var size int
		for end = start; end < len(keys) && size < limit; end++ {
			size += sizeFn(keys[end])
		}
		b = append(b, batchKeys{
			region: region,
			keys:   keys[start:end],
		})
	}
	return b
}

func (s *KVSnapshot) batchGetKeysByRegions(bo *retry.Backoffer, keys [][]byte, collectF func(k, v []byte)) error {
	defer func(start time.Time) {
		metrics.TxnCmdHistogramWithBatchGet.Observe(time.Since(start).Seconds())
	}(time.Now())
	groups, _, err := s.store.GetRegionCache().GroupKeysByRegion(bo, keys, nil)
	if err != nil {
		return err
	}

	metrics.TxnRegionsNumHistogramWithSnapshot.Observe(float64(len(groups)))

	var batches []batchKeys
	for id, g := range groups {
		batches = appendBatchKeysBySize(batches, id, g, func([]byte) int { return 1 }, batchGetSize)
	}

	if len(batches) == 0 {
		return nil
	}
	if len(batches) == 1 {
		return s.batchGetSingleRegion(bo, batches[0], collectF)
	}
	ch := make(chan error)
	for _, batch1 := range batches {
		batch := batch1
		go func() {
			backoffer, cancel := bo.Fork()
			defer cancel()
			ch <- s.batchGetSingleRegion(backoffer, batch, collectF)
		}()
	}
	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			logutil.BgLogger().Debug("snapshot batchGet failed",
				zap.Error(e),
				zap.Uint64("txnStartTS", s.version))
			err = errors.WithStack(e)
		}
	}
	return err
}

func (s *KVSnapshot) batchGetSingleRegion(bo *retry.Backoffer, batch batchKeys, collectF func(k, v []byte)) error {
	cli := NewClientHelper(s.store, &s.resolvedLocks, false)
	s.mu.RLock()
	if s.mu.stats != nil {
		cli.Stats = make(map[tikvrpc.CmdType]*locate.RPCRuntimeStats)
		defer func() {
			s.mergeRegionRequestStats(cli.Stats)
		}()
	}
	s.mu.RUnlock()

	pending := batch.keys
	for {
		s.mu.RLock()
		req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdBatchGet, &kvrpcpb.BatchGetRequest{
			Keys:    pending,
			Version: s.version,
		}, s.mu.replicaRead, &s.replicaReadSeed, kvrpcpb.Context{
			Priority:         s.priority.ToPB(),
			NotFillCache:     s.notFillCache,
			TaskId:           s.mu.taskID,
			ResourceGroupTag: s.resourceGroupTag,
		})
		scope := s.mu.readReplicaScope
		isStaleness := s.mu.isStaleness
		matchStoreLabels := s.mu.matchStoreLabels
		s.mu.RUnlock()
		req.TxnScope = scope
		req.ReadReplicaScope = scope
		if isStaleness {
			req.EnableStaleRead()
		}
		ops := make([]locate.StoreSelectorOption, 0, 2)
		if len(matchStoreLabels) > 0 {
			ops = append(ops, locate.WithMatchLabels(matchStoreLabels))
		}
		resp, _, _, err := cli.SendReqCtx(bo, req, batch.region, client.ReadTimeoutMedium, tikvrpc.TiKV, "", ops...)
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
			same, err := batch.relocate(bo, cli.regionCache)
			if err != nil {
				return err
			}
			if same {
				continue
			}
			return s.batchGetKeysByRegions(bo, pending, collectF)
		}
		if resp.Resp == nil {
			return errors.WithStack(tikverr.ErrBodyMissing)
		}
		batchGetResp := resp.Resp.(*kvrpcpb.BatchGetResponse)
		var (
			lockedKeys [][]byte
			locks      []*txnlock.Lock
		)
		if keyErr := batchGetResp.GetError(); keyErr != nil {
			// If a response-level error happens, skip reading pairs.
			lock, err := txnlock.ExtractLockFromKeyErr(keyErr)
			if err != nil {
				return err
			}
			lockedKeys = append(lockedKeys, lock.Key)
			locks = append(locks, lock)
		} else {
			for _, pair := range batchGetResp.Pairs {
				keyErr := pair.GetError()
				if keyErr == nil {
					collectF(pair.GetKey(), pair.GetValue())
					continue
				}
				lock, err := txnlock.ExtractLockFromKeyErr(keyErr)
				if err != nil {
					return err
				}
				lockedKeys = append(lockedKeys, lock.Key)
				locks = append(locks, lock)
			}
		}
		if batchGetResp.ExecDetailsV2 != nil {
			readKeys := len(batchGetResp.Pairs)
			readTime := float64(batchGetResp.ExecDetailsV2.GetTimeDetail().GetKvReadWallTimeMs() / 1000)
			readSize := float64(batchGetResp.ExecDetailsV2.GetScanDetailV2().GetProcessedVersionsSize())
			metrics.ObserveReadSLI(uint64(readKeys), readTime, readSize)
			s.mergeExecDetail(batchGetResp.ExecDetailsV2)
		}
		if len(lockedKeys) > 0 {
			msBeforeExpired, err := cli.ResolveLocks(bo, s.version, locks)
			if err != nil {
				return err
			}
			if msBeforeExpired > 0 {
				err = bo.BackoffWithMaxSleepTxnLockFast(int(msBeforeExpired), errors.Errorf("batchGet lockedKeys: %d", len(lockedKeys)))
				if err != nil {
					return err
				}
			}
			// Only reduce pending keys when there is no response-level error. Otherwise,
			// lockedKeys may be incomplete.
			if batchGetResp.GetError() == nil {
				pending = lockedKeys
			}
			continue
		}
		return nil
	}
}

const getMaxBackoff = 600000 // 10 minutes

// Get gets the value for key k from snapshot.
func (s *KVSnapshot) Get(ctx context.Context, k []byte) ([]byte, error) {
	defer func(start time.Time) {
		metrics.TxnCmdHistogramWithGet.Observe(time.Since(start).Seconds())
	}(time.Now())

	ctx = context.WithValue(ctx, retry.TxnStartKey, s.version)
	bo := retry.NewBackofferWithVars(ctx, getMaxBackoff, s.vars)
	val, err := s.get(ctx, bo, k)
	s.recordBackoffInfo(bo)
	if err != nil {
		return nil, err
	}
	err = s.store.CheckVisibility(s.version)
	if err != nil {
		return nil, err
	}

	if len(val) == 0 {
		return nil, tikverr.ErrNotExist
	}
	return val, nil
}

func (s *KVSnapshot) get(ctx context.Context, bo *retry.Backoffer, k []byte) ([]byte, error) {
	// Check the cached values first.
	s.mu.RLock()
	if s.mu.cached != nil {
		if value, ok := s.mu.cached[string(k)]; ok {
			atomic.AddInt64(&s.mu.hitCnt, 1)
			s.mu.RUnlock()
			return value, nil
		}
	}
	s.mu.RUnlock()
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tikvSnapshot.get", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		opentracing.ContextWithSpan(ctx, span1)
	}
	failpoint.Inject("snapshot-get-cache-fail", func(_ failpoint.Value) {
		if bo.GetCtx().Value("TestSnapshotCache") != nil {
			panic("cache miss")
		}
	})

	cli := NewClientHelper(s.store, &s.resolvedLocks, true)

	s.mu.RLock()
	if s.mu.stats != nil {
		cli.Stats = make(map[tikvrpc.CmdType]*locate.RPCRuntimeStats)
		defer func() {
			s.mergeRegionRequestStats(cli.Stats)
		}()
	}
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet,
		&kvrpcpb.GetRequest{
			Key:     k,
			Version: s.version,
		}, s.mu.replicaRead, &s.replicaReadSeed, kvrpcpb.Context{
			Priority:         s.priority.ToPB(),
			NotFillCache:     s.notFillCache,
			TaskId:           s.mu.taskID,
			ResourceGroupTag: s.resourceGroupTag,
		})
	isStaleness := s.mu.isStaleness
	matchStoreLabels := s.mu.matchStoreLabels
	scope := s.mu.readReplicaScope
	s.mu.RUnlock()
	req.TxnScope = scope
	req.ReadReplicaScope = scope
	var ops []locate.StoreSelectorOption
	if isStaleness {
		req.EnableStaleRead()
	}
	if len(matchStoreLabels) > 0 {
		ops = append(ops, locate.WithMatchLabels(matchStoreLabels))
	}

	var firstLock *txnlock.Lock
	for {
		util.EvalFailpoint("beforeSendPointGet")
		loc, err := s.store.GetRegionCache().LocateKey(bo, k)
		if err != nil {
			return nil, err
		}
		resp, _, _, err := cli.SendReqCtx(bo, req, loc.Region, client.ReadTimeoutShort, tikvrpc.TiKV, "", ops...)
		if err != nil {
			return nil, err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, err
		}
		if regionErr != nil {
			// For other region error and the fake region error, backoff because
			// there's something wrong.
			// For the real EpochNotMatch error, don't backoff.
			if regionErr.GetEpochNotMatch() == nil || locate.IsFakeRegionError(regionErr) {
				err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
				if err != nil {
					return nil, err
				}
			}
			continue
		}
		if resp.Resp == nil {
			return nil, errors.WithStack(tikverr.ErrBodyMissing)
		}
		cmdGetResp := resp.Resp.(*kvrpcpb.GetResponse)
		if cmdGetResp.ExecDetailsV2 != nil {
			readKeys := len(cmdGetResp.Value)
			readTime := float64(cmdGetResp.ExecDetailsV2.GetTimeDetail().GetKvReadWallTimeMs() / 1000)
			readSize := float64(cmdGetResp.ExecDetailsV2.GetScanDetailV2().GetProcessedVersionsSize())
			metrics.ObserveReadSLI(uint64(readKeys), readTime, readSize)
			s.mergeExecDetail(cmdGetResp.ExecDetailsV2)
		}
		val := cmdGetResp.GetValue()
		if keyErr := cmdGetResp.GetError(); keyErr != nil {
			lock, err := txnlock.ExtractLockFromKeyErr(keyErr)
			if err != nil {
				return nil, err
			}
			if firstLock == nil {
				firstLock = lock
			} else if s.version == maxTimestamp && firstLock.TxnID != lock.TxnID {
				// If it is an autocommit point get, it needs to be blocked only
				// by the first lock it meets. During retries, if the encountered
				// lock is different from the first one, we can omit it.
				cli.resolvedLocks.Put(lock.TxnID)
				continue
			}

			msBeforeExpired, err := cli.ResolveLocks(bo, s.version, []*txnlock.Lock{lock})
			if err != nil {
				return nil, err
			}
			if msBeforeExpired > 0 {
				err = bo.BackoffWithMaxSleepTxnLockFast(int(msBeforeExpired), errors.New(keyErr.String()))
				if err != nil {
					return nil, err
				}
			}
			continue
		}
		return val, nil
	}
}

func (s *KVSnapshot) mergeExecDetail(detail *kvrpcpb.ExecDetailsV2) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if detail == nil || s.mu.stats == nil {
		return
	}
	if s.mu.stats.scanDetail == nil {
		s.mu.stats.scanDetail = &util.ScanDetail{}
	}
	if s.mu.stats.timeDetail == nil {
		s.mu.stats.timeDetail = &util.TimeDetail{}
	}
	s.mu.stats.scanDetail.MergeFromScanDetailV2(detail.ScanDetailV2)
	s.mu.stats.timeDetail.MergeFromTimeDetail(detail.TimeDetail)
}

// Iter return a list of key-value pair after `k`.
func (s *KVSnapshot) Iter(k []byte, upperBound []byte) (unionstore.Iterator, error) {
	scanner, err := newScanner(s, k, upperBound, s.scanBatchSize, false)
	return scanner, err
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (s *KVSnapshot) IterReverse(k []byte) (unionstore.Iterator, error) {
	scanner, err := newScanner(s, nil, k, s.scanBatchSize, true)
	return scanner, err
}

// SetNotFillCache indicates whether tikv should skip filling cache when
// loading data.
func (s *KVSnapshot) SetNotFillCache(b bool) {
	s.notFillCache = b
}

// SetKeyOnly indicates if tikv can return only keys.
func (s *KVSnapshot) SetKeyOnly(b bool) {
	s.keyOnly = b
}

// SetScanBatchSize sets the scan batchSize used to scan data from tikv.
func (s *KVSnapshot) SetScanBatchSize(batchSize int) {
	s.scanBatchSize = batchSize
}

// SetReplicaRead sets up the replica read type.
func (s *KVSnapshot) SetReplicaRead(readType kv.ReplicaReadType) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.replicaRead = readType
}

// SetIsolationLevel sets the isolation level used to scan data from tikv.
func (s *KVSnapshot) SetIsolationLevel(level IsoLevel) {
	s.isolationLevel = level
}

// SetSampleStep skips 'step - 1' number of keys after each returned key.
func (s *KVSnapshot) SetSampleStep(step uint32) {
	s.sampleStep = step
}

// SetPriority sets the priority for tikv to execute commands.
func (s *KVSnapshot) SetPriority(pri txnutil.Priority) {
	s.priority = pri
}

// SetTaskID marks current task's unique ID to allow TiKV to schedule
// tasks more fairly.
func (s *KVSnapshot) SetTaskID(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.taskID = id
}

// SetRuntimeStats sets the stats to collect runtime statistics.
// Set it to nil to clear stored stats.
func (s *KVSnapshot) SetRuntimeStats(stats *SnapshotRuntimeStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.stats = stats
}

// SetTxnScope is same as SetReadReplicaScope, keep it in order to keep compatible for now.
func (s *KVSnapshot) SetTxnScope(scope string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.readReplicaScope = scope
}

// SetReadReplicaScope set read replica scope
func (s *KVSnapshot) SetReadReplicaScope(scope string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.readReplicaScope = scope
}

// SetIsStatenessReadOnly indicates whether the transaction is staleness read only transaction
func (s *KVSnapshot) SetIsStatenessReadOnly(b bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.isStaleness = b
}

// SetMatchStoreLabels sets up labels to filter target stores.
func (s *KVSnapshot) SetMatchStoreLabels(labels []*metapb.StoreLabel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.matchStoreLabels = labels
}

// SetResourceGroupTag sets resource group of the kv request.
func (s *KVSnapshot) SetResourceGroupTag(tag []byte) {
	s.resourceGroupTag = tag
}

// SnapCacheHitCount gets the snapshot cache hit count. Only for test.
func (s *KVSnapshot) SnapCacheHitCount() int {
	return int(atomic.LoadInt64(&s.mu.hitCnt))
}

// SnapCacheSize gets the snapshot cache size. Only for test.
func (s *KVSnapshot) SnapCacheSize() int {
	s.mu.RLock()
	defer s.mu.RLock()
	return len(s.mu.cached)
}

// SetVars sets variables to the transaction.
func (s *KVSnapshot) SetVars(vars *kv.Variables) {
	s.vars = vars
}

func (s *KVSnapshot) recordBackoffInfo(bo *retry.Backoffer) {
	s.mu.RLock()
	if s.mu.stats == nil || bo.GetTotalSleep() == 0 {
		s.mu.RUnlock()
		return
	}
	s.mu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.stats == nil {
		return
	}
	if s.mu.stats.backoffSleepMS == nil {
		s.mu.stats.backoffSleepMS = bo.GetBackoffSleepMS()
		s.mu.stats.backoffTimes = bo.GetBackoffTimes()
		return
	}
	for k, v := range bo.GetBackoffSleepMS() {
		s.mu.stats.backoffSleepMS[k] += v
	}
	for k, v := range bo.GetBackoffTimes() {
		s.mu.stats.backoffTimes[k] += v
	}
}

func (s *KVSnapshot) mergeRegionRequestStats(stats map[tikvrpc.CmdType]*locate.RPCRuntimeStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.stats == nil {
		return
	}
	if s.mu.stats.rpcStats.Stats == nil {
		s.mu.stats.rpcStats.Stats = stats
		return
	}
	for k, v := range stats {
		stat, ok := s.mu.stats.rpcStats.Stats[k]
		if !ok {
			s.mu.stats.rpcStats.Stats[k] = v
			continue
		}
		stat.Count += v.Count
		stat.Consume += v.Consume
	}
}

// SnapshotRuntimeStats records the runtime stats of snapshot.
type SnapshotRuntimeStats struct {
	rpcStats       locate.RegionRequestRuntimeStats
	backoffSleepMS map[string]int
	backoffTimes   map[string]int
	scanDetail     *util.ScanDetail
	timeDetail     *util.TimeDetail
}

// Clone implements the RuntimeStats interface.
func (rs *SnapshotRuntimeStats) Clone() *SnapshotRuntimeStats {
	newRs := SnapshotRuntimeStats{rpcStats: locate.NewRegionRequestRuntimeStats()}
	if rs.rpcStats.Stats != nil {
		for k, v := range rs.rpcStats.Stats {
			newRs.rpcStats.Stats[k] = v
		}
	}
	if len(rs.backoffSleepMS) > 0 {
		newRs.backoffSleepMS = make(map[string]int)
		newRs.backoffTimes = make(map[string]int)
		for k, v := range rs.backoffSleepMS {
			newRs.backoffSleepMS[k] += v
		}
		for k, v := range rs.backoffTimes {
			newRs.backoffTimes[k] += v
		}
	}
	return &newRs
}

// Merge implements the RuntimeStats interface.
func (rs *SnapshotRuntimeStats) Merge(other *SnapshotRuntimeStats) {
	if other.rpcStats.Stats != nil {
		if rs.rpcStats.Stats == nil {
			rs.rpcStats.Stats = make(map[tikvrpc.CmdType]*locate.RPCRuntimeStats, len(other.rpcStats.Stats))
		}
		rs.rpcStats.Merge(other.rpcStats)
	}
	if len(other.backoffSleepMS) > 0 {
		if rs.backoffSleepMS == nil {
			rs.backoffSleepMS = make(map[string]int)
		}
		if rs.backoffTimes == nil {
			rs.backoffTimes = make(map[string]int)
		}
		for k, v := range other.backoffSleepMS {
			rs.backoffSleepMS[k] += v
		}
		for k, v := range other.backoffTimes {
			rs.backoffTimes[k] += v
		}
	}
}

// String implements fmt.Stringer interface.
func (rs *SnapshotRuntimeStats) String() string {
	var buf bytes.Buffer
	buf.WriteString(rs.rpcStats.String())
	for k, v := range rs.backoffTimes {
		if buf.Len() > 0 {
			buf.WriteByte(',')
		}
		ms := rs.backoffSleepMS[k]
		d := time.Duration(ms) * time.Millisecond
		buf.WriteString(fmt.Sprintf("%s_backoff:{num:%d, total_time:%s}", k, v, util.FormatDuration(d)))
	}
	timeDetail := rs.timeDetail.String()
	if timeDetail != "" {
		buf.WriteString(", ")
		buf.WriteString(timeDetail)
	}
	scanDetail := rs.scanDetail.String()
	if scanDetail != "" {
		buf.WriteString(", ")
		buf.WriteString(scanDetail)
	}
	return buf.String()
}
