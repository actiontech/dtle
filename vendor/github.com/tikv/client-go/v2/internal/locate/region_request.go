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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/locate/region_request.go
//

// Copyright 2016 PingCAP, Inc.
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

package locate

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
)

// shuttingDown is a flag to indicate tidb-server is exiting (Ctrl+C signal
// receved for example). If this flag is set, tikv client should not retry on
// network error because tidb-server expect tikv client to exit as soon as possible.
var shuttingDown uint32

// StoreShuttingDown atomically stores ShuttingDown into v.
func StoreShuttingDown(v uint32) {
	atomic.StoreUint32(&shuttingDown, v)
}

// LoadShuttingDown atomically loads ShuttingDown.
func LoadShuttingDown() uint32 {
	return atomic.LoadUint32(&shuttingDown)
}

// RegionRequestSender sends KV/Cop requests to tikv server. It handles network
// errors and some region errors internally.
//
// Typically, a KV/Cop request is bind to a region, all keys that are involved
// in the request should be located in the region.
// The sending process begins with looking for the address of leader store's
// address of the target region from cache, and the request is then sent to the
// destination tikv server over TCP connection.
// If region is updated, can be caused by leader transfer, region split, region
// merge, or region balance, tikv server may not able to process request and
// send back a RegionError.
// RegionRequestSender takes care of errors that does not relevant to region
// range, such as 'I/O timeout', 'NotLeader', and 'ServerIsBusy'. If fails to
// send the request to all replicas, a fake rregion error may be returned.
// Caller which receives the error should retry the request.
//
// For other region errors, since region range have changed, the request may need to
// split, so we simply return the error to caller.
type RegionRequestSender struct {
	regionCache       *RegionCache
	client            client.Client
	storeAddr         string
	rpcError          error
	replicaSelector   *replicaSelector
	failStoreIDs      map[uint64]struct{}
	failProxyStoreIDs map[uint64]struct{}
	RegionRequestRuntimeStats
}

// RegionRequestRuntimeStats records the runtime stats of send region requests.
type RegionRequestRuntimeStats struct {
	Stats map[tikvrpc.CmdType]*RPCRuntimeStats
}

// NewRegionRequestRuntimeStats returns a new RegionRequestRuntimeStats.
func NewRegionRequestRuntimeStats() RegionRequestRuntimeStats {
	return RegionRequestRuntimeStats{
		Stats: make(map[tikvrpc.CmdType]*RPCRuntimeStats),
	}
}

// RPCRuntimeStats indicates the RPC request count and consume time.
type RPCRuntimeStats struct {
	Count int64
	// Send region request consume time.
	Consume int64
}

// String implements fmt.Stringer interface.
func (r *RegionRequestRuntimeStats) String() string {
	var builder strings.Builder
	for k, v := range r.Stats {
		if builder.Len() > 0 {
			builder.WriteByte(',')
		}
		// append string: fmt.Sprintf("%s:{num_rpc:%v, total_time:%s}", k.String(), v.Count, util.FormatDuration(time.Duration(v.Consume))")
		builder.WriteString(k.String())
		builder.WriteString(":{num_rpc:")
		builder.WriteString(strconv.FormatInt(v.Count, 10))
		builder.WriteString(", total_time:")
		builder.WriteString(util.FormatDuration(time.Duration(v.Consume)))
		builder.WriteString("}")
	}
	return builder.String()
}

// Clone returns a copy of itself.
func (r *RegionRequestRuntimeStats) Clone() RegionRequestRuntimeStats {
	newRs := NewRegionRequestRuntimeStats()
	for cmd, v := range r.Stats {
		newRs.Stats[cmd] = &RPCRuntimeStats{
			Count:   v.Count,
			Consume: v.Consume,
		}
	}
	return newRs
}

// Merge merges other RegionRequestRuntimeStats.
func (r *RegionRequestRuntimeStats) Merge(rs RegionRequestRuntimeStats) {
	for cmd, v := range rs.Stats {
		stat, ok := r.Stats[cmd]
		if !ok {
			r.Stats[cmd] = &RPCRuntimeStats{
				Count:   v.Count,
				Consume: v.Consume,
			}
			continue
		}
		stat.Count += v.Count
		stat.Consume += v.Consume
	}
}

// RecordRegionRequestRuntimeStats records request runtime stats.
func RecordRegionRequestRuntimeStats(stats map[tikvrpc.CmdType]*RPCRuntimeStats, cmd tikvrpc.CmdType, d time.Duration) {
	stat, ok := stats[cmd]
	if !ok {
		stats[cmd] = &RPCRuntimeStats{
			Count:   1,
			Consume: int64(d),
		}
		return
	}
	stat.Count++
	stat.Consume += int64(d)
}

// NewRegionRequestSender creates a new sender.
func NewRegionRequestSender(regionCache *RegionCache, client client.Client) *RegionRequestSender {
	return &RegionRequestSender{
		regionCache: regionCache,
		client:      client,
	}
}

// GetRegionCache returns the region cache.
func (s *RegionRequestSender) GetRegionCache() *RegionCache {
	return s.regionCache
}

// GetClient returns the RPC client.
func (s *RegionRequestSender) GetClient() client.Client {
	return s.client
}

// SetStoreAddr specifies the dest store address.
func (s *RegionRequestSender) SetStoreAddr(addr string) {
	s.storeAddr = addr
}

// GetStoreAddr returns the dest store address.
func (s *RegionRequestSender) GetStoreAddr() string {
	return s.storeAddr
}

// GetRPCError returns the RPC error.
func (s *RegionRequestSender) GetRPCError() error {
	return s.rpcError
}

// SetRPCError rewrite the rpc error.
func (s *RegionRequestSender) SetRPCError(err error) {
	s.rpcError = err
}

// SendReq sends a request to tikv server. If fails to send the request to all replicas,
// a fake region error may be returned. Caller which receives the error should retry the request.
func (s *RegionRequestSender) SendReq(bo *retry.Backoffer, req *tikvrpc.Request, regionID RegionVerID, timeout time.Duration) (*tikvrpc.Response, error) {
	resp, _, err := s.SendReqCtx(bo, req, regionID, timeout, tikvrpc.TiKV)
	return resp, err
}

type replica struct {
	store    *Store
	peer     *metapb.Peer
	epoch    uint32
	attempts int
}

func (r *replica) isEpochStale() bool {
	return r.epoch != atomic.LoadUint32(&r.store.epoch)
}

func (r *replica) isExhausted(maxAttempt int) bool {
	return r.attempts >= maxAttempt
}

type replicaSelector struct {
	regionCache *RegionCache
	region      *Region
	regionStore *regionStore
	replicas    []*replica
	state       selectorState
	// replicas[targetIdx] is the replica handling the request this time
	targetIdx AccessIndex
	// replicas[proxyIdx] is the store used to redirect requests this time
	proxyIdx AccessIndex
}

// selectorState is the interface of states of the replicaSelector.
// Here is the main state transition diagram:
//
//                                    exceeding maxReplicaAttempt
//           +-------------------+   || RPC failure && unreachable && no forwarding
// +-------->+ accessKnownLeader +----------------+
// |         +------+------------+                |
// |                |                             |
// |                | RPC failure                 v
// |                | && unreachable        +-----+-----+
// |                | && enable forwarding  |tryFollower+------+
// |                |                       +-----------+      |
// | leader becomes v                                          | all followers
// | reachable +----+-------------+                            | are tried
// +-----------+accessByKnownProxy|                            |
// ^           +------+-----------+                            |
// |                  |                           +-------+    |
// |                  | RPC failure               |backoff+<---+
// | leader becomes   v                           +---+---+
// | reachable  +-----+-----+ all proxies are tried   ^
// +------------+tryNewProxy+-------------------------+
//              +-----------+
type selectorState interface {
	next(*retry.Backoffer, *replicaSelector) (*RPCContext, error)
	onSendSuccess(*replicaSelector)
	onSendFailure(*retry.Backoffer, *replicaSelector, error)
	onNoLeader(*replicaSelector)
}

type stateChanged struct{}

func (c stateChanged) Error() string {
	return "replicaSelector state changed"
}

type stateBase struct{}

func (s stateBase) next(bo *retry.Backoffer, selector *replicaSelector) (*RPCContext, error) {
	return nil, nil
}

func (s stateBase) onSendSuccess(selector *replicaSelector) {
}

func (s stateBase) onSendFailure(backoffer *retry.Backoffer, selector *replicaSelector, err error) {
}

func (s stateBase) onNoLeader(selector *replicaSelector) {
}

// accessKnownLeader is the state where we are sending requests
// to the leader we suppose to be.
//
// After attempting maxReplicaAttempt times without success
// and without receiving new leader from the responses error,
// we should switch to tryFollower state.
type accessKnownLeader struct {
	stateBase
	leaderIdx AccessIndex
}

func (state *accessKnownLeader) next(bo *retry.Backoffer, selector *replicaSelector) (*RPCContext, error) {
	leader := selector.replicas[state.leaderIdx]
	if leader.isExhausted(maxReplicaAttempt) {
		selector.state = &tryFollower{leaderIdx: state.leaderIdx, lastIdx: state.leaderIdx}
		return nil, stateChanged{}
	}
	selector.targetIdx = state.leaderIdx
	return selector.buildRPCContext(bo)
}

func (state *accessKnownLeader) onSendFailure(bo *retry.Backoffer, selector *replicaSelector, cause error) {
	liveness := selector.checkLiveness(bo, selector.targetReplica())
	if liveness != reachable && len(selector.replicas) > 1 && selector.regionCache.enableForwarding {
		selector.state = &accessByKnownProxy{leaderIdx: state.leaderIdx}
		return
	}
	if liveness != reachable || selector.targetReplica().isExhausted(maxReplicaAttempt) {
		selector.state = &tryFollower{leaderIdx: state.leaderIdx, lastIdx: state.leaderIdx}
	}
	if liveness != reachable {
		selector.invalidateReplicaStore(selector.targetReplica(), cause)
	}
}

func (state *accessKnownLeader) onNoLeader(selector *replicaSelector) {
	selector.state = &tryFollower{leaderIdx: state.leaderIdx, lastIdx: state.leaderIdx}
}

// tryFollower is the state where we cannot access the known leader
// but still try other replicas in case they have become the leader.
//
// In this state, a follower that is not tried will be used. If all
// followers are tried, we think we have exhausted the replicas.
// On sending failure in this state, if leader info is returned,
// the leader will be updated to replicas[0] and give it another chance.
type tryFollower struct {
	stateBase
	leaderIdx AccessIndex
	lastIdx   AccessIndex
}

func (state *tryFollower) next(bo *retry.Backoffer, selector *replicaSelector) (*RPCContext, error) {
	var targetReplica *replica
	// Search replica that is not attempted from the last accessed replica
	for i := 1; i < len(selector.replicas); i++ {
		idx := AccessIndex((int(state.lastIdx) + i) % len(selector.replicas))
		if idx == state.leaderIdx {
			continue
		}
		targetReplica = selector.replicas[idx]
		// Each follower is only tried once
		if !targetReplica.isExhausted(1) {
			state.lastIdx = idx
			selector.targetIdx = idx
			break
		}
	}
	// If all followers are tried and fail, backoff and retry.
	if selector.targetIdx < 0 {
		metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("exhausted").Inc()
		selector.invalidateRegion()
		return nil, nil
	}
	return selector.buildRPCContext(bo)
}

func (state *tryFollower) onSendSuccess(selector *replicaSelector) {
	if !selector.regionCache.switchWorkLeaderToPeer(selector.region, selector.targetReplica().peer) {
		panic("the store must exist")
	}
}

func (state *tryFollower) onSendFailure(bo *retry.Backoffer, selector *replicaSelector, cause error) {
	if selector.checkLiveness(bo, selector.targetReplica()) != reachable {
		selector.invalidateReplicaStore(selector.targetReplica(), cause)
	}
}

// accessByKnownProxy is the state where we are sending requests through
// regionStore.proxyTiKVIdx as a proxy.
type accessByKnownProxy struct {
	stateBase
	leaderIdx AccessIndex
}

func (state *accessByKnownProxy) next(bo *retry.Backoffer, selector *replicaSelector) (*RPCContext, error) {
	leader := selector.replicas[state.leaderIdx]
	if atomic.LoadInt32(&leader.store.unreachable) == 0 {
		selector.regionStore.unsetProxyStoreIfNeeded(selector.region)
		selector.state = &accessKnownLeader{leaderIdx: state.leaderIdx}
		return nil, stateChanged{}
	}

	if selector.regionStore.proxyTiKVIdx >= 0 {
		selector.targetIdx = state.leaderIdx
		selector.proxyIdx = selector.regionStore.proxyTiKVIdx
		return selector.buildRPCContext(bo)
	}

	selector.state = &tryNewProxy{leaderIdx: state.leaderIdx}
	return nil, stateChanged{}
}

func (state *accessByKnownProxy) onSendFailure(bo *retry.Backoffer, selector *replicaSelector, cause error) {
	selector.state = &tryNewProxy{leaderIdx: state.leaderIdx}
	if selector.checkLiveness(bo, selector.proxyReplica()) != reachable {
		selector.invalidateReplicaStore(selector.proxyReplica(), cause)
	}
}

func (state *accessByKnownProxy) onNoLeader(selector *replicaSelector) {
	selector.state = &invalidLeader{}
}

// tryNewProxy is the state where we try to find a node from followers as proxy.
type tryNewProxy struct {
	stateBase
	leaderIdx AccessIndex
}

func (state *tryNewProxy) next(bo *retry.Backoffer, selector *replicaSelector) (*RPCContext, error) {
	leader := selector.replicas[state.leaderIdx]
	if atomic.LoadInt32(&leader.store.unreachable) == 0 {
		selector.regionStore.unsetProxyStoreIfNeeded(selector.region)
		selector.state = &accessKnownLeader{leaderIdx: state.leaderIdx}
		return nil, stateChanged{}
	}

	candidateNum := 0
	for idx, replica := range selector.replicas {
		if state.isCandidate(AccessIndex(idx), replica) {
			candidateNum++
		}
	}

	// If all followers are tried as a proxy and fail, mark the leader store invalid, then backoff and retry.
	if candidateNum == 0 {
		metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("exhausted").Inc()
		selector.invalidateReplicaStore(leader, errors.Errorf("all followers are tried as proxy but fail"))
		selector.region.scheduleReload()
		return nil, nil
	}

	// Skip advanceCnt valid candidates to find a proxy peer randomly
	advanceCnt := rand.Intn(candidateNum)
	for idx, replica := range selector.replicas {
		if !state.isCandidate(AccessIndex(idx), replica) {
			continue
		}
		if advanceCnt == 0 {
			selector.targetIdx = state.leaderIdx
			selector.proxyIdx = AccessIndex(idx)
			break
		}
		advanceCnt--
	}
	return selector.buildRPCContext(bo)
}

func (state *tryNewProxy) isCandidate(idx AccessIndex, replica *replica) bool {
	// Try each peer only once
	return idx != state.leaderIdx && !replica.isExhausted(1)
}

func (state *tryNewProxy) onSendSuccess(selector *replicaSelector) {
	selector.regionStore.setProxyStoreIdx(selector.region, selector.proxyIdx)
}

func (state *tryNewProxy) onSendFailure(bo *retry.Backoffer, selector *replicaSelector, cause error) {
	if selector.checkLiveness(bo, selector.proxyReplica()) != reachable {
		selector.invalidateReplicaStore(selector.proxyReplica(), cause)
	}
}

func (state *tryNewProxy) onNoLeader(selector *replicaSelector) {
	selector.state = &invalidLeader{}
}

// accessFollower is the state where we are sending requests to TiKV followers.
// If there is no suitable follower, requests will be sent to the leader as a fallback.
type accessFollower struct {
	stateBase
	// If tryLeader is true, the request can also be sent to the leader.
	tryLeader         bool
	isGlobalStaleRead bool
	option            storeSelectorOp
	leaderIdx         AccessIndex
	lastIdx           AccessIndex
}

func (state *accessFollower) next(bo *retry.Backoffer, selector *replicaSelector) (*RPCContext, error) {
	if state.lastIdx < 0 {
		if state.tryLeader {
			state.lastIdx = AccessIndex(rand.Intn(len(selector.replicas)))
		} else {
			if len(selector.replicas) <= 1 {
				state.lastIdx = state.leaderIdx
			} else {
				// Randomly select a non-leader peer
				state.lastIdx = AccessIndex(rand.Intn(len(selector.replicas) - 1))
				if state.lastIdx >= state.leaderIdx {
					state.lastIdx++
				}
			}
		}
	} else {
		// Stale Read request will retry the leader or next peer on error,
		// if txnScope is global, we will only retry the leader by using the WithLeaderOnly option,
		// if txnScope is local, we will retry both other peers and the leader by the strategy of replicaSelector.
		if state.isGlobalStaleRead {
			WithLeaderOnly()(&state.option)
		}
		state.lastIdx++
	}

	for i := 0; i < len(selector.replicas) && !state.option.leaderOnly; i++ {
		idx := AccessIndex((int(state.lastIdx) + i) % len(selector.replicas))
		if state.isCandidate(idx, selector.replicas[idx]) {
			state.lastIdx = idx
			selector.targetIdx = idx
			break
		}
	}
	// If there is no candidate, fallback to the leader.
	if selector.targetIdx < 0 {
		if len(state.option.labels) > 0 {
			logutil.BgLogger().Warn("unable to find stores with given labels")
		}
		leader := selector.replicas[state.leaderIdx]
		if leader.isEpochStale() || leader.isExhausted(1) {
			metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("exhausted").Inc()
			selector.invalidateRegion()
			return nil, nil
		}
		state.lastIdx = state.leaderIdx
		selector.targetIdx = state.leaderIdx
	}
	return selector.buildRPCContext(bo)
}

func (state *accessFollower) onSendFailure(bo *retry.Backoffer, selector *replicaSelector, cause error) {
	if selector.checkLiveness(bo, selector.targetReplica()) != reachable {
		selector.invalidateReplicaStore(selector.targetReplica(), cause)
	}
}

func (state *accessFollower) isCandidate(idx AccessIndex, replica *replica) bool {
	return !replica.isEpochStale() && !replica.isExhausted(1) &&
		// The request can only be sent to the leader.
		((state.option.leaderOnly && idx == state.leaderIdx) ||
			// Choose a replica with matched labels.
			(!state.option.leaderOnly && (state.tryLeader || idx != state.leaderIdx) && replica.store.IsLabelsMatch(state.option.labels)))
}

type invalidStore struct {
	stateBase
}

func (state *invalidStore) next(_ *retry.Backoffer, _ *replicaSelector) (*RPCContext, error) {
	metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("invalidStore").Inc()
	return nil, nil
}

// TODO(sticnarf): If using request forwarding and the leader is unknown, try other followers
// instead of just switching to this state to backoff and retry.
type invalidLeader struct {
	stateBase
}

func (state *invalidLeader) next(_ *retry.Backoffer, _ *replicaSelector) (*RPCContext, error) {
	metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("invalidLeader").Inc()
	return nil, nil
}

// newReplicaSelector creates a replicaSelector which selects replicas according to reqType and opts.
// opts is currently only effective for follower read.
func newReplicaSelector(regionCache *RegionCache, regionID RegionVerID, req *tikvrpc.Request, opts ...StoreSelectorOption) (*replicaSelector, error) {
	cachedRegion := regionCache.GetCachedRegionWithRLock(regionID)
	if cachedRegion == nil || !cachedRegion.isValid() {
		return nil, nil
	}
	regionStore := cachedRegion.getStore()
	replicas := make([]*replica, 0, regionStore.accessStoreNum(tiKVOnly))
	for _, storeIdx := range regionStore.accessIndex[tiKVOnly] {
		replicas = append(replicas, &replica{
			store:    regionStore.stores[storeIdx],
			peer:     cachedRegion.meta.Peers[storeIdx],
			epoch:    regionStore.storeEpochs[storeIdx],
			attempts: 0,
		})
	}
	var state selectorState
	if !req.ReplicaReadType.IsFollowerRead() {
		if regionCache.enableForwarding && regionStore.proxyTiKVIdx >= 0 {
			state = &accessByKnownProxy{leaderIdx: regionStore.workTiKVIdx}
		} else {
			state = &accessKnownLeader{leaderIdx: regionStore.workTiKVIdx}
		}
	} else {
		option := storeSelectorOp{}
		for _, op := range opts {
			op(&option)
		}
		state = &accessFollower{
			tryLeader:         req.ReplicaReadType == kv.ReplicaReadMixed,
			isGlobalStaleRead: req.IsGlobalStaleRead(),
			option:            option,
			leaderIdx:         regionStore.workTiKVIdx,
			lastIdx:           -1,
		}
	}

	return &replicaSelector{
		regionCache,
		cachedRegion,
		regionStore,
		replicas,
		state,
		-1,
		-1,
	}, nil
}

const maxReplicaAttempt = 10

// next creates the RPCContext of the current candidate replica.
// It returns a SendError if runs out of all replicas or the cached region is invalidated.
func (s *replicaSelector) next(bo *retry.Backoffer) (rpcCtx *RPCContext, err error) {
	if !s.region.isValid() {
		metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("invalid").Inc()
		return nil, nil
	}

	s.targetIdx = -1
	s.proxyIdx = -1
	s.refreshRegionStore()
	for {
		rpcCtx, err = s.state.next(bo, s)
		if _, isStateChanged := err.(stateChanged); !isStateChanged {
			return
		}
	}
}

func (s *replicaSelector) targetReplica() *replica {
	if s.targetIdx >= 0 && int(s.targetIdx) < len(s.replicas) {
		return s.replicas[s.targetIdx]
	}
	return nil
}

func (s *replicaSelector) proxyReplica() *replica {
	if s.proxyIdx >= 0 && int(s.proxyIdx) < len(s.replicas) {
		return s.replicas[s.proxyIdx]
	}
	return nil
}

func (s *replicaSelector) refreshRegionStore() {
	oldRegionStore := s.regionStore
	newRegionStore := s.region.getStore()
	if oldRegionStore == newRegionStore {
		return
	}
	s.regionStore = newRegionStore

	// In the current implementation, if stores change, the address of it must change.
	// So we just compare the address here.
	// When stores change, we mark this replicaSelector as invalid to let the caller
	// recreate a new replicaSelector.
	if &oldRegionStore.stores != &newRegionStore.stores {
		s.state = &invalidStore{}
		return
	}

	// If leader has changed, it means a recent request succeeds an RPC
	// on the new leader.
	if oldRegionStore.workTiKVIdx != newRegionStore.workTiKVIdx {
		switch state := s.state.(type) {
		case *accessFollower:
			state.leaderIdx = newRegionStore.workTiKVIdx
		default:
			// Try the new leader and give it an addition chance if the
			// request is sent to the leader.
			newLeaderIdx := newRegionStore.workTiKVIdx
			s.state = &accessKnownLeader{leaderIdx: newLeaderIdx}
			if s.replicas[newLeaderIdx].attempts == maxReplicaAttempt {
				s.replicas[newLeaderIdx].attempts--
			}
		}
	}
}

func (s *replicaSelector) buildRPCContext(bo *retry.Backoffer) (*RPCContext, error) {
	targetReplica, proxyReplica := s.targetReplica(), s.proxyReplica()

	// Backoff and retry if no replica is selected or the selected replica is stale
	if targetReplica == nil || targetReplica.isEpochStale() ||
		(proxyReplica != nil && proxyReplica.isEpochStale()) {
		// TODO(youjiali1995): Is it necessary to invalidate the region?
		metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("stale_store").Inc()
		s.invalidateRegion()
		return nil, nil
	}

	rpcCtx := &RPCContext{
		Region:     s.region.VerID(),
		Meta:       s.region.meta,
		Peer:       targetReplica.peer,
		Store:      targetReplica.store,
		AccessMode: tiKVOnly,
		TiKVNum:    len(s.replicas),
	}

	// Set leader addr
	addr, err := s.regionCache.getStoreAddr(bo, s.region, targetReplica.store)
	if err != nil {
		return nil, err
	}
	if len(addr) == 0 {
		return nil, nil
	}
	rpcCtx.Addr = addr
	targetReplica.attempts++

	// Set proxy addr
	if proxyReplica != nil {
		addr, err = s.regionCache.getStoreAddr(bo, s.region, proxyReplica.store)
		if err != nil {
			return nil, err
		}
		if len(addr) == 0 {
			return nil, nil
		}
		rpcCtx.ProxyStore = proxyReplica.store
		rpcCtx.ProxyAddr = addr
		proxyReplica.attempts++
	}

	return rpcCtx, nil
}

func (s *replicaSelector) onSendFailure(bo *retry.Backoffer, err error) {
	metrics.RegionCacheCounterWithSendFail.Inc()
	s.state.onSendFailure(bo, s, err)
}

func (s *replicaSelector) checkLiveness(bo *retry.Backoffer, accessReplica *replica) livenessState {
	store := accessReplica.store
	liveness := store.requestLiveness(bo, s.regionCache)
	// We only check health in loop if forwarding is enabled now.
	// The restriction might be relaxed if necessary, but the implementation
	// may be checked carefully again.
	if liveness != reachable && s.regionCache.enableForwarding {
		store.startHealthCheckLoopIfNeeded(s.regionCache)
	}
	return liveness
}

func (s *replicaSelector) invalidateReplicaStore(replica *replica, cause error) {
	store := replica.store
	if atomic.CompareAndSwapUint32(&store.epoch, replica.epoch, replica.epoch+1) {
		logutil.BgLogger().Info("mark store's regions need be refill", zap.Uint64("id", store.storeID), zap.String("addr", store.addr), zap.Error(cause))
		metrics.RegionCacheCounterWithInvalidateStoreRegionsOK.Inc()
		// schedule a store addr resolve.
		store.markNeedCheck(s.regionCache.notifyCheckCh)
	}
}

func (s *replicaSelector) onSendSuccess() {
	s.state.onSendSuccess(s)
}

func (s *replicaSelector) onNotLeader(bo *retry.Backoffer, ctx *RPCContext, notLeader *errorpb.NotLeader) (shouldRetry bool, err error) {
	leader := notLeader.GetLeader()
	if leader == nil {
		// The region may be during transferring leader.
		s.state.onNoLeader(s)
		if err = bo.Backoff(retry.BoRegionScheduling, errors.Errorf("no leader, ctx: %v", ctx)); err != nil {
			return false, err
		}
	} else {
		s.updateLeader(notLeader.GetLeader())
	}
	return true, nil
}

// updateLeader updates the leader of the cached region.
// If the leader peer isn't found in the region, the region will be invalidated.
func (s *replicaSelector) updateLeader(leader *metapb.Peer) {
	if leader == nil {
		return
	}
	for i, replica := range s.replicas {
		if isSamePeer(replica.peer, leader) {
			if replica.isExhausted(maxReplicaAttempt) {
				// Give the replica one more chance and because each follower is tried only once,
				// it won't result in infinite retry.
				replica.attempts = maxReplicaAttempt - 1
			}
			s.state = &accessKnownLeader{leaderIdx: AccessIndex(i)}
			// Update the workTiKVIdx so that following requests can be sent to the leader immediately.
			if !s.regionCache.switchWorkLeaderToPeer(s.region, leader) {
				panic("the store must exist")
			}
			logutil.BgLogger().Debug("switch region leader to specific leader due to kv return NotLeader",
				zap.Uint64("regionID", s.region.GetID()),
				zap.Uint64("leaderStoreID", leader.GetStoreId()))
			return
		}
	}
	// Invalidate the region since the new leader is not in the cached version.
	s.region.invalidate(StoreNotFound)
}

func (s *replicaSelector) invalidateRegion() {
	if s.region != nil {
		s.region.invalidate(Other)
	}
}

func (s *RegionRequestSender) getRPCContext(
	bo *retry.Backoffer,
	req *tikvrpc.Request,
	regionID RegionVerID,
	et tikvrpc.EndpointType,
	opts ...StoreSelectorOption,
) (*RPCContext, error) {
	switch et {
	case tikvrpc.TiKV:
		if s.replicaSelector == nil {
			selector, err := newReplicaSelector(s.regionCache, regionID, req, opts...)
			if selector == nil || err != nil {
				return nil, err
			}
			s.replicaSelector = selector
		}
		return s.replicaSelector.next(bo)
	case tikvrpc.TiFlash:
		return s.regionCache.GetTiFlashRPCContext(bo, regionID, true)
	case tikvrpc.TiDB:
		return &RPCContext{Addr: s.storeAddr}, nil
	default:
		return nil, errors.Errorf("unsupported storage type: %v", et)
	}
}

func (s *RegionRequestSender) reset() {
	s.replicaSelector = nil
	s.failStoreIDs = nil
	s.failProxyStoreIDs = nil
}

// IsFakeRegionError returns true if err is fack region error.
func IsFakeRegionError(err *errorpb.Error) bool {
	return err != nil && err.GetEpochNotMatch() != nil && len(err.GetEpochNotMatch().CurrentRegions) == 0
}

// SendReqCtx sends a request to tikv server and return response and RPCCtx of this RPC.
func (s *RegionRequestSender) SendReqCtx(
	bo *retry.Backoffer,
	req *tikvrpc.Request,
	regionID RegionVerID,
	timeout time.Duration,
	et tikvrpc.EndpointType,
	opts ...StoreSelectorOption,
) (
	resp *tikvrpc.Response,
	rpcCtx *RPCContext,
	err error,
) {
	if span := opentracing.SpanFromContext(bo.GetCtx()); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("regionRequest.SendReqCtx", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.SetCtx(opentracing.ContextWithSpan(bo.GetCtx(), span1))
	}

	if val, err := util.EvalFailpoint("tikvStoreSendReqResult"); err == nil {
		switch val.(string) {
		case "timeout":
			return nil, nil, errors.New("timeout")
		case "GCNotLeader":
			if req.Type == tikvrpc.CmdGC {
				return &tikvrpc.Response{
					Resp: &kvrpcpb.GCResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
				}, nil, nil
			}
		case "GCServerIsBusy":
			if req.Type == tikvrpc.CmdGC {
				return &tikvrpc.Response{
					Resp: &kvrpcpb.GCResponse{RegionError: &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}},
				}, nil, nil
			}
		case "busy":
			return &tikvrpc.Response{
				Resp: &kvrpcpb.GCResponse{RegionError: &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}},
			}, nil, nil
		case "requestTiDBStoreError":
			if et == tikvrpc.TiDB {
				return nil, nil, errors.WithStack(tikverr.ErrTiKVServerTimeout)
			}
		case "requestTiFlashError":
			if et == tikvrpc.TiFlash {
				return nil, nil, errors.WithStack(tikverr.ErrTiFlashServerTimeout)
			}
		}
	}

	// If the MaxExecutionDurationMs is not set yet, we set it to be the RPC timeout duration
	// so TiKV can give up the requests whose response TiDB cannot receive due to timeout.
	if req.Context.MaxExecutionDurationMs == 0 {
		req.Context.MaxExecutionDurationMs = uint64(timeout.Milliseconds())
	}

	s.reset()
	tryTimes := 0
	defer func() {
		if tryTimes > 0 {
			metrics.TiKVRequestRetryTimesHistogram.Observe(float64(tryTimes))
		}
	}()
	for {
		if tryTimes > 0 {
			req.IsRetryRequest = true
			if tryTimes%100 == 0 {
				logutil.Logger(bo.GetCtx()).Warn("retry", zap.Uint64("region", regionID.GetID()), zap.Int("times", tryTimes))
			}
		}

		rpcCtx, err = s.getRPCContext(bo, req, regionID, et, opts...)
		if err != nil {
			return nil, nil, err
		}

		if _, err := util.EvalFailpoint("invalidCacheAndRetry"); err == nil {
			// cooperate with tikvclient/setGcResolveMaxBackoff
			if c := bo.GetCtx().Value("injectedBackoff"); c != nil {
				resp, err = tikvrpc.GenRegionErrorResp(req, &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}})
				return resp, nil, err
			}
		}
		if rpcCtx == nil {
			// TODO(youjiali1995): remove it when using the replica selector for all requests.
			// If the region is not found in cache, it must be out
			// of date and already be cleaned up. We can skip the
			// RPC by returning RegionError directly.

			// TODO: Change the returned error to something like "region missing in cache",
			// and handle this error like EpochNotMatch, which means to re-split the request and retry.
			logutil.Logger(bo.GetCtx()).Debug("throwing pseudo region error due to region not found in cache", zap.Stringer("region", &regionID))
			resp, err = tikvrpc.GenRegionErrorResp(req, &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}})
			return resp, nil, err
		}

		logutil.Eventf(bo.GetCtx(), "send %s request to region %d at %s", req.Type, regionID.id, rpcCtx.Addr)
		s.storeAddr = rpcCtx.Addr
		var retry bool
		resp, retry, err = s.sendReqToRegion(bo, rpcCtx, req, timeout)
		if err != nil {
			return nil, nil, err
		}

		// recheck whether the session/query is killed during the Next()
		boVars := bo.GetVars()
		if boVars != nil && boVars.Killed != nil && atomic.LoadUint32(boVars.Killed) == 1 {
			return nil, nil, errors.WithStack(tikverr.ErrQueryInterrupted)
		}
		if val, err := util.EvalFailpoint("mockRetrySendReqToRegion"); err == nil {
			if val.(bool) {
				retry = true
			}
		}
		if retry {
			tryTimes++
			continue
		}

		var regionErr *errorpb.Error
		regionErr, err = resp.GetRegionError()
		if err != nil {
			return nil, nil, err
		}
		if regionErr != nil {
			retry, err = s.onRegionError(bo, rpcCtx, req, regionErr)
			if err != nil {
				return nil, nil, err
			}
			if retry {
				tryTimes++
				continue
			}
		} else {
			if s.replicaSelector != nil {
				s.replicaSelector.onSendSuccess()
			}
		}
		return resp, rpcCtx, nil
	}
}

// RPCCancellerCtxKey is context key attach rpc send cancelFunc collector to ctx.
type RPCCancellerCtxKey struct{}

// RPCCanceller is rpc send cancelFunc collector.
type RPCCanceller struct {
	sync.Mutex
	allocID   int
	cancels   map[int]func()
	cancelled bool
}

// NewRPCanceller creates RPCCanceller with init state.
func NewRPCanceller() *RPCCanceller {
	return &RPCCanceller{cancels: make(map[int]func())}
}

// WithCancel generates new context with cancel func.
func (h *RPCCanceller) WithCancel(ctx context.Context) (context.Context, func()) {
	nctx, cancel := context.WithCancel(ctx)
	h.Lock()
	if h.cancelled {
		h.Unlock()
		cancel()
		return nctx, func() {}
	}
	id := h.allocID
	h.allocID++
	h.cancels[id] = cancel
	h.Unlock()
	return nctx, func() {
		cancel()
		h.Lock()
		delete(h.cancels, id)
		h.Unlock()
	}
}

// CancelAll cancels all inflight rpc context.
func (h *RPCCanceller) CancelAll() {
	h.Lock()
	for _, c := range h.cancels {
		c()
	}
	h.cancelled = true
	h.Unlock()
}

func (s *RegionRequestSender) sendReqToRegion(bo *retry.Backoffer, rpcCtx *RPCContext, req *tikvrpc.Request, timeout time.Duration) (resp *tikvrpc.Response, retry bool, err error) {
	if e := tikvrpc.SetContext(req, rpcCtx.Meta, rpcCtx.Peer); e != nil {
		return nil, false, err
	}
	// judge the store limit switch.
	if limit := kv.StoreLimit.Load(); limit > 0 {
		if err := s.getStoreToken(rpcCtx.Store, limit); err != nil {
			return nil, false, err
		}
		defer s.releaseStoreToken(rpcCtx.Store)
	}

	ctx := bo.GetCtx()
	if rawHook := ctx.Value(RPCCancellerCtxKey{}); rawHook != nil {
		var cancel context.CancelFunc
		ctx, cancel = rawHook.(*RPCCanceller).WithCancel(ctx)
		defer cancel()
	}

	// sendToAddr is the first target address that will receive the request. If proxy is used, sendToAddr will point to
	// the proxy that will forward the request to the final target.
	sendToAddr := rpcCtx.Addr
	if rpcCtx.ProxyStore == nil {
		req.ForwardedHost = ""
	} else {
		req.ForwardedHost = rpcCtx.Addr
		sendToAddr = rpcCtx.ProxyAddr
	}

	var sessionID uint64
	if v := bo.GetCtx().Value(util.SessionID); v != nil {
		sessionID = v.(uint64)
	}

	injectFailOnSend := false
	if val, e := util.EvalFailpoint("rpcFailOnSend"); e == nil {
		inject := true
		// Optional filters
		if s, ok := val.(string); ok {
			if s == "greengc" && !req.IsGreenGCRequest() {
				inject = false
			} else if s == "write" && !req.IsTxnWriteRequest() {
				inject = false
			}
		} else if sessionID == 0 {
			inject = false
		}

		if inject {
			logutil.Logger(ctx).Info("[failpoint] injected RPC error on send", zap.Stringer("type", req.Type),
				zap.Stringer("req", req.Req.(fmt.Stringer)), zap.Stringer("ctx", &req.Context))
			injectFailOnSend = true
			err = errors.New("injected RPC error on send")
		}
	}

	if !injectFailOnSend {
		start := time.Now()
		resp, err = s.client.SendRequest(ctx, sendToAddr, req, timeout)
		if s.Stats != nil {
			RecordRegionRequestRuntimeStats(s.Stats, req.Type, time.Since(start))
			if val, err := util.EvalFailpoint("tikvStoreRespResult"); err == nil {
				if val.(bool) {
					if req.Type == tikvrpc.CmdCop && bo.GetTotalSleep() == 0 {
						return &tikvrpc.Response{
							Resp: &coprocessor.Response{RegionError: &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}},
						}, false, nil
					}
				}
			}
		}

		if val, e := util.EvalFailpoint("rpcFailOnRecv"); e == nil {
			inject := true
			// Optional filters
			if s, ok := val.(string); ok {
				if s == "greengc" && !req.IsGreenGCRequest() {
					inject = false
				} else if s == "write" && !req.IsTxnWriteRequest() {
					inject = false
				}
			} else if sessionID == 0 {
				inject = false
			}

			if inject {
				logutil.Logger(ctx).Info("[failpoint] injected RPC error on recv", zap.Stringer("type", req.Type),
					zap.Stringer("req", req.Req.(fmt.Stringer)), zap.Stringer("ctx", &req.Context))
				err = errors.New("injected RPC error on recv")
				resp = nil
			}
		}

		if val, e := util.EvalFailpoint("rpcContextCancelErr"); e == nil {
			if val.(bool) {
				ctx1, cancel := context.WithCancel(context.Background())
				cancel()
				<-ctx1.Done()
				ctx = ctx1
				err = ctx.Err()
				resp = nil
			}
		}
	}

	if rpcCtx.ProxyStore != nil {
		fromStore := strconv.FormatUint(rpcCtx.ProxyStore.storeID, 10)
		toStore := strconv.FormatUint(rpcCtx.Store.storeID, 10)
		result := "ok"
		if err != nil {
			result = "fail"
		}
		metrics.TiKVForwardRequestCounter.WithLabelValues(fromStore, toStore, req.Type.String(), result).Inc()
	}

	if err != nil {
		s.rpcError = err

		// Because in rpc logic, context.Cancel() will be transferred to rpcContext.Cancel error. For rpcContext cancel,
		// we need to retry the request. But for context cancel active, for example, limitExec gets the required rows,
		// we shouldn't retry the request, it will go to backoff and hang in retry logic.
		if ctx.Err() != nil && errors.Cause(ctx.Err()) == context.Canceled {
			return nil, false, errors.WithStack(ctx.Err())
		}

		if val, e := util.EvalFailpoint("noRetryOnRpcError"); e == nil {
			if val.(bool) {
				return nil, false, err
			}
		}
		if e := s.onSendFail(bo, rpcCtx, err); e != nil {
			return nil, false, err
		}
		return nil, true, nil
	}
	return
}

func (s *RegionRequestSender) getStoreToken(st *Store, limit int64) error {
	// Checking limit is not thread safe, preferring this for avoiding load in loop.
	count := st.tokenCount.Load()
	if count < limit {
		// Adding tokenCount is no thread safe, preferring this for avoiding check in loop.
		st.tokenCount.Add(1)
		return nil
	}
	metrics.TiKVStoreLimitErrorCounter.WithLabelValues(st.addr, strconv.FormatUint(st.storeID, 10)).Inc()
	return errors.WithStack(&tikverr.ErrTokenLimit{StoreID: st.storeID})
}

func (s *RegionRequestSender) releaseStoreToken(st *Store) {
	count := st.tokenCount.Load()
	// Decreasing tokenCount is no thread safe, preferring this for avoiding check in loop.
	if count > 0 {
		st.tokenCount.Sub(1)
		return
	}
	logutil.BgLogger().Warn("release store token failed, count equals to 0")
}

func (s *RegionRequestSender) onSendFail(bo *retry.Backoffer, ctx *RPCContext, err error) error {
	if span := opentracing.SpanFromContext(bo.GetCtx()); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("regionRequest.onSendFail", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.SetCtx(opentracing.ContextWithSpan(bo.GetCtx(), span1))
	}
	// If it failed because the context is cancelled by ourself, don't retry.
	if errors.Cause(err) == context.Canceled {
		return errors.WithStack(err)
	} else if LoadShuttingDown() > 0 {
		return errors.WithStack(tikverr.ErrTiDBShuttingDown)
	}
	if status.Code(errors.Cause(err)) == codes.Canceled {
		select {
		case <-bo.GetCtx().Done():
			return errors.WithStack(err)
		default:
			// If we don't cancel, but the error code is Canceled, it must be from grpc remote.
			// This may happen when tikv is killed and exiting.
			// Backoff and retry in this case.
			logutil.BgLogger().Warn("receive a grpc cancel signal from remote", zap.Error(err))
		}
	}

	if ctx.Meta != nil {
		if s.replicaSelector != nil {
			s.replicaSelector.onSendFailure(bo, err)
		} else {
			s.regionCache.OnSendFail(bo, ctx, s.NeedReloadRegion(ctx), err)
		}
	}

	// Retry on send request failure when it's not canceled.
	// When a store is not available, the leader of related region should be elected quickly.
	// TODO: the number of retry time should be limited:since region may be unavailable
	// when some unrecoverable disaster happened.
	if ctx.Store != nil && ctx.Store.storeType == tikvrpc.TiFlash {
		err = bo.Backoff(retry.BoTiFlashRPC, errors.Errorf("send tiflash request error: %v, ctx: %v, try next peer later", err, ctx))
	} else {
		err = bo.Backoff(retry.BoTiKVRPC, errors.Errorf("send tikv request error: %v, ctx: %v, try next peer later", err, ctx))
	}
	return err
}

// NeedReloadRegion checks is all peers has sent failed, if so need reload.
func (s *RegionRequestSender) NeedReloadRegion(ctx *RPCContext) (need bool) {
	if s.failStoreIDs == nil {
		s.failStoreIDs = make(map[uint64]struct{})
	}
	if s.failProxyStoreIDs == nil {
		s.failProxyStoreIDs = make(map[uint64]struct{})
	}
	s.failStoreIDs[ctx.Store.storeID] = struct{}{}
	if ctx.ProxyStore != nil {
		s.failProxyStoreIDs[ctx.ProxyStore.storeID] = struct{}{}
	}

	if ctx.AccessMode == tiKVOnly && len(s.failStoreIDs)+len(s.failProxyStoreIDs) >= ctx.TiKVNum {
		need = true
	} else if ctx.AccessMode == tiFlashOnly && len(s.failStoreIDs) >= len(ctx.Meta.Peers)-ctx.TiKVNum {
		need = true
	} else if len(s.failStoreIDs)+len(s.failProxyStoreIDs) >= len(ctx.Meta.Peers) {
		need = true
	}

	if need {
		s.failStoreIDs = nil
		s.failProxyStoreIDs = nil
	}
	return
}

func regionErrorToLabel(e *errorpb.Error) string {
	if e.GetNotLeader() != nil {
		return "not_leader"
	} else if e.GetRegionNotFound() != nil {
		return "region_not_found"
	} else if e.GetKeyNotInRegion() != nil {
		return "key_not_in_region"
	} else if e.GetEpochNotMatch() != nil {
		return "epoch_not_match"
	} else if e.GetServerIsBusy() != nil {
		return "server_is_busy"
	} else if e.GetStaleCommand() != nil {
		return "stale_command"
	} else if e.GetStoreNotMatch() != nil {
		return "store_not_match"
	} else if e.GetRaftEntryTooLarge() != nil {
		return "raft_entry_too_large"
	} else if e.GetMaxTimestampNotSynced() != nil {
		return "max_timestamp_not_synced"
	} else if e.GetReadIndexNotReady() != nil {
		return "read_index_not_ready"
	} else if e.GetProposalInMergingMode() != nil {
		return "proposal_in_merging_mode"
	} else if e.GetDataIsNotReady() != nil {
		return "data_is_not_ready"
	} else if e.GetRegionNotInitialized() != nil {
		return "region_not_initialized"
	} else if e.GetDiskFull() != nil {
		return "disk_full"
	}
	return "unknown"
}

func (s *RegionRequestSender) onRegionError(bo *retry.Backoffer, ctx *RPCContext, req *tikvrpc.Request, regionErr *errorpb.Error) (shouldRetry bool, err error) {
	if span := opentracing.SpanFromContext(bo.GetCtx()); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tikv.onRegionError", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.SetCtx(opentracing.ContextWithSpan(bo.GetCtx(), span1))
	}

	// NOTE: Please add the region error handler in the same order of errorpb.Error.
	metrics.TiKVRegionErrorCounter.WithLabelValues(regionErrorToLabel(regionErr)).Inc()

	if notLeader := regionErr.GetNotLeader(); notLeader != nil {
		// Retry if error is `NotLeader`.
		logutil.BgLogger().Debug("tikv reports `NotLeader` retry later",
			zap.String("notLeader", notLeader.String()),
			zap.String("ctx", ctx.String()))

		if s.replicaSelector != nil {
			return s.replicaSelector.onNotLeader(bo, ctx, notLeader)
		} else if notLeader.GetLeader() == nil {
			// The peer doesn't know who is the current leader. Generally it's because
			// the Raft group is in an election, but it's possible that the peer is
			// isolated and removed from the Raft group. So it's necessary to reload
			// the region from PD.
			s.regionCache.InvalidateCachedRegionWithReason(ctx.Region, NoLeader)
			if err = bo.Backoff(retry.BoRegionScheduling, errors.Errorf("not leader: %v, ctx: %v", notLeader, ctx)); err != nil {
				return false, err
			}
			return false, nil
		} else {
			// don't backoff if a new leader is returned.
			s.regionCache.UpdateLeader(ctx.Region, notLeader.GetLeader(), ctx.AccessIdx)
			return true, nil
		}
	}

	// Retry it when tikv disk full happens.
	if diskFull := regionErr.GetDiskFull(); diskFull != nil {
		if err = bo.Backoff(retry.BoTiKVDiskFull, errors.Errorf("tikv disk full: %v ctx: %v", diskFull.String(), ctx.String())); err != nil {
			retry.BoTiKVDiskFull.SetErrors(errors.Errorf("tikv disk full: %v", diskFull.String()))
			return false, nil
		}
		return true, nil
	}

	// This peer is removed from the region. Invalidate the region since it's too stale.
	if regionErr.GetRegionNotFound() != nil {
		s.regionCache.InvalidateCachedRegion(ctx.Region)
		return false, nil
	}

	if regionErr.GetKeyNotInRegion() != nil {
		logutil.BgLogger().Debug("tikv reports `KeyNotInRegion`", zap.Stringer("ctx", ctx))
		s.regionCache.InvalidateCachedRegion(ctx.Region)
		return false, nil
	}

	if epochNotMatch := regionErr.GetEpochNotMatch(); epochNotMatch != nil {
		logutil.BgLogger().Debug("tikv reports `EpochNotMatch` retry later",
			zap.Stringer("EpochNotMatch", epochNotMatch),
			zap.Stringer("ctx", ctx))
		retry, err := s.regionCache.OnRegionEpochNotMatch(bo, ctx, epochNotMatch.CurrentRegions)
		if !retry && s.replicaSelector != nil {
			s.replicaSelector.invalidateRegion()
		}
		return retry, err
	}

	if regionErr.GetServerIsBusy() != nil {
		logutil.BgLogger().Warn("tikv reports `ServerIsBusy` retry later",
			zap.String("reason", regionErr.GetServerIsBusy().GetReason()),
			zap.Stringer("ctx", ctx))
		if ctx != nil && ctx.Store != nil && ctx.Store.storeType == tikvrpc.TiFlash {
			err = bo.Backoff(retry.BoTiFlashServerBusy, errors.Errorf("server is busy, ctx: %v", ctx))
		} else {
			err = bo.Backoff(retry.BoTiKVServerBusy, errors.Errorf("server is busy, ctx: %v", ctx))
		}
		if err != nil {
			return false, err
		}
		return true, nil
	}

	// StaleCommand error indicates the request is sent to the old leader and its term is changed.
	// We can't know whether the request is committed or not, so it's an undetermined error too,
	// but we don't handle it now.
	if regionErr.GetStaleCommand() != nil {
		logutil.BgLogger().Debug("tikv reports `StaleCommand`", zap.Stringer("ctx", ctx))
		if s.replicaSelector != nil {
			// Needn't backoff because the new leader should be elected soon
			// and the replicaSelector will try the next peer.
		} else {
			err = bo.Backoff(retry.BoStaleCmd, errors.Errorf("stale command, ctx: %v", ctx))
			if err != nil {
				return false, err
			}
		}
		return true, nil
	}

	if storeNotMatch := regionErr.GetStoreNotMatch(); storeNotMatch != nil {
		// store not match
		logutil.BgLogger().Debug("tikv reports `StoreNotMatch` retry later",
			zap.Stringer("storeNotMatch", storeNotMatch),
			zap.Stringer("ctx", ctx))
		ctx.Store.markNeedCheck(s.regionCache.notifyCheckCh)
		s.regionCache.InvalidateCachedRegion(ctx.Region)
		return false, nil
	}

	if regionErr.GetRaftEntryTooLarge() != nil {
		logutil.BgLogger().Warn("tikv reports `RaftEntryTooLarge`", zap.Stringer("ctx", ctx))
		return false, errors.New(regionErr.String())
	}

	if regionErr.GetMaxTimestampNotSynced() != nil {
		logutil.BgLogger().Debug("tikv reports `MaxTimestampNotSynced`", zap.Stringer("ctx", ctx))
		err = bo.Backoff(retry.BoMaxTsNotSynced, errors.Errorf("max timestamp not synced, ctx: %v", ctx))
		if err != nil {
			return false, err
		}
		return true, nil
	}

	// A read request may be sent to a peer which has not been initialized yet, we should retry in this case.
	if regionErr.GetRegionNotInitialized() != nil {
		logutil.BgLogger().Debug("tikv reports `RegionNotInitialized` retry later",
			zap.Uint64("store-id", ctx.Store.storeID),
			zap.Uint64("region-id", regionErr.GetRegionNotInitialized().GetRegionId()),
			zap.Stringer("ctx", ctx))
		err = bo.Backoff(retry.BoMaxRegionNotInitialized, errors.Errorf("region not initialized"))
		if err != nil {
			return false, err
		}
		return true, nil
	}

	// The read-index can't be handled timely because the region is splitting or merging.
	if regionErr.GetReadIndexNotReady() != nil {
		logutil.BgLogger().Debug("tikv reports `ReadIndexNotReady` retry later",
			zap.Uint64("store-id", ctx.Store.storeID),
			zap.Uint64("region-id", regionErr.GetRegionNotInitialized().GetRegionId()),
			zap.Stringer("ctx", ctx))
		// The region can't provide service until split or merge finished, so backoff.
		err = bo.Backoff(retry.BoRegionScheduling, errors.Errorf("read index not ready, ctx: %v", ctx))
		if err != nil {
			return false, err
		}
		return true, nil
	}

	if regionErr.GetProposalInMergingMode() != nil {
		logutil.BgLogger().Debug("tikv reports `ProposalInMergingMode`", zap.Stringer("ctx", ctx))
		// The region is merging and it can't provide service until merge finished, so backoff.
		err = bo.Backoff(retry.BoRegionScheduling, errors.Errorf("region is merging, ctx: %v", ctx))
		if err != nil {
			return false, err
		}
		return true, nil
	}

	// A stale read request may be sent to a peer which the data is not ready yet, we should retry in this case.
	// This error is specific to stale read and the target replica is randomly selected. If the request is sent
	// to the leader, the data must be ready, so we don't backoff here.
	if regionErr.GetDataIsNotReady() != nil {
		logutil.BgLogger().Warn("tikv reports `DataIsNotReady` retry later",
			zap.Uint64("store-id", ctx.Store.storeID),
			zap.Uint64("peer-id", regionErr.GetDataIsNotReady().GetPeerId()),
			zap.Uint64("region-id", regionErr.GetDataIsNotReady().GetRegionId()),
			zap.Uint64("safe-ts", regionErr.GetDataIsNotReady().GetSafeTs()),
			zap.Stringer("ctx", ctx))
		err = bo.Backoff(retry.BoMaxDataNotReady, errors.New("data is not ready"))
		if err != nil {
			return false, err
		}
		return true, nil
	}

	logutil.BgLogger().Debug("tikv reports region failed",
		zap.Stringer("regionErr", regionErr),
		zap.Stringer("ctx", ctx))

	if s.replicaSelector != nil {
		// Try the next replica.
		return true, nil
	}

	// When the request is sent to TiDB, there is no region in the request, so the region id will be 0.
	// So when region id is 0, there is no business with region cache.
	if ctx.Region.id != 0 {
		s.regionCache.InvalidateCachedRegion(ctx.Region)
	}
	// For other errors, we only drop cache here.
	// Because caller may need to re-split the request.
	return false, nil
}
