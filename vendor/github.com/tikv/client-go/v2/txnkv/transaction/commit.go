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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/commit.go
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
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/prometheus/client_golang/prometheus"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
)

type actionCommit struct{ retry bool }

var _ twoPhaseCommitAction = actionCommit{}

func (actionCommit) String() string {
	return "commit"
}

func (actionCommit) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return metrics.TxnRegionsNumHistogramCommit
}

func (actionCommit) handleSingleBatch(c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) error {
	keys := batch.mutations.GetKeys()
	req := tikvrpc.NewRequest(tikvrpc.CmdCommit, &kvrpcpb.CommitRequest{
		StartVersion:  c.startTS,
		Keys:          keys,
		CommitVersion: c.commitTS,
	}, kvrpcpb.Context{Priority: c.priority, SyncLog: c.syncLog,
		ResourceGroupTag: c.resourceGroupTag, DiskFullOpt: c.diskFullOpt,
		MaxExecutionDurationMs: uint64(client.MaxWriteExecutionTime.Milliseconds())})

	tBegin := time.Now()
	attempts := 0

	sender := locate.NewRegionRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient())
	for {
		attempts++
		if time.Since(tBegin) > slowRequestThreshold {
			logutil.BgLogger().Warn("slow commit request", zap.Uint64("startTS", c.startTS), zap.Stringer("region", &batch.region), zap.Int("attempts", attempts))
			tBegin = time.Now()
		}

		resp, err := sender.SendReq(bo, req, batch.region, client.ReadTimeoutShort)
		// If we fail to receive response for the request that commits primary key, it will be undetermined whether this
		// transaction has been successfully committed.
		// Under this circumstance, we can not declare the commit is complete (may lead to data lost), nor can we throw
		// an error (may lead to the duplicated key error when upper level restarts the transaction). Currently the best
		// solution is to populate this error and let upper layer drop the connection to the corresponding mysql client.
		if batch.isPrimary && sender.GetRPCError() != nil && !c.isAsyncCommit() {
			c.setUndeterminedErr(errors.WithStack(sender.GetRPCError()))
		}

		// Unexpected error occurs, return it.
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
			return c.doActionOnMutations(bo, actionCommit{true}, batch.mutations)
		}

		if resp.Resp == nil {
			return errors.WithStack(tikverr.ErrBodyMissing)
		}
		commitResp := resp.Resp.(*kvrpcpb.CommitResponse)
		// Here we can make sure tikv has processed the commit primary key request. So
		// we can clean undetermined error.
		if batch.isPrimary && !c.isAsyncCommit() {
			c.setUndeterminedErr(nil)
		}
		if keyErr := commitResp.GetError(); keyErr != nil {
			if rejected := keyErr.GetCommitTsExpired(); rejected != nil {
				logutil.Logger(bo.GetCtx()).Info("2PC commitTS rejected by TiKV, retry with a newer commitTS",
					zap.Uint64("txnStartTS", c.startTS),
					zap.Stringer("info", logutil.Hex(rejected)))

				// Do not retry for a txn which has a too large MinCommitTs
				// 3600000 << 18 = 943718400000
				if rejected.MinCommitTs-rejected.AttemptedCommitTs > 943718400000 {
					return errors.Errorf("2PC MinCommitTS is too large, we got MinCommitTS: %d, and AttemptedCommitTS: %d",
						rejected.MinCommitTs, rejected.AttemptedCommitTs)
				}

				// Update commit ts and retry.
				commitTS, err := c.store.GetTimestampWithRetry(bo, c.txn.GetScope())
				if err != nil {
					logutil.Logger(bo.GetCtx()).Warn("2PC get commitTS failed",
						zap.Error(err),
						zap.Uint64("txnStartTS", c.startTS))
					return err
				}

				c.mu.Lock()
				c.commitTS = commitTS
				c.mu.Unlock()
				// Update the commitTS of the request and retry.
				req.Commit().CommitVersion = commitTS
				continue
			}

			c.mu.RLock()
			defer c.mu.RUnlock()
			err = tikverr.ExtractKeyErr(keyErr)
			if c.mu.committed {
				// No secondary key could be rolled back after it's primary key is committed.
				// There must be a serious bug somewhere.
				hexBatchKeys := func(keys [][]byte) []string {
					var res []string
					for _, k := range keys {
						res = append(res, hex.EncodeToString(k))
					}
					return res
				}
				logutil.Logger(bo.GetCtx()).Error("2PC failed commit key after primary key committed",
					zap.Error(err),
					zap.Uint64("txnStartTS", c.startTS),
					zap.Uint64("commitTS", c.commitTS),
					zap.Strings("keys", hexBatchKeys(keys)))
				return err
			}
			// The transaction maybe rolled back by concurrent transactions.
			logutil.Logger(bo.GetCtx()).Debug("2PC failed commit primary key",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS))
			return err
		}
		break
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Group that contains primary key is always the first.
	// We mark transaction's status committed when we receive the first success response.
	c.mu.committed = true
	return nil
}

func (c *twoPhaseCommitter) commitMutations(bo *retry.Backoffer, mutations CommitterMutations) error {
	if span := opentracing.SpanFromContext(bo.GetCtx()); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("twoPhaseCommitter.commitMutations", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.SetCtx(opentracing.ContextWithSpan(bo.GetCtx(), span1))
	}

	return c.doActionOnMutations(bo, actionCommit{}, mutations)
}
