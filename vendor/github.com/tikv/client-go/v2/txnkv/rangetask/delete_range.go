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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/delete_range.go
//

// Copyright 2018 PingCAP, Inc.
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

package rangetask

import (
	"bytes"
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type storage interface {
	// GetRegionCache gets the RegionCache.
	GetRegionCache() *locate.RegionCache
	// SendReq sends a request to TiKV.
	SendReq(bo *retry.Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration) (*tikvrpc.Response, error)
}

// DeleteRangeTask is used to delete all keys in a range. After
// performing DeleteRange, it keeps how many ranges it affects and
// if the task was canceled or not.
type DeleteRangeTask struct {
	completedRegions int
	store            storage
	startKey         []byte
	endKey           []byte
	notifyOnly       bool
	concurrency      int
}

// NewDeleteRangeTask creates a DeleteRangeTask. Deleting will be performed when `Execute` method is invoked.
// Be careful while using this API. This API doesn't keep recent MVCC versions, but will delete all versions of all keys
// in the range immediately. Also notice that frequent invocation to this API may cause performance problems to TiKV.
func NewDeleteRangeTask(store storage, startKey []byte, endKey []byte, concurrency int) *DeleteRangeTask {
	return &DeleteRangeTask{
		completedRegions: 0,
		store:            store,
		startKey:         startKey,
		endKey:           endKey,
		notifyOnly:       false,
		concurrency:      concurrency,
	}
}

// NewNotifyDeleteRangeTask creates a task that sends delete range requests to all regions in the range, but with the
// flag `notifyOnly` set. TiKV will not actually delete the range after receiving request, but it will be replicated via
// raft. This is used to notify the involved regions before sending UnsafeDestroyRange requests.
func NewNotifyDeleteRangeTask(store storage, startKey []byte, endKey []byte, concurrency int) *DeleteRangeTask {
	task := NewDeleteRangeTask(store, startKey, endKey, concurrency)
	task.notifyOnly = true
	return task
}

// getRunnerName returns a name for RangeTaskRunner.
func (t *DeleteRangeTask) getRunnerName() string {
	if t.notifyOnly {
		return "delete-range-notify"
	}
	return "delete-range"
}

// Execute performs the delete range operation.
func (t *DeleteRangeTask) Execute(ctx context.Context) error {
	runnerName := t.getRunnerName()

	runner := NewRangeTaskRunner(runnerName, t.store, t.concurrency, t.sendReqOnRange)
	err := runner.RunOnRange(ctx, t.startKey, t.endKey)
	t.completedRegions = runner.CompletedRegions()

	return err
}

const deleteRangeOneRegionMaxBackoff = 100000

// Execute performs the delete range operation.
func (t *DeleteRangeTask) sendReqOnRange(ctx context.Context, r kv.KeyRange) (TaskStat, error) {
	startKey, rangeEndKey := r.StartKey, r.EndKey
	var stat TaskStat
	for {
		select {
		case <-ctx.Done():
			return stat, errors.WithStack(ctx.Err())
		default:
		}

		if bytes.Compare(startKey, rangeEndKey) >= 0 {
			break
		}

		bo := retry.NewBackofferWithVars(ctx, deleteRangeOneRegionMaxBackoff, nil)
		loc, err := t.store.GetRegionCache().LocateKey(bo, startKey)
		if err != nil {
			return stat, err
		}

		// Delete to the end of the region, except if it's the last region overlapping the range
		endKey := loc.EndKey
		// If it is the last region
		if loc.Contains(rangeEndKey) {
			endKey = rangeEndKey
		}

		req := tikvrpc.NewRequest(tikvrpc.CmdDeleteRange, &kvrpcpb.DeleteRangeRequest{
			StartKey:   startKey,
			EndKey:     endKey,
			NotifyOnly: t.notifyOnly,
		})

		resp, err := t.store.SendReq(bo, req, loc.Region, client.ReadTimeoutMedium)
		if err != nil {
			return stat, err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return stat, err
		}
		if regionErr != nil {
			err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return stat, err
			}
			continue
		}
		if resp.Resp == nil {
			return stat, errors.WithStack(tikverr.ErrBodyMissing)
		}
		deleteRangeResp := resp.Resp.(*kvrpcpb.DeleteRangeResponse)
		if err := deleteRangeResp.GetError(); err != "" {
			return stat, errors.Errorf("unexpected delete range err: %v", err)
		}
		stat.CompletedRegions++
		startKey = endKey
	}

	return stat, nil
}

// CompletedRegions returns the number of regions that are affected by this delete range task
func (t *DeleteRangeTask) CompletedRegions() int {
	return t.completedRegions
}
