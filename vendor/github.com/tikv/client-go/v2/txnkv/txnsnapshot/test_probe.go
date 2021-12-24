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

package txnsnapshot

import (
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// SnapshotProbe exposes some snapshot utilities for testing purpose.
type SnapshotProbe struct {
	*KVSnapshot
}

// MergeRegionRequestStats merges RPC runtime stats into snapshot's stats.
func (s SnapshotProbe) MergeRegionRequestStats(stats map[tikvrpc.CmdType]*locate.RPCRuntimeStats) {
	s.mergeRegionRequestStats(stats)
}

// RecordBackoffInfo records backoff stats into snapshot's stats.
func (s SnapshotProbe) RecordBackoffInfo(bo *retry.Backoffer) {
	s.recordBackoffInfo(bo)
}

// MergeExecDetail merges exec stats into snapshot's stats.
func (s SnapshotProbe) MergeExecDetail(detail *kvrpcpb.ExecDetailsV2) {
	s.mergeExecDetail(detail)
}

// FormatStats dumps information of stats.
func (s SnapshotProbe) FormatStats() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.stats.String()
}

// BatchGetSingleRegion gets a batch of keys from a region.
func (s SnapshotProbe) BatchGetSingleRegion(bo *retry.Backoffer, region locate.RegionVerID, keys [][]byte, collectF func(k, v []byte)) error {
	return s.batchGetSingleRegion(bo, batchKeys{region: region, keys: keys}, collectF)
}

// NewScanner returns a scanner to iterate given key range.
func (s SnapshotProbe) NewScanner(start, end []byte, batchSize int, reverse bool) (*Scanner, error) {
	return newScanner(s.KVSnapshot, start, end, batchSize, reverse)
}

// ConfigProbe exposes configurations and global variables for testing purpose.
type ConfigProbe struct{}

// GetScanBatchSize returns the batch size to scan ranges.
func (c ConfigProbe) GetScanBatchSize() int {
	return defaultScanBatchSize
}

// GetGetMaxBackoff returns the max sleep for get command.
func (c ConfigProbe) GetGetMaxBackoff() int {
	return getMaxBackoff
}
