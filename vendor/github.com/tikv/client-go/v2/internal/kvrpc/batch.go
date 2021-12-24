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

package kvrpc

import (
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// Batch is part of the mutation set that will be sent to tikv in a request.
type Batch struct {
	RegionID locate.RegionVerID
	Keys     [][]byte
	Values   [][]byte
	TTLs     []uint64
}

// BatchResult wraps a Batch request's server response or an error.
type BatchResult struct {
	*tikvrpc.Response
	Error error
}

// AppendBatches divides the mutation to be requested into Batches so that the size of each batch is
// approximately the same as the given limit.
func AppendBatches(batches []Batch, regionID locate.RegionVerID, groupKeys [][]byte, keyToValue map[string][]byte, keyToTTL map[string]uint64, limit int) []Batch {
	var start, size int
	var keys, values [][]byte
	var ttls []uint64
	for start = 0; start < len(groupKeys); start++ {
		if size >= limit {
			batches = append(batches, Batch{RegionID: regionID, Keys: keys, Values: values, TTLs: ttls})
			keys = make([][]byte, 0)
			values = make([][]byte, 0)
			ttls = make([]uint64, 0)
			size = 0
		}
		key := groupKeys[start]
		value := keyToValue[string(key)]
		ttl := keyToTTL[string(key)]
		keys = append(keys, key)
		values = append(values, value)
		ttls = append(ttls, ttl)
		size += len(key)
		size += len(value)
	}
	if len(keys) != 0 {
		batches = append(batches, Batch{RegionID: regionID, Keys: keys, Values: values, TTLs: ttls})
	}
	return batches
}

// AppendKeyBatches divides the mutation to be requested into Batches, ensuring that the count of keys of each
// Batch is not greater than the given limit.
func AppendKeyBatches(batches []Batch, regionID locate.RegionVerID, groupKeys [][]byte, limit int) []Batch {
	var keys [][]byte
	for start, count := 0, 0; start < len(groupKeys); start++ {
		if count > limit {
			batches = append(batches, Batch{RegionID: regionID, Keys: keys})
			keys = make([][]byte, 0, limit)
			count = 0
		}
		keys = append(keys, groupKeys[start])
		count++
	}
	if len(keys) != 0 {
		batches = append(batches, Batch{RegionID: regionID, Keys: keys})
	}
	return batches
}
