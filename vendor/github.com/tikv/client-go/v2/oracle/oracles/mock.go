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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/oracle/oracles/mock.go
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

package oracles

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/tikv/client-go/v2/oracle"
)

var errStopped = errors.New("stopped")

// MockOracle is a mock oracle for test.
type MockOracle struct {
	sync.RWMutex
	stop   bool
	offset time.Duration
	lastTS uint64
}

// Enable enables the Oracle
func (o *MockOracle) Enable() {
	o.Lock()
	defer o.Unlock()
	o.stop = false
}

// Disable disables the Oracle
func (o *MockOracle) Disable() {
	o.Lock()
	defer o.Unlock()
	o.stop = true
}

// AddOffset adds the offset of the oracle.
func (o *MockOracle) AddOffset(d time.Duration) {
	o.Lock()
	defer o.Unlock()

	o.offset += d
}

// GetTimestamp implements oracle.Oracle interface.
func (o *MockOracle) GetTimestamp(ctx context.Context, _ *oracle.Option) (uint64, error) {
	o.Lock()
	defer o.Unlock()

	if o.stop {
		return 0, errors.WithStack(errStopped)
	}
	ts := oracle.GoTimeToTS(time.Now().Add(o.offset))
	if oracle.ExtractPhysical(o.lastTS) == oracle.ExtractPhysical(ts) {
		ts = o.lastTS + 1
	}
	o.lastTS = ts
	return ts, nil
}

// GetStaleTimestamp implements oracle.Oracle interface.
func (o *MockOracle) GetStaleTimestamp(ctx context.Context, txnScope string, prevSecond uint64) (ts uint64, err error) {
	return oracle.GoTimeToTS(time.Now().Add(-time.Second * time.Duration(prevSecond))), nil
}

type mockOracleFuture struct {
	o   *MockOracle
	ctx context.Context
}

func (m *mockOracleFuture) Wait() (uint64, error) {
	return m.o.GetTimestamp(m.ctx, &oracle.Option{})
}

// GetTimestampAsync implements oracle.Oracle interface.
func (o *MockOracle) GetTimestampAsync(ctx context.Context, _ *oracle.Option) oracle.Future {
	return &mockOracleFuture{o, ctx}
}

// GetLowResolutionTimestamp implements oracle.Oracle interface.
func (o *MockOracle) GetLowResolutionTimestamp(ctx context.Context, opt *oracle.Option) (uint64, error) {
	return o.GetTimestamp(ctx, opt)
}

// GetLowResolutionTimestampAsync implements oracle.Oracle interface.
func (o *MockOracle) GetLowResolutionTimestampAsync(ctx context.Context, opt *oracle.Option) oracle.Future {
	return o.GetTimestampAsync(ctx, opt)
}

// IsExpired implements oracle.Oracle interface.
func (o *MockOracle) IsExpired(lockTimestamp, TTL uint64, _ *oracle.Option) bool {
	o.RLock()
	defer o.RUnlock()
	expire := oracle.GetTimeFromTS(lockTimestamp).Add(time.Duration(TTL) * time.Millisecond)
	return !time.Now().Add(o.offset).Before(expire)
}

// UntilExpired implement oracle.Oracle interface.
func (o *MockOracle) UntilExpired(lockTimeStamp, TTL uint64, _ *oracle.Option) int64 {
	o.RLock()
	defer o.RUnlock()
	expire := oracle.GetTimeFromTS(lockTimeStamp).Add(time.Duration(TTL) * time.Millisecond)
	return expire.Sub(time.Now().Add(o.offset)).Milliseconds()
}

// Close implements oracle.Oracle interface.
func (o *MockOracle) Close() {

}
