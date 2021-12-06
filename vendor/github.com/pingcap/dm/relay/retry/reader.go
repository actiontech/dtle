// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package retry

import (
	"context"
	"time"

	"github.com/pingcap/failpoint"

	"github.com/pingcap/dm/pkg/backoff"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/terror"
)

// ReaderRetryConfig is the configuration used for binlog reader retry backoff.
// we always enable this now.
type ReaderRetryConfig struct {
	BackoffRollback time.Duration `toml:"backoff-rollback" json:"backoff-rollback"`
	BackoffMax      time.Duration `toml:"backoff-max" json:"backoff-max"`
	// unexposed config
	BackoffMin    time.Duration `json:"-"`
	BackoffJitter bool          `json:"-"`
	BackoffFactor float64       `json:"-"`
}

// ReaderRetry is used to control the retry for the ReaderRetry.
// It is not thread-safe.
type ReaderRetry struct {
	cfg           ReaderRetryConfig
	bf            *backoff.Backoff
	lastRetryTime time.Time
}

// NewReaderRetry creates a new ReaderRetry instance.
func NewReaderRetry(cfg ReaderRetryConfig) (*ReaderRetry, error) {
	bf, err := backoff.NewBackoff(cfg.BackoffFactor, cfg.BackoffJitter, cfg.BackoffMin, cfg.BackoffMax)
	if err != nil {
		return nil, terror.WithClass(err, terror.ClassRelayUnit)
	}
	return &ReaderRetry{
		cfg:           cfg,
		bf:            bf,
		lastRetryTime: time.Now(),
	}, nil
}

// Check checks whether should retry for the error.
func (rr *ReaderRetry) Check(ctx context.Context, err error) bool {
	failpoint.Inject("RelayAllowRetry", func() {
		failpoint.Return(true)
	})
	if !retry.IsConnectionError(err) {
		return false
	}

	for lrt := rr.lastRetryTime; time.Since(lrt) > rr.cfg.BackoffRollback; lrt = lrt.Add(rr.cfg.BackoffRollback) {
		rr.bf.Rollback()
	}

	duration := rr.bf.Current()
	select {
	case <-ctx.Done():
		return false
	case <-time.After(duration):
	}

	rr.lastRetryTime = time.Now()
	rr.bf.BoundaryForward()
	return true
}
