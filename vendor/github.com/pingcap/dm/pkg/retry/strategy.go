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
	"time"

	tcontext "github.com/pingcap/dm/pkg/context"
)

// backoffStrategy represents enum of retry wait interval.
type backoffStrategy uint8

const (
	// Stable represents fixed time wait retry policy, every retry should wait a fixed time.
	Stable backoffStrategy = iota + 1
	// LinearIncrease represents increase time wait retry policy, every retry should wait more time depends on increasing retry times.
	LinearIncrease
)

// Params define parameters for Apply
// it's a parameters union set of all implements which implement Apply.
type Params struct {
	RetryCount         int
	FirstRetryDuration time.Duration

	BackoffStrategy backoffStrategy

	// IsRetryableFn tells whether we should retry when operateFn failed
	// params: (number of retry, error of operation)
	// return: (bool)
	//   1. true: means operateFn can be retried
	//   2. false: means operateFn cannot retry after receive this error
	IsRetryableFn func(int, error) bool
}

// Strategy define different kind of retry strategy.
type Strategy interface {

	// Apply define retry strategy
	// params: (retry parameters for this strategy, a normal operation)
	// return: (result of operation, number of retry, error of operation)
	Apply(ctx *tcontext.Context,
		params Params,
		// operateFn:
		//   params: (context)
		//   return: (result of operation, error of operation)
		operateFn func(*tcontext.Context) (interface{}, error),
	) (interface{}, int, error)
}

// FiniteRetryStrategy will retry `RetryCount` times when failed to operate DB.
type FiniteRetryStrategy struct{}

// Apply for FiniteRetryStrategy, it wait `FirstRetryDuration` before it starts first retry, and then rest of retries wait time depends on BackoffStrategy.
func (*FiniteRetryStrategy) Apply(ctx *tcontext.Context, params Params,
	operateFn func(*tcontext.Context) (interface{}, error)) (ret interface{}, i int, err error) {
	for ; i < params.RetryCount; i++ {
		ret, err = operateFn(ctx)
		if err != nil {
			if params.IsRetryableFn(i, err) {
				duration := params.FirstRetryDuration

				switch params.BackoffStrategy {
				case LinearIncrease:
					duration = time.Duration(i+1) * params.FirstRetryDuration
				default:
				}

				select {
				case <-ctx.Context().Done():
					return ret, i, err // return `ret` rather than `nil`
				case <-time.After(duration):
				}
				continue
			}
		}
		break
	}
	return ret, i, err
}
