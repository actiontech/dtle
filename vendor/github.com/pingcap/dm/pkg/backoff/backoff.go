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

package backoff

import (
	"math"
	"math/rand"
	"time"

	"github.com/pingcap/dm/pkg/terror"
)

// Backoff is an exponential counter, it starts from `Min` duration, and after
// every call to `Duration` method the duration will be multiplied by `Factor`,
// but it never exceeds `Max`. Backoff is not thread-safe.
type Backoff struct {
	// cwnd is the congestion window
	cwnd int

	// Factor is the multiplying factor for each increment step
	Factor float64

	// Jitter eases contention by randomizing backoff steps
	Jitter bool

	// Min and Max are the minimum and maximum values of the counter
	Min, Max time.Duration
}

// NewBackoff creates a new backoff instance.
func NewBackoff(factor float64, jitter bool, min, max time.Duration) (*Backoff, error) {
	if factor <= 0 {
		return nil, terror.ErrBackoffArgsNotValid.Generate("factor", factor)
	}
	if min < 0 {
		return nil, terror.ErrBackoffArgsNotValid.Generate("min", min)
	}
	if max < 0 || max < min {
		return nil, terror.ErrBackoffArgsNotValid.Generate("max", max)
	}
	return &Backoff{
		Factor: factor,
		Jitter: jitter,
		Min:    min,
		Max:    max,
	}, nil
}

// Duration returns the duration for the current cwnd and increases the cwnd counter.
func (b *Backoff) Duration() time.Duration {
	d := b.Current()
	b.Forward()
	return d
}

// Current returns the duration for the current cwnd, but doesn't increase the
// cwnd counter.
func (b *Backoff) Current() time.Duration {
	return b.durationcwnd(b.cwnd)
}

// BoundaryForward checks whether `Current` reaches `Max` duration, if not then
// increases the cwnd counter, else does nothing.
func (b *Backoff) BoundaryForward() {
	if b.Current() < b.Max {
		b.cwnd++
	}
}

// Forward increases the cwnd counter.
func (b *Backoff) Forward() {
	b.cwnd++
}

// Rollback try to decrease cwnd by one if it is greater or equal than one.
// This is used for we have a long enough duration without backoff try.
func (b *Backoff) Rollback() {
	if b.cwnd > 0 {
		b.cwnd--
	}
}

// durationcwnd returns the duration for a specific cwnd. The first
// cwnd should be 0.
func (b *Backoff) durationcwnd(cwnd int) time.Duration {
	minf := float64(b.Min)
	durf := minf * math.Pow(b.Factor, float64(cwnd))
	// Full jitter and Decorr jitter is more aggressive and more suitable for fast recovery
	// We use Equal jitter here to achieve more stability
	// refer to: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
	if b.Jitter {
		if durf <= minf {
			return b.Min
		}
		durf = durf/2 + rand.Float64()*(durf/2)
	}
	// why minus 512 here, because i = 512 is the minimal value that ensures
	// float64(math.MaxInt64) > float64(math.MaxInt64-i)) is true
	if durf > float64(math.MaxInt64-512) {
		return b.Max
	}
	dur := time.Duration(durf)
	if dur > b.Max {
		return b.Max
	}
	return dur
}

// Reset restarts the current cwnd counter to zero.
func (b *Backoff) Reset() {
	b.cwnd = 0
}
