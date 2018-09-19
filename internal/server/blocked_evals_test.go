/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package server

import (
	"reflect"
	"sync"
	"testing"
	"time"
	"github.com/actiontech/udup/internal/models"
)

func TestNewBlockedEvals(t *testing.T) {
	type args struct {
		evalBroker *EvalBroker
	}
	tests := []struct {
		name string
		args args
		want *BlockedEvals
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewBlockedEvals(tt.args.evalBroker); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBlockedEvals() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBlockedEvals_Enabled(t *testing.T) {
	type fields struct {
		evalBroker       *EvalBroker
		enabled          bool
		stats            *BlockedStats
		l                sync.RWMutex
		captured         map[string]wrappedEval
		escaped          map[string]wrappedEval
		capacityChangeCh chan *capacityUpdate
		jobs             map[string]string
		unblockIndexes   map[string]uint64
		duplicates       []*models.Evaluation
		duplicateCh      chan struct{}
		stopCh           chan struct{}
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BlockedEvals{
				evalBroker:       tt.fields.evalBroker,
				enabled:          tt.fields.enabled,
				stats:            tt.fields.stats,
				l:                tt.fields.l,
				captured:         tt.fields.captured,
				escaped:          tt.fields.escaped,
				capacityChangeCh: tt.fields.capacityChangeCh,
				jobs:             tt.fields.jobs,
				unblockIndexes:   tt.fields.unblockIndexes,
				duplicates:       tt.fields.duplicates,
				duplicateCh:      tt.fields.duplicateCh,
				stopCh:           tt.fields.stopCh,
			}
			if got := b.Enabled(); got != tt.want {
				t.Errorf("BlockedEvals.Enabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBlockedEvals_SetEnabled(t *testing.T) {
	type fields struct {
		evalBroker       *EvalBroker
		enabled          bool
		stats            *BlockedStats
		l                sync.RWMutex
		captured         map[string]wrappedEval
		escaped          map[string]wrappedEval
		capacityChangeCh chan *capacityUpdate
		jobs             map[string]string
		unblockIndexes   map[string]uint64
		duplicates       []*models.Evaluation
		duplicateCh      chan struct{}
		stopCh           chan struct{}
	}
	type args struct {
		enabled bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BlockedEvals{
				evalBroker:       tt.fields.evalBroker,
				enabled:          tt.fields.enabled,
				stats:            tt.fields.stats,
				l:                tt.fields.l,
				captured:         tt.fields.captured,
				escaped:          tt.fields.escaped,
				capacityChangeCh: tt.fields.capacityChangeCh,
				jobs:             tt.fields.jobs,
				unblockIndexes:   tt.fields.unblockIndexes,
				duplicates:       tt.fields.duplicates,
				duplicateCh:      tt.fields.duplicateCh,
				stopCh:           tt.fields.stopCh,
			}
			b.SetEnabled(tt.args.enabled)
		})
	}
}

func TestBlockedEvals_Block(t *testing.T) {
	type fields struct {
		evalBroker       *EvalBroker
		enabled          bool
		stats            *BlockedStats
		l                sync.RWMutex
		captured         map[string]wrappedEval
		escaped          map[string]wrappedEval
		capacityChangeCh chan *capacityUpdate
		jobs             map[string]string
		unblockIndexes   map[string]uint64
		duplicates       []*models.Evaluation
		duplicateCh      chan struct{}
		stopCh           chan struct{}
	}
	type args struct {
		eval *models.Evaluation
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BlockedEvals{
				evalBroker:       tt.fields.evalBroker,
				enabled:          tt.fields.enabled,
				stats:            tt.fields.stats,
				l:                tt.fields.l,
				captured:         tt.fields.captured,
				escaped:          tt.fields.escaped,
				capacityChangeCh: tt.fields.capacityChangeCh,
				jobs:             tt.fields.jobs,
				unblockIndexes:   tt.fields.unblockIndexes,
				duplicates:       tt.fields.duplicates,
				duplicateCh:      tt.fields.duplicateCh,
				stopCh:           tt.fields.stopCh,
			}
			b.Block(tt.args.eval)
		})
	}
}

func TestBlockedEvals_Reblock(t *testing.T) {
	type fields struct {
		evalBroker       *EvalBroker
		enabled          bool
		stats            *BlockedStats
		l                sync.RWMutex
		captured         map[string]wrappedEval
		escaped          map[string]wrappedEval
		capacityChangeCh chan *capacityUpdate
		jobs             map[string]string
		unblockIndexes   map[string]uint64
		duplicates       []*models.Evaluation
		duplicateCh      chan struct{}
		stopCh           chan struct{}
	}
	type args struct {
		eval  *models.Evaluation
		token string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BlockedEvals{
				evalBroker:       tt.fields.evalBroker,
				enabled:          tt.fields.enabled,
				stats:            tt.fields.stats,
				l:                tt.fields.l,
				captured:         tt.fields.captured,
				escaped:          tt.fields.escaped,
				capacityChangeCh: tt.fields.capacityChangeCh,
				jobs:             tt.fields.jobs,
				unblockIndexes:   tt.fields.unblockIndexes,
				duplicates:       tt.fields.duplicates,
				duplicateCh:      tt.fields.duplicateCh,
				stopCh:           tt.fields.stopCh,
			}
			b.Reblock(tt.args.eval, tt.args.token)
		})
	}
}

func TestBlockedEvals_processBlock(t *testing.T) {
	type fields struct {
		evalBroker       *EvalBroker
		enabled          bool
		stats            *BlockedStats
		l                sync.RWMutex
		captured         map[string]wrappedEval
		escaped          map[string]wrappedEval
		capacityChangeCh chan *capacityUpdate
		jobs             map[string]string
		unblockIndexes   map[string]uint64
		duplicates       []*models.Evaluation
		duplicateCh      chan struct{}
		stopCh           chan struct{}
	}
	type args struct {
		eval  *models.Evaluation
		token string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BlockedEvals{
				evalBroker:       tt.fields.evalBroker,
				enabled:          tt.fields.enabled,
				stats:            tt.fields.stats,
				l:                tt.fields.l,
				captured:         tt.fields.captured,
				escaped:          tt.fields.escaped,
				capacityChangeCh: tt.fields.capacityChangeCh,
				jobs:             tt.fields.jobs,
				unblockIndexes:   tt.fields.unblockIndexes,
				duplicates:       tt.fields.duplicates,
				duplicateCh:      tt.fields.duplicateCh,
				stopCh:           tt.fields.stopCh,
			}
			b.processBlock(tt.args.eval, tt.args.token)
		})
	}
}

func TestBlockedEvals_missedUnblock(t *testing.T) {
	type fields struct {
		evalBroker       *EvalBroker
		enabled          bool
		stats            *BlockedStats
		l                sync.RWMutex
		captured         map[string]wrappedEval
		escaped          map[string]wrappedEval
		capacityChangeCh chan *capacityUpdate
		jobs             map[string]string
		unblockIndexes   map[string]uint64
		duplicates       []*models.Evaluation
		duplicateCh      chan struct{}
		stopCh           chan struct{}
	}
	type args struct {
		eval *models.Evaluation
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BlockedEvals{
				evalBroker:       tt.fields.evalBroker,
				enabled:          tt.fields.enabled,
				stats:            tt.fields.stats,
				l:                tt.fields.l,
				captured:         tt.fields.captured,
				escaped:          tt.fields.escaped,
				capacityChangeCh: tt.fields.capacityChangeCh,
				jobs:             tt.fields.jobs,
				unblockIndexes:   tt.fields.unblockIndexes,
				duplicates:       tt.fields.duplicates,
				duplicateCh:      tt.fields.duplicateCh,
				stopCh:           tt.fields.stopCh,
			}
			if got := b.missedUnblock(tt.args.eval); got != tt.want {
				t.Errorf("BlockedEvals.missedUnblock() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBlockedEvals_Untrack(t *testing.T) {
	type fields struct {
		evalBroker       *EvalBroker
		enabled          bool
		stats            *BlockedStats
		l                sync.RWMutex
		captured         map[string]wrappedEval
		escaped          map[string]wrappedEval
		capacityChangeCh chan *capacityUpdate
		jobs             map[string]string
		unblockIndexes   map[string]uint64
		duplicates       []*models.Evaluation
		duplicateCh      chan struct{}
		stopCh           chan struct{}
	}
	type args struct {
		jobID string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BlockedEvals{
				evalBroker:       tt.fields.evalBroker,
				enabled:          tt.fields.enabled,
				stats:            tt.fields.stats,
				l:                tt.fields.l,
				captured:         tt.fields.captured,
				escaped:          tt.fields.escaped,
				capacityChangeCh: tt.fields.capacityChangeCh,
				jobs:             tt.fields.jobs,
				unblockIndexes:   tt.fields.unblockIndexes,
				duplicates:       tt.fields.duplicates,
				duplicateCh:      tt.fields.duplicateCh,
				stopCh:           tt.fields.stopCh,
			}
			b.Untrack(tt.args.jobID)
		})
	}
}

func TestBlockedEvals_Unblock(t *testing.T) {
	type fields struct {
		evalBroker       *EvalBroker
		enabled          bool
		stats            *BlockedStats
		l                sync.RWMutex
		captured         map[string]wrappedEval
		escaped          map[string]wrappedEval
		capacityChangeCh chan *capacityUpdate
		jobs             map[string]string
		unblockIndexes   map[string]uint64
		duplicates       []*models.Evaluation
		duplicateCh      chan struct{}
		stopCh           chan struct{}
	}
	type args struct {
		computedClass string
		index         uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BlockedEvals{
				evalBroker:       tt.fields.evalBroker,
				enabled:          tt.fields.enabled,
				stats:            tt.fields.stats,
				l:                tt.fields.l,
				captured:         tt.fields.captured,
				escaped:          tt.fields.escaped,
				capacityChangeCh: tt.fields.capacityChangeCh,
				jobs:             tt.fields.jobs,
				unblockIndexes:   tt.fields.unblockIndexes,
				duplicates:       tt.fields.duplicates,
				duplicateCh:      tt.fields.duplicateCh,
				stopCh:           tt.fields.stopCh,
			}
			b.Unblock(tt.args.computedClass, tt.args.index)
		})
	}
}

func TestBlockedEvals_watchCapacity(t *testing.T) {
	type fields struct {
		evalBroker       *EvalBroker
		enabled          bool
		stats            *BlockedStats
		l                sync.RWMutex
		captured         map[string]wrappedEval
		escaped          map[string]wrappedEval
		capacityChangeCh chan *capacityUpdate
		jobs             map[string]string
		unblockIndexes   map[string]uint64
		duplicates       []*models.Evaluation
		duplicateCh      chan struct{}
		stopCh           chan struct{}
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BlockedEvals{
				evalBroker:       tt.fields.evalBroker,
				enabled:          tt.fields.enabled,
				stats:            tt.fields.stats,
				l:                tt.fields.l,
				captured:         tt.fields.captured,
				escaped:          tt.fields.escaped,
				capacityChangeCh: tt.fields.capacityChangeCh,
				jobs:             tt.fields.jobs,
				unblockIndexes:   tt.fields.unblockIndexes,
				duplicates:       tt.fields.duplicates,
				duplicateCh:      tt.fields.duplicateCh,
				stopCh:           tt.fields.stopCh,
			}
			b.watchCapacity()
		})
	}
}

func TestBlockedEvals_unblock(t *testing.T) {
	type fields struct {
		evalBroker       *EvalBroker
		enabled          bool
		stats            *BlockedStats
		l                sync.RWMutex
		captured         map[string]wrappedEval
		escaped          map[string]wrappedEval
		capacityChangeCh chan *capacityUpdate
		jobs             map[string]string
		unblockIndexes   map[string]uint64
		duplicates       []*models.Evaluation
		duplicateCh      chan struct{}
		stopCh           chan struct{}
	}
	type args struct {
		computedClass string
		index         uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BlockedEvals{
				evalBroker:       tt.fields.evalBroker,
				enabled:          tt.fields.enabled,
				stats:            tt.fields.stats,
				l:                tt.fields.l,
				captured:         tt.fields.captured,
				escaped:          tt.fields.escaped,
				capacityChangeCh: tt.fields.capacityChangeCh,
				jobs:             tt.fields.jobs,
				unblockIndexes:   tt.fields.unblockIndexes,
				duplicates:       tt.fields.duplicates,
				duplicateCh:      tt.fields.duplicateCh,
				stopCh:           tt.fields.stopCh,
			}
			b.unblock(tt.args.computedClass, tt.args.index)
		})
	}
}

func TestBlockedEvals_UnblockFailed(t *testing.T) {
	type fields struct {
		evalBroker       *EvalBroker
		enabled          bool
		stats            *BlockedStats
		l                sync.RWMutex
		captured         map[string]wrappedEval
		escaped          map[string]wrappedEval
		capacityChangeCh chan *capacityUpdate
		jobs             map[string]string
		unblockIndexes   map[string]uint64
		duplicates       []*models.Evaluation
		duplicateCh      chan struct{}
		stopCh           chan struct{}
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BlockedEvals{
				evalBroker:       tt.fields.evalBroker,
				enabled:          tt.fields.enabled,
				stats:            tt.fields.stats,
				l:                tt.fields.l,
				captured:         tt.fields.captured,
				escaped:          tt.fields.escaped,
				capacityChangeCh: tt.fields.capacityChangeCh,
				jobs:             tt.fields.jobs,
				unblockIndexes:   tt.fields.unblockIndexes,
				duplicates:       tt.fields.duplicates,
				duplicateCh:      tt.fields.duplicateCh,
				stopCh:           tt.fields.stopCh,
			}
			b.UnblockFailed()
		})
	}
}

func TestBlockedEvals_GetDuplicates(t *testing.T) {
	type fields struct {
		evalBroker       *EvalBroker
		enabled          bool
		stats            *BlockedStats
		l                sync.RWMutex
		captured         map[string]wrappedEval
		escaped          map[string]wrappedEval
		capacityChangeCh chan *capacityUpdate
		jobs             map[string]string
		unblockIndexes   map[string]uint64
		duplicates       []*models.Evaluation
		duplicateCh      chan struct{}
		stopCh           chan struct{}
	}
	type args struct {
		timeout time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []*models.Evaluation
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BlockedEvals{
				evalBroker:       tt.fields.evalBroker,
				enabled:          tt.fields.enabled,
				stats:            tt.fields.stats,
				l:                tt.fields.l,
				captured:         tt.fields.captured,
				escaped:          tt.fields.escaped,
				capacityChangeCh: tt.fields.capacityChangeCh,
				jobs:             tt.fields.jobs,
				unblockIndexes:   tt.fields.unblockIndexes,
				duplicates:       tt.fields.duplicates,
				duplicateCh:      tt.fields.duplicateCh,
				stopCh:           tt.fields.stopCh,
			}
			if got := b.GetDuplicates(tt.args.timeout); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BlockedEvals.GetDuplicates() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBlockedEvals_Flush(t *testing.T) {
	type fields struct {
		evalBroker       *EvalBroker
		enabled          bool
		stats            *BlockedStats
		l                sync.RWMutex
		captured         map[string]wrappedEval
		escaped          map[string]wrappedEval
		capacityChangeCh chan *capacityUpdate
		jobs             map[string]string
		unblockIndexes   map[string]uint64
		duplicates       []*models.Evaluation
		duplicateCh      chan struct{}
		stopCh           chan struct{}
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BlockedEvals{
				evalBroker:       tt.fields.evalBroker,
				enabled:          tt.fields.enabled,
				stats:            tt.fields.stats,
				l:                tt.fields.l,
				captured:         tt.fields.captured,
				escaped:          tt.fields.escaped,
				capacityChangeCh: tt.fields.capacityChangeCh,
				jobs:             tt.fields.jobs,
				unblockIndexes:   tt.fields.unblockIndexes,
				duplicates:       tt.fields.duplicates,
				duplicateCh:      tt.fields.duplicateCh,
				stopCh:           tt.fields.stopCh,
			}
			b.Flush()
		})
	}
}

func TestBlockedEvals_Stats(t *testing.T) {
	type fields struct {
		evalBroker       *EvalBroker
		enabled          bool
		stats            *BlockedStats
		l                sync.RWMutex
		captured         map[string]wrappedEval
		escaped          map[string]wrappedEval
		capacityChangeCh chan *capacityUpdate
		jobs             map[string]string
		unblockIndexes   map[string]uint64
		duplicates       []*models.Evaluation
		duplicateCh      chan struct{}
		stopCh           chan struct{}
	}
	tests := []struct {
		name   string
		fields fields
		want   *BlockedStats
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BlockedEvals{
				evalBroker:       tt.fields.evalBroker,
				enabled:          tt.fields.enabled,
				stats:            tt.fields.stats,
				l:                tt.fields.l,
				captured:         tt.fields.captured,
				escaped:          tt.fields.escaped,
				capacityChangeCh: tt.fields.capacityChangeCh,
				jobs:             tt.fields.jobs,
				unblockIndexes:   tt.fields.unblockIndexes,
				duplicates:       tt.fields.duplicates,
				duplicateCh:      tt.fields.duplicateCh,
				stopCh:           tt.fields.stopCh,
			}
			if got := b.Stats(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BlockedEvals.Stats() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBlockedEvals_EmitStats(t *testing.T) {
	type fields struct {
		evalBroker       *EvalBroker
		enabled          bool
		stats            *BlockedStats
		l                sync.RWMutex
		captured         map[string]wrappedEval
		escaped          map[string]wrappedEval
		capacityChangeCh chan *capacityUpdate
		jobs             map[string]string
		unblockIndexes   map[string]uint64
		duplicates       []*models.Evaluation
		duplicateCh      chan struct{}
		stopCh           chan struct{}
	}
	type args struct {
		period time.Duration
		stopCh chan struct{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BlockedEvals{
				evalBroker:       tt.fields.evalBroker,
				enabled:          tt.fields.enabled,
				stats:            tt.fields.stats,
				l:                tt.fields.l,
				captured:         tt.fields.captured,
				escaped:          tt.fields.escaped,
				capacityChangeCh: tt.fields.capacityChangeCh,
				jobs:             tt.fields.jobs,
				unblockIndexes:   tt.fields.unblockIndexes,
				duplicates:       tt.fields.duplicates,
				duplicateCh:      tt.fields.duplicateCh,
				stopCh:           tt.fields.stopCh,
			}
			b.EmitStats(tt.args.period, tt.args.stopCh)
		})
	}
}
