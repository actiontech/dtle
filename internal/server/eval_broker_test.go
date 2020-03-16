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
	"github.com/actiontech/dts/internal/models"
)

func TestNewEvalBroker(t *testing.T) {
	type args struct {
		timeout       time.Duration
		deliveryLimit int
	}
	tests := []struct {
		name    string
		args    args
		want    *EvalBroker
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewEvalBroker(tt.args.timeout, tt.args.deliveryLimit)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewEvalBroker() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewEvalBroker() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvalBroker_Enabled(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
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
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			if got := b.Enabled(); got != tt.want {
				t.Errorf("EvalBroker.Enabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvalBroker_SetEnabled(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
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
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			b.SetEnabled(tt.args.enabled)
		})
	}
}

func TestEvalBroker_Enqueue(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
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
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			b.Enqueue(tt.args.eval)
		})
	}
}

func TestEvalBroker_EnqueueAll(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
	}
	type args struct {
		evals map[*models.Evaluation]string
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
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			b.EnqueueAll(tt.args.evals)
		})
	}
}

func TestEvalBroker_processEnqueue(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
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
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			b.processEnqueue(tt.args.eval, tt.args.token)
		})
	}
}

func TestEvalBroker_enqueueWaiting(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
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
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			b.enqueueWaiting(tt.args.eval)
		})
	}
}

func TestEvalBroker_enqueueLocked(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
	}
	type args struct {
		eval  *models.Evaluation
		queue string
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
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			b.enqueueLocked(tt.args.eval, tt.args.queue)
		})
	}
}

func TestEvalBroker_Dequeue(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
	}
	type args struct {
		schedulers []string
		timeout    time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *models.Evaluation
		want1   string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			got, got1, err := b.Dequeue(tt.args.schedulers, tt.args.timeout)
			if (err != nil) != tt.wantErr {
				t.Errorf("EvalBroker.Dequeue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EvalBroker.Dequeue() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("EvalBroker.Dequeue() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestEvalBroker_scanForSchedulers(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
	}
	type args struct {
		schedulers []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *models.Evaluation
		want1   string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			got, got1, err := b.scanForSchedulers(tt.args.schedulers)
			if (err != nil) != tt.wantErr {
				t.Errorf("EvalBroker.scanForSchedulers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EvalBroker.scanForSchedulers() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("EvalBroker.scanForSchedulers() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestEvalBroker_dequeueForSched(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
	}
	type args struct {
		sched string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *models.Evaluation
		want1   string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			got, got1, err := b.dequeueForSched(tt.args.sched)
			if (err != nil) != tt.wantErr {
				t.Errorf("EvalBroker.dequeueForSched() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EvalBroker.dequeueForSched() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("EvalBroker.dequeueForSched() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestEvalBroker_waitForSchedulers(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
	}
	type args struct {
		schedulers []string
		timeoutCh  <-chan time.Time
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
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			if got := b.waitForSchedulers(tt.args.schedulers, tt.args.timeoutCh); got != tt.want {
				t.Errorf("EvalBroker.waitForSchedulers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvalBroker_Outstanding(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
	}
	type args struct {
		evalID string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
		want1  bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			got, got1 := b.Outstanding(tt.args.evalID)
			if got != tt.want {
				t.Errorf("EvalBroker.Outstanding() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("EvalBroker.Outstanding() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestEvalBroker_OutstandingReset(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
	}
	type args struct {
		evalID string
		token  string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			if err := b.OutstandingReset(tt.args.evalID, tt.args.token); (err != nil) != tt.wantErr {
				t.Errorf("EvalBroker.OutstandingReset() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEvalBroker_Ack(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
	}
	type args struct {
		evalID string
		token  string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			if err := b.Ack(tt.args.evalID, tt.args.token); (err != nil) != tt.wantErr {
				t.Errorf("EvalBroker.Ack() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEvalBroker_Nack(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
	}
	type args struct {
		evalID string
		token  string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			if err := b.Nack(tt.args.evalID, tt.args.token); (err != nil) != tt.wantErr {
				t.Errorf("EvalBroker.Nack() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEvalBroker_PauseNackTimeout(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
	}
	type args struct {
		evalID string
		token  string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			if err := b.PauseNackTimeout(tt.args.evalID, tt.args.token); (err != nil) != tt.wantErr {
				t.Errorf("EvalBroker.PauseNackTimeout() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEvalBroker_ResumeNackTimeout(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
	}
	type args struct {
		evalID string
		token  string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			if err := b.ResumeNackTimeout(tt.args.evalID, tt.args.token); (err != nil) != tt.wantErr {
				t.Errorf("EvalBroker.ResumeNackTimeout() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEvalBroker_Flush(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			b.Flush()
		})
	}
}

func TestEvalBroker_Stats(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
	}
	tests := []struct {
		name   string
		fields fields
		want   *BrokerStats
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			if got := b.Stats(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EvalBroker.Stats() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvalBroker_EmitStats(t *testing.T) {
	type fields struct {
		nackTimeout   time.Duration
		deliveryLimit int
		enabled       bool
		stats         *BrokerStats
		evals         map[string]int
		jobEvals      map[string]string
		blocked       map[string]PendingEvaluations
		ready         map[string]PendingEvaluations
		unack         map[string]*unackEval
		waiting       map[string]chan struct{}
		requeue       map[string]*models.Evaluation
		timeWait      map[string]*time.Timer
		l             sync.RWMutex
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
			b := &EvalBroker{
				nackTimeout:   tt.fields.nackTimeout,
				deliveryLimit: tt.fields.deliveryLimit,
				enabled:       tt.fields.enabled,
				stats:         tt.fields.stats,
				evals:         tt.fields.evals,
				jobEvals:      tt.fields.jobEvals,
				blocked:       tt.fields.blocked,
				ready:         tt.fields.ready,
				unack:         tt.fields.unack,
				waiting:       tt.fields.waiting,
				requeue:       tt.fields.requeue,
				timeWait:      tt.fields.timeWait,
				l:             tt.fields.l,
			}
			b.EmitStats(tt.args.period, tt.args.stopCh)
		})
	}
}

func TestPendingEvaluations_Len(t *testing.T) {
	tests := []struct {
		name string
		p    PendingEvaluations
		want int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.Len(); got != tt.want {
				t.Errorf("PendingEvaluations.Len() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPendingEvaluations_Less(t *testing.T) {
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name string
		p    PendingEvaluations
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.Less(tt.args.i, tt.args.j); got != tt.want {
				t.Errorf("PendingEvaluations.Less() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPendingEvaluations_Swap(t *testing.T) {
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name string
		p    PendingEvaluations
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.p.Swap(tt.args.i, tt.args.j)
		})
	}
}

func TestPendingEvaluations_Push(t *testing.T) {
	type args struct {
		e interface{}
	}
	tests := []struct {
		name string
		p    *PendingEvaluations
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.p.Push(tt.args.e)
		})
	}
}

func TestPendingEvaluations_Pop(t *testing.T) {
	tests := []struct {
		name string
		p    *PendingEvaluations
		want interface{}
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.Pop(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PendingEvaluations.Pop() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPendingEvaluations_Peek(t *testing.T) {
	tests := []struct {
		name string
		p    PendingEvaluations
		want *models.Evaluation
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.Peek(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PendingEvaluations.Peek() = %v, want %v", got, tt.want)
			}
		})
	}
}
