/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package client

import (
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"
	"github.com/actiontech/dts/internal/models"
)

func Test_newRestartTracker(t *testing.T) {
	tests := []struct {
		name string
		want *RestartTracker
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newRestartTracker(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newRestartTracker() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRestartTracker_SetStartError(t *testing.T) {
	type fields struct {
		waitRes          *models.WaitResult
		startErr         error
		restartTriggered bool
		count            int
		onSuccess        bool
		startTime        time.Time
		reason           string
		rand             *rand.Rand
		lock             sync.Mutex
	}
	type args struct {
		err error
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *RestartTracker
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RestartTracker{
				waitRes:          tt.fields.waitRes,
				startErr:         tt.fields.startErr,
				restartTriggered: tt.fields.restartTriggered,
				count:            tt.fields.count,
				onSuccess:        tt.fields.onSuccess,
				startTime:        tt.fields.startTime,
				reason:           tt.fields.reason,
				rand:             tt.fields.rand,
				lock:             tt.fields.lock,
			}
			if got := r.SetStartError(tt.args.err); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RestartTracker.SetStartError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRestartTracker_SetWaitResult(t *testing.T) {
	type fields struct {
		waitRes          *models.WaitResult
		startErr         error
		restartTriggered bool
		count            int
		onSuccess        bool
		startTime        time.Time
		reason           string
		rand             *rand.Rand
		lock             sync.Mutex
	}
	type args struct {
		res *models.WaitResult
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *RestartTracker
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RestartTracker{
				waitRes:          tt.fields.waitRes,
				startErr:         tt.fields.startErr,
				restartTriggered: tt.fields.restartTriggered,
				count:            tt.fields.count,
				onSuccess:        tt.fields.onSuccess,
				startTime:        tt.fields.startTime,
				reason:           tt.fields.reason,
				rand:             tt.fields.rand,
				lock:             tt.fields.lock,
			}
			if got := r.SetWaitResult(tt.args.res); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RestartTracker.SetWaitResult() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRestartTracker_SetRestartTriggered(t *testing.T) {
	type fields struct {
		waitRes          *models.WaitResult
		startErr         error
		restartTriggered bool
		count            int
		onSuccess        bool
		startTime        time.Time
		reason           string
		rand             *rand.Rand
		lock             sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   *RestartTracker
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RestartTracker{
				waitRes:          tt.fields.waitRes,
				startErr:         tt.fields.startErr,
				restartTriggered: tt.fields.restartTriggered,
				count:            tt.fields.count,
				onSuccess:        tt.fields.onSuccess,
				startTime:        tt.fields.startTime,
				reason:           tt.fields.reason,
				rand:             tt.fields.rand,
				lock:             tt.fields.lock,
			}
			if got := r.SetRestartTriggered(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RestartTracker.SetRestartTriggered() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRestartTracker_GetReason(t *testing.T) {
	type fields struct {
		waitRes          *models.WaitResult
		startErr         error
		restartTriggered bool
		count            int
		onSuccess        bool
		startTime        time.Time
		reason           string
		rand             *rand.Rand
		lock             sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RestartTracker{
				waitRes:          tt.fields.waitRes,
				startErr:         tt.fields.startErr,
				restartTriggered: tt.fields.restartTriggered,
				count:            tt.fields.count,
				onSuccess:        tt.fields.onSuccess,
				startTime:        tt.fields.startTime,
				reason:           tt.fields.reason,
				rand:             tt.fields.rand,
				lock:             tt.fields.lock,
			}
			if got := r.GetReason(); got != tt.want {
				t.Errorf("RestartTracker.GetReason() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRestartTracker_GetState(t *testing.T) {
	type fields struct {
		waitRes          *models.WaitResult
		startErr         error
		restartTriggered bool
		count            int
		onSuccess        bool
		startTime        time.Time
		reason           string
		rand             *rand.Rand
		lock             sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   string
		want1  time.Duration
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RestartTracker{
				waitRes:          tt.fields.waitRes,
				startErr:         tt.fields.startErr,
				restartTriggered: tt.fields.restartTriggered,
				count:            tt.fields.count,
				onSuccess:        tt.fields.onSuccess,
				startTime:        tt.fields.startTime,
				reason:           tt.fields.reason,
				rand:             tt.fields.rand,
				lock:             tt.fields.lock,
			}
			got, got1 := r.GetState()
			if got != tt.want {
				t.Errorf("RestartTracker.GetState() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("RestartTracker.GetState() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestRestartTracker_handleStartError(t *testing.T) {
	type fields struct {
		waitRes          *models.WaitResult
		startErr         error
		restartTriggered bool
		count            int
		onSuccess        bool
		startTime        time.Time
		reason           string
		rand             *rand.Rand
		lock             sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   string
		want1  time.Duration
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RestartTracker{
				waitRes:          tt.fields.waitRes,
				startErr:         tt.fields.startErr,
				restartTriggered: tt.fields.restartTriggered,
				count:            tt.fields.count,
				onSuccess:        tt.fields.onSuccess,
				startTime:        tt.fields.startTime,
				reason:           tt.fields.reason,
				rand:             tt.fields.rand,
				lock:             tt.fields.lock,
			}
			got, got1 := r.handleStartError()
			if got != tt.want {
				t.Errorf("RestartTracker.handleStartError() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("RestartTracker.handleStartError() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestRestartTracker_handleWaitResult(t *testing.T) {
	type fields struct {
		waitRes          *models.WaitResult
		startErr         error
		restartTriggered bool
		count            int
		onSuccess        bool
		startTime        time.Time
		reason           string
		rand             *rand.Rand
		lock             sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   string
		want1  time.Duration
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RestartTracker{
				waitRes:          tt.fields.waitRes,
				startErr:         tt.fields.startErr,
				restartTriggered: tt.fields.restartTriggered,
				count:            tt.fields.count,
				onSuccess:        tt.fields.onSuccess,
				startTime:        tt.fields.startTime,
				reason:           tt.fields.reason,
				rand:             tt.fields.rand,
				lock:             tt.fields.lock,
			}
			got, got1 := r.handleWaitResult()
			if got != tt.want {
				t.Errorf("RestartTracker.handleWaitResult() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("RestartTracker.handleWaitResult() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestRestartTracker_getDelay(t *testing.T) {
	type fields struct {
		waitRes          *models.WaitResult
		startErr         error
		restartTriggered bool
		count            int
		onSuccess        bool
		startTime        time.Time
		reason           string
		rand             *rand.Rand
		lock             sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Duration
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RestartTracker{
				waitRes:          tt.fields.waitRes,
				startErr:         tt.fields.startErr,
				restartTriggered: tt.fields.restartTriggered,
				count:            tt.fields.count,
				onSuccess:        tt.fields.onSuccess,
				startTime:        tt.fields.startTime,
				reason:           tt.fields.reason,
				rand:             tt.fields.rand,
				lock:             tt.fields.lock,
			}
			if got := r.getDelay(); got != tt.want {
				t.Errorf("RestartTracker.getDelay() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRestartTracker_jitter(t *testing.T) {
	type fields struct {
		waitRes          *models.WaitResult
		startErr         error
		restartTriggered bool
		count            int
		onSuccess        bool
		startTime        time.Time
		reason           string
		rand             *rand.Rand
		lock             sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Duration
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RestartTracker{
				waitRes:          tt.fields.waitRes,
				startErr:         tt.fields.startErr,
				restartTriggered: tt.fields.restartTriggered,
				count:            tt.fields.count,
				onSuccess:        tt.fields.onSuccess,
				startTime:        tt.fields.startTime,
				reason:           tt.fields.reason,
				rand:             tt.fields.rand,
				lock:             tt.fields.lock,
			}
			if got := r.jitter(); got != tt.want {
				t.Errorf("RestartTracker.jitter() = %v, want %v", got, tt.want)
			}
		})
	}
}
