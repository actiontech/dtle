/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package command

import (
	"reflect"
	"sync"
	"testing"
	"github.com/actiontech/dts/api"

	"github.com/mitchellh/cli"
)

func Test_newEvalState(t *testing.T) {
	tests := []struct {
		name string
		want *evalState
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newEvalState(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newEvalState() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newMonitor(t *testing.T) {
	type args struct {
		ui     cli.Ui
		client *api.Client
		length int
	}
	tests := []struct {
		name string
		args args
		want *monitor
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newMonitor(tt.args.ui, tt.args.client, tt.args.length); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newMonitor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_monitor_update(t *testing.T) {
	type fields struct {
		ui     cli.Ui
		client *api.Client
		state  *evalState
		length int
		Mutex  sync.Mutex
	}
	type args struct {
		update *evalState
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
			m := &monitor{
				ui:     tt.fields.ui,
				client: tt.fields.client,
				state:  tt.fields.state,
				length: tt.fields.length,
				Mutex:  tt.fields.Mutex,
			}
			m.update(tt.args.update)
		})
	}
}

func Test_monitor_monitor(t *testing.T) {
	type fields struct {
		ui     cli.Ui
		client *api.Client
		state  *evalState
		length int
		Mutex  sync.Mutex
	}
	type args struct {
		evalID      string
		allowPrefix bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &monitor{
				ui:     tt.fields.ui,
				client: tt.fields.client,
				state:  tt.fields.state,
				length: tt.fields.length,
				Mutex:  tt.fields.Mutex,
			}
			if got := m.monitor(tt.args.evalID, tt.args.allowPrefix); got != tt.want {
				t.Errorf("monitor.monitor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_dumpAllocStatus(t *testing.T) {
	type args struct {
		ui     cli.Ui
		alloc  *api.Allocation
		length int
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dumpAllocStatus(tt.args.ui, tt.args.alloc, tt.args.length)
		})
	}
}

func Test_formatAllocMetrics(t *testing.T) {
	type args struct {
		metrics *api.AllocationMetric
		scores  bool
		prefix  string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatAllocMetrics(tt.args.metrics, tt.args.scores, tt.args.prefix); got != tt.want {
				t.Errorf("formatAllocMetrics() = %v, want %v", got, tt.want)
			}
		})
	}
}
