/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package server

import (
	"reflect"
	"testing"
)

func TestNewEvaluatePool(t *testing.T) {
	type args struct {
		workers int
		bufSize int
	}
	tests := []struct {
		name string
		args args
		want *EvaluatePool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewEvaluatePool(tt.args.workers, tt.args.bufSize); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewEvaluatePool() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvaluatePool_Size(t *testing.T) {
	type fields struct {
		workers    int
		workerStop []chan struct{}
		req        chan evaluateRequest
		res        chan evaluateResult
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &EvaluatePool{
				workers:    tt.fields.workers,
				workerStop: tt.fields.workerStop,
				req:        tt.fields.req,
				res:        tt.fields.res,
			}
			if got := p.Size(); got != tt.want {
				t.Errorf("EvaluatePool.Size() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvaluatePool_SetSize(t *testing.T) {
	type fields struct {
		workers    int
		workerStop []chan struct{}
		req        chan evaluateRequest
		res        chan evaluateResult
	}
	type args struct {
		size int
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
			p := &EvaluatePool{
				workers:    tt.fields.workers,
				workerStop: tt.fields.workerStop,
				req:        tt.fields.req,
				res:        tt.fields.res,
			}
			p.SetSize(tt.args.size)
		})
	}
}

func TestEvaluatePool_RequestCh(t *testing.T) {
	type fields struct {
		workers    int
		workerStop []chan struct{}
		req        chan evaluateRequest
		res        chan evaluateResult
	}
	tests := []struct {
		name   string
		fields fields
		want   chan<- evaluateRequest
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &EvaluatePool{
				workers:    tt.fields.workers,
				workerStop: tt.fields.workerStop,
				req:        tt.fields.req,
				res:        tt.fields.res,
			}
			if got := p.RequestCh(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EvaluatePool.RequestCh() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvaluatePool_ResultCh(t *testing.T) {
	type fields struct {
		workers    int
		workerStop []chan struct{}
		req        chan evaluateRequest
		res        chan evaluateResult
	}
	tests := []struct {
		name   string
		fields fields
		want   <-chan evaluateResult
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &EvaluatePool{
				workers:    tt.fields.workers,
				workerStop: tt.fields.workerStop,
				req:        tt.fields.req,
				res:        tt.fields.res,
			}
			if got := p.ResultCh(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EvaluatePool.ResultCh() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvaluatePool_Shutdown(t *testing.T) {
	type fields struct {
		workers    int
		workerStop []chan struct{}
		req        chan evaluateRequest
		res        chan evaluateResult
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &EvaluatePool{
				workers:    tt.fields.workers,
				workerStop: tt.fields.workerStop,
				req:        tt.fields.req,
				res:        tt.fields.res,
			}
			p.Shutdown()
		})
	}
}

func TestEvaluatePool_run(t *testing.T) {
	type fields struct {
		workers    int
		workerStop []chan struct{}
		req        chan evaluateRequest
		res        chan evaluateResult
	}
	type args struct {
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
			p := &EvaluatePool{
				workers:    tt.fields.workers,
				workerStop: tt.fields.workerStop,
				req:        tt.fields.req,
				res:        tt.fields.res,
			}
			p.run(tt.args.stopCh)
		})
	}
}
