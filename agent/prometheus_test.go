/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package agent

import (
	"reflect"
	"sync"
	"testing"
	"time"
	log "udup/internal/logger"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewPrometheusSink(t *testing.T) {
	type args struct {
		metricsAddr     string
		metricsInterval time.Duration
		logger          *log.Logger
	}
	tests := []struct {
		name    string
		args    args
		want    *PrometheusSink
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewPrometheusSink(tt.args.metricsAddr, tt.args.metricsInterval, tt.args.logger)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewPrometheusSink() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewPrometheusSink() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPrometheusSink_flattenKey(t *testing.T) {
	type fields struct {
		mu        sync.Mutex
		logger    *log.Logger
		gauges    map[string]prometheus.Gauge
		summaries map[string]prometheus.Summary
		counters  map[string]prometheus.Counter
	}
	type args struct {
		parts []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PrometheusSink{
				mu:        tt.fields.mu,
				logger:    tt.fields.logger,
				gauges:    tt.fields.gauges,
				summaries: tt.fields.summaries,
				counters:  tt.fields.counters,
			}
			if got := p.flattenKey(tt.args.parts); got != tt.want {
				t.Errorf("PrometheusSink.flattenKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPrometheusSink_SetGauge(t *testing.T) {
	type fields struct {
		mu        sync.Mutex
		logger    *log.Logger
		gauges    map[string]prometheus.Gauge
		summaries map[string]prometheus.Summary
		counters  map[string]prometheus.Counter
	}
	type args struct {
		parts []string
		val   float32
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
			p := &PrometheusSink{
				mu:        tt.fields.mu,
				logger:    tt.fields.logger,
				gauges:    tt.fields.gauges,
				summaries: tt.fields.summaries,
				counters:  tt.fields.counters,
			}
			p.SetGauge(tt.args.parts, tt.args.val)
		})
	}
}

func TestPrometheusSink_SetGaugeOpts(t *testing.T) {
	type fields struct {
		mu        sync.Mutex
		logger    *log.Logger
		gauges    map[string]prometheus.Gauge
		summaries map[string]prometheus.Summary
		counters  map[string]prometheus.Counter
	}
	type args struct {
		labels map[string]string
		parts  []string
		val    float32
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
			p := &PrometheusSink{
				mu:        tt.fields.mu,
				logger:    tt.fields.logger,
				gauges:    tt.fields.gauges,
				summaries: tt.fields.summaries,
				counters:  tt.fields.counters,
			}
			p.SetGaugeOpts(tt.args.labels, tt.args.parts, tt.args.val)
		})
	}
}

func TestPrometheusSink_AddSample(t *testing.T) {
	type fields struct {
		mu        sync.Mutex
		logger    *log.Logger
		gauges    map[string]prometheus.Gauge
		summaries map[string]prometheus.Summary
		counters  map[string]prometheus.Counter
	}
	type args struct {
		parts []string
		val   float32
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
			p := &PrometheusSink{
				mu:        tt.fields.mu,
				logger:    tt.fields.logger,
				gauges:    tt.fields.gauges,
				summaries: tt.fields.summaries,
				counters:  tt.fields.counters,
			}
			p.AddSample(tt.args.parts, tt.args.val)
		})
	}
}

func TestPrometheusSink_EmitKey(t *testing.T) {
	type fields struct {
		mu        sync.Mutex
		logger    *log.Logger
		gauges    map[string]prometheus.Gauge
		summaries map[string]prometheus.Summary
		counters  map[string]prometheus.Counter
	}
	type args struct {
		key []string
		val float32
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
			p := &PrometheusSink{
				mu:        tt.fields.mu,
				logger:    tt.fields.logger,
				gauges:    tt.fields.gauges,
				summaries: tt.fields.summaries,
				counters:  tt.fields.counters,
			}
			p.EmitKey(tt.args.key, tt.args.val)
		})
	}
}

func TestPrometheusSink_IncrCounter(t *testing.T) {
	type fields struct {
		mu        sync.Mutex
		logger    *log.Logger
		gauges    map[string]prometheus.Gauge
		summaries map[string]prometheus.Summary
		counters  map[string]prometheus.Counter
	}
	type args struct {
		parts []string
		val   float32
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
			p := &PrometheusSink{
				mu:        tt.fields.mu,
				logger:    tt.fields.logger,
				gauges:    tt.fields.gauges,
				summaries: tt.fields.summaries,
				counters:  tt.fields.counters,
			}
			p.IncrCounter(tt.args.parts, tt.args.val)
		})
	}
}

func TestPrometheusSink_pushMetric(t *testing.T) {
	type fields struct {
		mu        sync.Mutex
		logger    *log.Logger
		gauges    map[string]prometheus.Gauge
		summaries map[string]prometheus.Summary
		counters  map[string]prometheus.Counter
	}
	type args struct {
		addr     string
		interval time.Duration
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
			p := &PrometheusSink{
				mu:        tt.fields.mu,
				logger:    tt.fields.logger,
				gauges:    tt.fields.gauges,
				summaries: tt.fields.summaries,
				counters:  tt.fields.counters,
			}
			p.pushMetric(tt.args.addr, tt.args.interval)
		})
	}
}

func TestPrometheusSink_prometheusPushClient(t *testing.T) {
	type fields struct {
		mu        sync.Mutex
		logger    *log.Logger
		gauges    map[string]prometheus.Gauge
		summaries map[string]prometheus.Summary
		counters  map[string]prometheus.Counter
	}
	type args struct {
		addr     string
		interval time.Duration
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
			p := &PrometheusSink{
				mu:        tt.fields.mu,
				logger:    tt.fields.logger,
				gauges:    tt.fields.gauges,
				summaries: tt.fields.summaries,
				counters:  tt.fields.counters,
			}
			p.prometheusPushClient(tt.args.addr, tt.args.interval)
		})
	}
}
