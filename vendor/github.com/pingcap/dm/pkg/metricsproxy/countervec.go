// Copyright 2020 PingCAP, Inc.
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

package metricsproxy // nolint:dupl

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// CounterVecProxy to proxy prometheus.CounterVec.
type CounterVecProxy struct {
	mu sync.Mutex

	LabelNamesIndex map[string]int
	Labels          map[string][]string
	*prometheus.CounterVec
}

// NewCounterVec creates a new CounterVec based on the provided CounterOpts and
// partitioned by the given label names.
func NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *CounterVecProxy {
	counterVecProxy := &CounterVecProxy{
		LabelNamesIndex: make(map[string]int, len(labelNames)),
		Labels:          make(map[string][]string),
		CounterVec:      prometheus.NewCounterVec(opts, labelNames),
	}
	for idx, v := range labelNames {
		counterVecProxy.LabelNamesIndex[v] = idx
	}
	return counterVecProxy
}

// WithLabelValues works as GetMetricWithLabelValues, but panics where
// GetMetricWithLabelValues would have returned an error. Not returning an
// error allows shortcuts like
//     myVec.WithLabelValues("404", "GET").Add(42)
func (c *CounterVecProxy) WithLabelValues(lvs ...string) prometheus.Counter {
	if len(lvs) > 0 {
		noteLabelsInMetricsProxy(c, lvs)
	}
	return c.CounterVec.WithLabelValues(lvs...)
}

// With works as GetMetricWith, but panics where GetMetricWithLabels would have
// returned an error. Not returning an error allows shortcuts like
//     myVec.With(prometheus.Labels{"code": "404", "method": "GET"}).Add(42)
func (c *CounterVecProxy) With(labels prometheus.Labels) prometheus.Counter {
	if len(labels) > 0 {
		values := make([]string, len(labels))
		labelNameIndex := c.GetLabelNamesIndex()
		for k, v := range labels {
			values[labelNameIndex[k]] = v
		}
		noteLabelsInMetricsProxy(c, values)
	}

	return c.CounterVec.With(labels)
}

// DeleteAllAboutLabels Remove all labelsValue with these labels.
func (c *CounterVecProxy) DeleteAllAboutLabels(labels prometheus.Labels) bool {
	if len(labels) == 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return findAndDeleteLabelsInMetricsProxy(c, labels)
}

// GetLabelNamesIndex to support get CounterVecProxy's LabelNames when you use Proxy object.
func (c *CounterVecProxy) GetLabelNamesIndex() map[string]int {
	return c.LabelNamesIndex
}

// GetLabels to support get CounterVecProxy's Labels when you use Proxy object.
func (c *CounterVecProxy) GetLabels() map[string][]string {
	return c.Labels
}

// SetLabel to support set CounterVecProxy's Label when you use Proxy object.
func (c *CounterVecProxy) SetLabel(key string, vals []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Labels[key] = vals
}

// vecDelete to support delete CounterVecProxy's Labels when you use Proxy object.
func (c *CounterVecProxy) vecDelete(labels prometheus.Labels) bool {
	return c.CounterVec.Delete(labels)
}
