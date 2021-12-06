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

// GaugeVecProxy to proxy prometheus.GaugeVec.
type GaugeVecProxy struct {
	mu sync.Mutex

	LabelNamesIndex map[string]int
	Labels          map[string][]string
	*prometheus.GaugeVec
}

// NewGaugeVec creates a new GaugeVec based on the provided GaugeOpts and
// partitioned by the given label names.
func NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *GaugeVecProxy {
	gaugeVecProxy := &GaugeVecProxy{
		LabelNamesIndex: make(map[string]int, len(labelNames)),
		Labels:          make(map[string][]string),
		GaugeVec:        prometheus.NewGaugeVec(opts, labelNames),
	}
	for idx, v := range labelNames {
		gaugeVecProxy.LabelNamesIndex[v] = idx
	}
	return gaugeVecProxy
}

// WithLabelValues works as GetMetricWithLabelValues, but panics where
// GetMetricWithLabelValues would have returned an error. Not returning an
// error allows shortcuts like
//     myVec.WithLabelValues("404", "GET").Add(42)
func (c *GaugeVecProxy) WithLabelValues(lvs ...string) prometheus.Gauge {
	if len(lvs) > 0 {
		noteLabelsInMetricsProxy(c, lvs)
	}
	return c.GaugeVec.WithLabelValues(lvs...)
}

// With works as GetMetricWith, but panics where GetMetricWithLabels would have
// returned an error. Not returning an error allows shortcuts like
//     myVec.With(prometheus.Labels{"code": "404", "method": "GET"}).Add(42)
func (c *GaugeVecProxy) With(labels prometheus.Labels) prometheus.Gauge {
	if len(labels) > 0 {
		values := make([]string, len(labels))
		labelNameIndex := c.GetLabelNamesIndex()
		for k, v := range labels {
			values[labelNameIndex[k]] = v
		}
		noteLabelsInMetricsProxy(c, values)
	}

	return c.GaugeVec.With(labels)
}

// DeleteAllAboutLabels Remove all labelsValue with these labels.
func (c *GaugeVecProxy) DeleteAllAboutLabels(labels prometheus.Labels) bool {
	if len(labels) == 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return findAndDeleteLabelsInMetricsProxy(c, labels)
}

// GetLabelNamesIndex to support get GaugeVecProxy's LabelNames when you use Proxy object.
func (c *GaugeVecProxy) GetLabelNamesIndex() map[string]int {
	return c.LabelNamesIndex
}

// GetLabels to support get GaugeVecProxy's Labels when you use Proxy object.
func (c *GaugeVecProxy) GetLabels() map[string][]string {
	return c.Labels
}

// SetLabel to support set GaugeVecProxy's Label when you use Proxy object.
func (c *GaugeVecProxy) SetLabel(key string, vals []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Labels[key] = vals
}

// vecDelete to support delete GaugeVecProxy's Labels when you use Proxy object.
func (c *GaugeVecProxy) vecDelete(labels prometheus.Labels) bool {
	return c.GaugeVec.Delete(labels)
}
