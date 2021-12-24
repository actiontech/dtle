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

// SummaryVecProxy to proxy prometheus.SummaryVec.
type SummaryVecProxy struct {
	mu sync.Mutex

	LabelNamesIndex map[string]int
	Labels          map[string][]string
	*prometheus.SummaryVec
}

// NewSummaryVec creates a new SummaryVec based on the provided SummaryOpts and
// partitioned by the given label names.
//
// Due to the way a Summary is represented in the Prometheus text format and how
// it is handled by the Prometheus server internally, “quantile” is an illegal
// label name. NewSummaryVec will panic if this label name is used.
func NewSummaryVec(opts prometheus.SummaryOpts, labelNames []string) *SummaryVecProxy {
	summaryVecProxy := &SummaryVecProxy{
		LabelNamesIndex: make(map[string]int),
		Labels:          make(map[string][]string),
		SummaryVec:      prometheus.NewSummaryVec(opts, labelNames),
	}
	for idx, v := range labelNames {
		summaryVecProxy.LabelNamesIndex[v] = idx
	}
	return summaryVecProxy
}

// WithLabelValues works as GetMetricWithLabelValues, but panics where
// GetMetricWithLabelValues would have returned an error. Not returning an
// error allows shortcuts like
//     myVec.WithLabelValues("404", "GET").Observe(42.21)
func (c *SummaryVecProxy) WithLabelValues(lvs ...string) prometheus.Observer {
	if len(lvs) > 0 {
		noteLabelsInMetricsProxy(c, lvs)
	}
	return c.SummaryVec.WithLabelValues(lvs...)
}

// With works as GetMetricWith, but panics where GetMetricWithLabels would have
// returned an error. Not returning an error allows shortcuts like
//     myVec.With(prometheus.Labels{"code": "404", "method": "GET"}).Observe(42.21)
func (c *SummaryVecProxy) With(labels prometheus.Labels) prometheus.Observer {
	if len(labels) > 0 {
		values := make([]string, len(labels))
		labelNameIndex := c.GetLabelNamesIndex()
		for k, v := range labels {
			values[labelNameIndex[k]] = v
		}
		noteLabelsInMetricsProxy(c, values)
	}

	return c.SummaryVec.With(labels)
}

// DeleteAllAboutLabels Remove all labelsValue with these labels.
func (c *SummaryVecProxy) DeleteAllAboutLabels(labels prometheus.Labels) bool {
	if len(labels) == 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return findAndDeleteLabelsInMetricsProxy(c, labels)
}

// GetLabelNamesIndex to support get SummaryVecProxy's LabelNames when you use Proxy object.
func (c *SummaryVecProxy) GetLabelNamesIndex() map[string]int {
	return c.LabelNamesIndex
}

// GetLabels to support get SummaryVecProxy's Labels when you use Proxy object.
func (c *SummaryVecProxy) GetLabels() map[string][]string {
	return c.Labels
}

// SetLabel to support set SummaryVecProxy's Label when you use Proxy object.
func (c *SummaryVecProxy) SetLabel(key string, vals []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Labels[key] = vals
}

// vecDelete to support delete SummaryVecProxy's Labels when you use Proxy object.
func (c *SummaryVecProxy) vecDelete(labels prometheus.Labels) bool {
	return c.SummaryVec.Delete(labels)
}
