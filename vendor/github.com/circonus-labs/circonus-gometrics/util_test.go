// Copyright 2016 Circonus, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package circonusgometrics

import (
	"testing"
)

func TestReset(t *testing.T) {
	t.Log("Testing util.Reset")

	cm := &CirconusMetrics{}

	cm.counters = make(map[string]uint64)
	cm.counterFuncs = make(map[string]func() uint64)
	cm.Increment("foo")

	cm.gauges = make(map[string]string)
	cm.gaugeFuncs = make(map[string]func() int64)
	cm.Gauge("foo", 1)

	cm.histograms = make(map[string]*Histogram)
	cm.Timing("foo", 1)

	cm.text = make(map[string]string)
	cm.textFuncs = make(map[string]func() string)
	cm.SetText("foo", "bar")

	if len(cm.counters) != 1 {
		t.Errorf("Expected 1, found %d", len(cm.counters))
	}

	if len(cm.gauges) != 1 {
		t.Errorf("Expected 1, found %d", len(cm.gauges))
	}

	if len(cm.histograms) != 1 {
		t.Errorf("Expected 1, found %d", len(cm.histograms))
	}

	if len(cm.text) != 1 {
		t.Errorf("Expected 1, found %d", len(cm.text))
	}

	cm.Reset()

	if len(cm.counters) != 0 {
		t.Errorf("Expected 0, found %d", len(cm.counters))
	}

	if len(cm.gauges) != 0 {
		t.Errorf("Expected 0, found %d", len(cm.gauges))
	}

	if len(cm.histograms) != 0 {
		t.Errorf("Expected 0, found %d", len(cm.histograms))
	}

	if len(cm.text) != 0 {
		t.Errorf("Expected 0, found %d", len(cm.text))
	}
}

func TestSnapshot(t *testing.T) {
	t.Log("Testing util.snapshot")

	cm := &CirconusMetrics{}

	cm.resetCounters = true
	cm.counters = make(map[string]uint64)
	cm.counterFuncs = make(map[string]func() uint64)
	cm.Increment("foo")

	cm.resetGauges = true
	cm.gauges = make(map[string]string)
	cm.gaugeFuncs = make(map[string]func() int64)
	cm.Gauge("foo", 1)

	cm.resetHistograms = true
	cm.histograms = make(map[string]*Histogram)
	cm.Timing("foo", 1)

	cm.resetText = true
	cm.text = make(map[string]string)
	cm.textFuncs = make(map[string]func() string)
	cm.SetText("foo", "bar")

	if len(cm.counters) != 1 {
		t.Errorf("Expected 1, found %d", len(cm.counters))
	}

	if len(cm.gauges) != 1 {
		t.Errorf("Expected 1, found %d", len(cm.gauges))
	}

	if len(cm.histograms) != 1 {
		t.Errorf("Expected 1, found %d", len(cm.histograms))
	}

	if len(cm.text) != 1 {
		t.Errorf("Expected 1, found %d", len(cm.text))
	}

	counters, gauges, histograms, text := cm.snapshot()

	if len(counters) != 1 {
		t.Errorf("Expected 1, found %d", len(counters))
	}

	if len(gauges) != 1 {
		t.Errorf("Expected 1, found %d", len(gauges))
	}

	if len(histograms) != 1 {
		t.Errorf("Expected 1, found %d", len(histograms))
	}

	if len(text) != 1 {
		t.Errorf("Expected 1, found %d", len(text))
	}
}
