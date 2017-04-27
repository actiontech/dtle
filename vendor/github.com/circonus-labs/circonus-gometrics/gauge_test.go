// Copyright 2016 Circonus, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package circonusgometrics

import (
	"testing"
)

func TestGauge(t *testing.T) {
	t.Log("int")
	{
		cm := &CirconusMetrics{gauges: make(map[string]string)}

		cm.Gauge("foo", int(1))
		val, ok := cm.gauges["foo"]
		if !ok {
			t.Errorf("Expected to find foo")
		}

		if val != "1" {
			t.Errorf("Expected 1, found %s", val)
		}
	}

	t.Log("int8")
	{
		cm := &CirconusMetrics{gauges: make(map[string]string)}

		cm.Gauge("foo", int8(1))
		val, ok := cm.gauges["foo"]
		if !ok {
			t.Errorf("Expected to find foo")
		}

		if val != "1" {
			t.Errorf("Expected 1, found %s", val)
		}
	}

	t.Log("int16")
	{
		cm := &CirconusMetrics{gauges: make(map[string]string)}

		cm.Gauge("foo", int16(1))
		val, ok := cm.gauges["foo"]
		if !ok {
			t.Errorf("Expected to find foo")
		}

		if val != "1" {
			t.Errorf("Expected 1, found %s", val)
		}
	}

	t.Log("int32")
	{
		cm := &CirconusMetrics{gauges: make(map[string]string)}

		cm.Gauge("foo", int32(1))
		val, ok := cm.gauges["foo"]
		if !ok {
			t.Errorf("Expected to find foo")
		}

		if val != "1" {
			t.Errorf("Expected 1, found %s", val)
		}
	}

	t.Log("int64")
	{
		cm := &CirconusMetrics{gauges: make(map[string]string)}

		cm.Gauge("foo", int64(1))
		val, ok := cm.gauges["foo"]
		if !ok {
			t.Errorf("Expected to find foo")
		}

		if val != "1" {
			t.Errorf("Expected 1, found %s", val)
		}
	}

	t.Log("uint")
	{
		cm := &CirconusMetrics{gauges: make(map[string]string)}

		cm.Gauge("foo", uint(1))
		val, ok := cm.gauges["foo"]
		if !ok {
			t.Errorf("Expected to find foo")
		}

		if val != "1" {
			t.Errorf("Expected 1, found %s", val)
		}
	}

	t.Log("uint8")
	{
		cm := &CirconusMetrics{gauges: make(map[string]string)}

		cm.Gauge("foo", uint8(1))
		val, ok := cm.gauges["foo"]
		if !ok {
			t.Errorf("Expected to find foo")
		}

		if val != "1" {
			t.Errorf("Expected 1, found %s", val)
		}
	}

	t.Log("uint16")
	{
		cm := &CirconusMetrics{gauges: make(map[string]string)}

		cm.Gauge("foo", uint16(1))
		val, ok := cm.gauges["foo"]
		if !ok {
			t.Errorf("Expected to find foo")
		}

		if val != "1" {
			t.Errorf("Expected 1, found %s", val)
		}
	}

	t.Log("uint32")
	{
		cm := &CirconusMetrics{gauges: make(map[string]string)}

		cm.Gauge("foo", uint32(1))
		val, ok := cm.gauges["foo"]
		if !ok {
			t.Errorf("Expected to find foo")
		}

		if val != "1" {
			t.Errorf("Expected 1, found %s", val)
		}
	}

	t.Log("uint64")
	{
		cm := &CirconusMetrics{gauges: make(map[string]string)}

		cm.Gauge("foo", uint64(1))
		val, ok := cm.gauges["foo"]
		if !ok {
			t.Errorf("Expected to find foo")
		}

		if val != "1" {
			t.Errorf("Expected 1, found %s", val)
		}
	}

	t.Log("float32")
	{
		cm := &CirconusMetrics{gauges: make(map[string]string)}

		cm.Gauge("foo", float32(3.12))
		val, ok := cm.gauges["foo"]
		if !ok {
			t.Errorf("Expected to find foo")
		}

		if val[0:4] != "3.12" {
			t.Errorf("Expected 3.12, found %s", val)
		}
	}

	t.Log("float64")
	{
		cm := &CirconusMetrics{gauges: make(map[string]string)}

		cm.Gauge("foo", float64(3.12))
		val, ok := cm.gauges["foo"]
		if !ok {
			t.Errorf("Expected to find foo")
		}

		if val[0:4] != "3.12" {
			t.Errorf("Expected 3.12, found %s", val)
		}
	}
}

func TestSetGauge(t *testing.T) {
	t.Log("Testing gauge.SetGauge")

	cm := &CirconusMetrics{gauges: make(map[string]string)}

	cm.SetGauge("foo", 10)

	val, ok := cm.gauges["foo"]
	if !ok {
		t.Errorf("Expected to find foo")
	}

	if val != "10" {
		t.Errorf("Expected 10, found %s", val)
	}
}

func TestRemoveGauge(t *testing.T) {
	t.Log("Testing gauge.RemoveGauge")

	cm := &CirconusMetrics{gauges: make(map[string]string)}

	cm.Gauge("foo", 5)

	val, ok := cm.gauges["foo"]
	if !ok {
		t.Errorf("Expected to find foo")
	}

	if val != "5" {
		t.Errorf("Expected 5, found %s", val)
	}

	cm.RemoveGauge("foo")

	val, ok = cm.gauges["foo"]
	if ok {
		t.Errorf("Expected NOT to find foo")
	}

	if val != "" {
		t.Errorf("Expected '', found '%s'", val)
	}
}

func TestSetGaugeFunc(t *testing.T) {
	t.Log("Testing gauge.SetGaugeFunc")

	gf := func() int64 {
		return 1
	}

	cm := &CirconusMetrics{gaugeFuncs: make(map[string]func() int64)}

	cm.SetGaugeFunc("foo", gf)

	val, ok := cm.gaugeFuncs["foo"]
	if !ok {
		t.Errorf("Expected to find foo")
	}

	if val() != 1 {
		t.Errorf("Expected 1, found %d", val())
	}
}

func TestRemoveGaugeFunc(t *testing.T) {
	t.Log("Testing gauge.RemoveGaugeFunc")

	gf := func() int64 {
		return 1
	}

	cm := &CirconusMetrics{gaugeFuncs: make(map[string]func() int64)}

	cm.SetGaugeFunc("foo", gf)

	val, ok := cm.gaugeFuncs["foo"]
	if !ok {
		t.Errorf("Expected to find foo")
	}

	if val() != 1 {
		t.Errorf("Expected 1, found %d", val())
	}

	cm.RemoveGaugeFunc("foo")

	val, ok = cm.gaugeFuncs["foo"]
	if ok {
		t.Errorf("Expected NOT to find foo")
	}

	if val != nil {
		t.Errorf("Expected nil, found %v", val())
	}

}
