// Copyright 2016 Circonus, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package circonusgometrics

import (
	"testing"
)

func TestSet(t *testing.T) {
	t.Log("Testing counter.Set")

	cm := &CirconusMetrics{counters: make(map[string]uint64)}

	cm.Set("foo", 30)

	val, ok := cm.counters["foo"]
	if !ok {
		t.Errorf("Expected to find foo")
	}

	if val != 30 {
		t.Errorf("Expected 30, found %d", val)
	}

	cm.Set("foo", 10)

	val, ok = cm.counters["foo"]
	if !ok {
		t.Errorf("Expected to find foo")
	}

	if val != 10 {
		t.Errorf("Expected 10, found %d", val)
	}
}

func TestIncrement(t *testing.T) {
	t.Log("Testing counter.Increment")

	cm := &CirconusMetrics{counters: make(map[string]uint64)}

	cm.Increment("foo")

	val, ok := cm.counters["foo"]
	if !ok {
		t.Errorf("Expected to find foo")
	}

	if val != 1 {
		t.Errorf("Expected 1, found %d", val)
	}
}

func TestIncrementByValue(t *testing.T) {
	t.Log("Testing counter.IncrementByValue")

	cm := &CirconusMetrics{counters: make(map[string]uint64)}

	cm.IncrementByValue("foo", 10)

	val, ok := cm.counters["foo"]
	if !ok {
		t.Errorf("Expected to find foo")
	}

	if val != 10 {
		t.Errorf("Expected 1, found %d", val)
	}
}

func TestAdd(t *testing.T) {
	t.Log("Testing counter.Add")

	cm := &CirconusMetrics{counters: make(map[string]uint64)}

	cm.Add("foo", 5)

	val, ok := cm.counters["foo"]
	if !ok {
		t.Errorf("Expected to find foo")
	}

	if val != 5 {
		t.Errorf("Expected 1, found %d", val)
	}
}

func TestRemoveCounter(t *testing.T) {
	t.Log("Testing counter.RemoveCounter")

	cm := &CirconusMetrics{counters: make(map[string]uint64)}

	cm.Increment("foo")

	val, ok := cm.counters["foo"]
	if !ok {
		t.Errorf("Expected to find foo")
	}

	if val != 1 {
		t.Errorf("Expected 1, found %d", val)
	}

	cm.RemoveCounter("foo")

	val, ok = cm.counters["foo"]
	if ok {
		t.Errorf("Expected NOT to find foo")
	}

	if val != 0 {
		t.Errorf("Expected 0, found %d", val)
	}
}

func TestSetCounterFunc(t *testing.T) {
	t.Log("Testing counter.SetCounterFunc")

	cf := func() uint64 {
		return 1
	}

	cm := &CirconusMetrics{counterFuncs: make(map[string]func() uint64)}

	cm.SetCounterFunc("foo", cf)

	val, ok := cm.counterFuncs["foo"]
	if !ok {
		t.Errorf("Expected to find foo")
	}

	if val() != 1 {
		t.Errorf("Expected 1, found %d", val())
	}
}

func TestRemoveCounterFunc(t *testing.T) {
	t.Log("Testing counter.RemoveCounterFunc")

	cf := func() uint64 {
		return 1
	}

	cm := &CirconusMetrics{counterFuncs: make(map[string]func() uint64)}

	cm.SetCounterFunc("foo", cf)

	val, ok := cm.counterFuncs["foo"]
	if !ok {
		t.Errorf("Expected to find foo")
	}

	if val() != 1 {
		t.Errorf("Expected 1, found %d", val())
	}

	cm.RemoveCounterFunc("foo")

	val, ok = cm.counterFuncs["foo"]
	if ok {
		t.Errorf("Expected NOT to find foo")
	}

	if val != nil {
		t.Errorf("Expected nil, found %v", val())
	}

}
