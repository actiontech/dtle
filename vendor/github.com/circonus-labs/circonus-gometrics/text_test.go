// Copyright 2016 Circonus, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package circonusgometrics

import (
	"testing"
)

func TestSetText(t *testing.T) {
	t.Log("Testing gauge.SetText")

	cm := &CirconusMetrics{}
	cm.text = make(map[string]string)
	cm.SetText("foo", "bar")

	val, ok := cm.text["foo"]
	if !ok {
		t.Errorf("Expected to find foo")
	}

	if val != "bar" {
		t.Errorf("Expected 'bar', found '%s'", val)
	}
}

func TestSetTextValue(t *testing.T) {
	t.Log("Testing gauge.SetTextValue")

	cm := &CirconusMetrics{}
	cm.text = make(map[string]string)
	cm.SetTextValue("foo", "bar")

	val, ok := cm.text["foo"]
	if !ok {
		t.Errorf("Expected to find foo")
	}

	if val != "bar" {
		t.Errorf("Expected 'bar', found '%s'", val)
	}
}

func TestRemoveText(t *testing.T) {
	t.Log("Testing text.RemoveText")

	cm := &CirconusMetrics{}
	cm.text = make(map[string]string)
	cm.SetText("foo", "bar")

	val, ok := cm.text["foo"]
	if !ok {
		t.Errorf("Expected to find foo")
	}

	if val != "bar" {
		t.Errorf("Expected 'bar', found '%s'", val)
	}

	cm.RemoveText("foo")

	val, ok = cm.text["foo"]
	if ok {
		t.Errorf("Expected NOT to find foo")
	}

	if val != "" {
		t.Errorf("Expected '', found '%s'", val)
	}
}

func TestSetTextFunc(t *testing.T) {
	t.Log("Testing text.SetTextFunc")

	tf := func() string {
		return "bar"
	}
	cm := &CirconusMetrics{}
	cm.textFuncs = make(map[string]func() string)
	cm.SetTextFunc("foo", tf)

	val, ok := cm.textFuncs["foo"]
	if !ok {
		t.Errorf("Expected to find foo")
	}

	if val() != "bar" {
		t.Errorf("Expected 'bar', found '%s'", val())
	}
}

func TestRemoveTextFunc(t *testing.T) {
	t.Log("Testing text.RemoveTextFunc")

	tf := func() string {
		return "bar"
	}
	cm := &CirconusMetrics{}
	cm.textFuncs = make(map[string]func() string)
	cm.SetTextFunc("foo", tf)

	val, ok := cm.textFuncs["foo"]
	if !ok {
		t.Errorf("Expected to find foo")
	}

	if val() != "bar" {
		t.Errorf("Expected 'bar', found '%s'", val())
	}

	cm.RemoveTextFunc("foo")

	val, ok = cm.textFuncs["foo"]
	if ok {
		t.Errorf("Expected NOT to find foo")
	}

	if val != nil {
		t.Errorf("Expected nil, found %s", val())
	}

}
