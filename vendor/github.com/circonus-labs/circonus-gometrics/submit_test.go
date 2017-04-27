// Copyright 2016 Circonus, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package circonusgometrics

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/circonus-labs/circonus-gometrics/api"
)

func fakeBroker() *httptest.Server {
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{"stats":1}`)
	}

	return httptest.NewServer(http.HandlerFunc(handler))
}

func TestSubmit(t *testing.T) {
	t.Log("Testing submit.submit")

	server := fakeBroker()
	defer server.Close()

	cfg := &Config{}
	cfg.CheckManager.Check.SubmissionURL = server.URL

	cm, err := NewCirconusMetrics(cfg)
	if err != nil {
		t.Errorf("Expected no error, got '%v'", err)
	}

	newMetrics := make(map[string]*api.CheckBundleMetric)
	output := make(map[string]interface{})
	output["foo"] = map[string]interface{}{
		"_type":  "n",
		"_value": 1,
	}
	cm.submit(output, newMetrics)
}

func TestTrapCall(t *testing.T) {
	t.Log("Testing submit.trapCall")

	server := fakeBroker()
	defer server.Close()

	cfg := &Config{}
	cfg.CheckManager.Check.SubmissionURL = server.URL

	cm, err := NewCirconusMetrics(cfg)
	if err != nil {
		t.Errorf("Expected no error, got '%v'", err)
	}

	for !cm.check.IsReady() {
		t.Log("\twaiting for cm to init")
		time.Sleep(1 * time.Second)
	}

	output := make(map[string]interface{})
	output["foo"] = map[string]interface{}{
		"_type":  "n",
		"_value": 1,
	}

	str, err := json.Marshal(output)
	if err != nil {
		t.Errorf("Expected no error, got '%v'", err)
	}

	numStats, err := cm.trapCall(str)
	if err != nil {
		t.Errorf("Expected no error, got '%v'", err)
	}

	if numStats != 1 {
		t.Errorf("Expected 1, got %d", numStats)
	}
}
