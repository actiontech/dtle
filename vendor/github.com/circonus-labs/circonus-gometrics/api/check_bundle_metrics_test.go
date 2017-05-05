// Copyright 2016 Circonus, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

var (
	testCheckBundleMetrics = CheckBundleMetrics{
		CID: "/check_bundle_metrics/1234",
		Metrics: []CheckBundleMetric{
			{Name: "foo", Type: "numeric", Status: "active"},
			{Name: "bar", Type: "histogram", Status: "active"},
			{Name: "baz", Type: "text", Status: "available"},
			{Name: "fum", Type: "composite", Status: "active", Tags: []string{"cat:tag"}},
			{Name: "zot", Type: "caql", Status: "active", Units: &[]string{"milliseconds"}[0]},
		},
	}
)

func testCheckBundleMetricsServer() *httptest.Server {
	f := func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/check_bundle_metrics/1234" {
			switch r.Method {
			case "GET":
				ret, err := json.Marshal(testCheckBundleMetrics)
				if err != nil {
					panic(err)
				}
				w.WriteHeader(200)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, string(ret))
			case "PUT":
				defer r.Body.Close()
				b, err := ioutil.ReadAll(r.Body)
				if err != nil {
					panic(err)
				}
				w.WriteHeader(200)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, string(b))
			default:
				w.WriteHeader(404)
				fmt.Fprintln(w, fmt.Sprintf("not found: %s %s", r.Method, path))
			}
		} else {
			w.WriteHeader(404)
			fmt.Fprintln(w, fmt.Sprintf("not found: %s %s", r.Method, path))
		}
	}

	return httptest.NewServer(http.HandlerFunc(f))
}

func TestFetchCheckBundleMetrics(t *testing.T) {
	server := testCheckBundleMetricsServer()
	defer server.Close()

	ac := &Config{
		TokenKey: "abc123",
		TokenApp: "test",
		URL:      server.URL,
	}
	apih, err := NewAPI(ac)
	if err != nil {
		t.Errorf("Expected no error, got '%v'", err)
	}

	t.Log("invalid CID [nil]")
	{
		expectedError := errors.New("Invalid check bundle metrics CID [none]")
		_, err := apih.FetchCheckBundleMetrics(nil)
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid CID [\"\"]")
	{
		cid := ""
		expectedError := errors.New("Invalid check bundle metrics CID [none]")
		_, err := apih.FetchCheckBundleMetrics(CIDType(&cid))
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid CID [/invalid]")
	{
		cid := "/invalid"
		expectedError := errors.New("Invalid check bundle metrics CID [/invalid]")
		_, err := apih.FetchCheckBundleMetrics(CIDType(&cid))
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID")
	{
		cid := "/check_bundle_metrics/1234"
		metrics, err := apih.FetchCheckBundleMetrics(CIDType(&cid))
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(metrics)
		expectedType := "*api.CheckBundleMetrics"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}

		if metrics.CID != testCheckBundleMetrics.CID {
			t.Fatalf("CIDs do not match: %+v != %+v\n", metrics, testCheckBundleMetrics)
		}
	}
}

func TestUpdateCheckBundleMetrics(t *testing.T) {
	server := testCheckBundleMetricsServer()
	defer server.Close()

	var apih *API

	ac := &Config{
		TokenKey: "abc123",
		TokenApp: "test",
		URL:      server.URL,
	}
	apih, err := NewAPI(ac)
	if err != nil {
		t.Errorf("Expected no error, got '%v'", err)
	}

	t.Log("invalid config [nil]")
	{
		expectedError := errors.New("Invalid check bundle metrics config [nil]")
		_, err := apih.UpdateCheckBundleMetrics(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid config [CID /invalid]")
	{
		expectedError := errors.New("Invalid check bundle metrics CID [/invalid]")
		x := &CheckBundleMetrics{CID: "/invalid"}
		_, err := apih.UpdateCheckBundleMetrics(x)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		metrics, err := apih.UpdateCheckBundleMetrics(&testCheckBundleMetrics)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(metrics)
		expectedType := "*api.CheckBundleMetrics"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}
