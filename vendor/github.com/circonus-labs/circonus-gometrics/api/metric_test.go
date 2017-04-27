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
	testMetric = Metric{
		CID:            "/metric/1234_foo",
		Active:         true,
		CheckCID:       "/check/1234",
		CheckActive:    true,
		CheckBundleCID: "/check_bundle/1234",
		CheckTags:      []string{"cat:tag"},
		CheckUUID:      "",
		Histogram:      false,
		MetricName:     "foo",
		MetricType:     "numeric",
		Tags:           []string{"cat1:tag1"},
		Units:          &[]string{"light years"}[0],
	}
)

func testMetricServer() *httptest.Server {
	f := func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/metric/1234_foo" {
			switch r.Method {
			case "GET":
				ret, err := json.Marshal(testMetric)
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
		} else if path == "/metric" {
			switch r.Method {
			case "GET":
				reqURL := r.URL.String()
				var c []Metric
				if reqURL == "/metric?search=vm%60memory%60used" {
					c = []Metric{testMetric}
				} else if reqURL == "/metric?f_tags_has=service%3Acache" {
					c = []Metric{testMetric}
				} else if reqURL == "/metric?f_tags_has=service%3Acache&search=vm%60memory%60used" {
					c = []Metric{testMetric}
				} else if reqURL == "/metric" {
					c = []Metric{testMetric}
				} else {
					c = []Metric{}
				}
				if len(c) > 0 {
					ret, err := json.Marshal(c)
					if err != nil {
						panic(err)
					}
					w.WriteHeader(200)
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprintln(w, string(ret))
				} else {
					w.WriteHeader(404)
					fmt.Fprintln(w, fmt.Sprintf("not found: %s %s", r.Method, reqURL))
				}
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

func TestFetchMetric(t *testing.T) {
	server := testMetricServer()
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

	t.Log("without CID")
	{
		cid := ""
		expectedError := errors.New("Invalid metric CID [none]")
		_, err := apih.FetchMetric(CIDType(&cid))
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("with valid CID")
	{
		cid := "/metric/1234_foo"
		metric, err := apih.FetchMetric(CIDType(&cid))
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(metric)
		expectedType := "*api.Metric"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}

		if metric.CID != testMetric.CID {
			t.Fatalf("CIDs do not match: %+v != %+v\n", metric, testMetric)
		}
	}

	t.Log("with invalid CID")
	{
		cid := "/invalid"
		expectedError := errors.New("Invalid metric CID [/invalid]")
		_, err := apih.FetchMetric(CIDType(&cid))
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}
}

func TestFetchMetrics(t *testing.T) {
	server := testMetricServer()
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

	metrics, err := apih.FetchMetrics()
	if err != nil {
		t.Fatalf("Expected no error, got '%v'", err)
	}

	actualType := reflect.TypeOf(metrics)
	expectedType := "*[]api.Metric"
	if actualType.String() != expectedType {
		t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
	}

}

func TestUpdateMetric(t *testing.T) {
	server := testMetricServer()
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

	t.Log("valid Metric")
	{
		metric, err := apih.UpdateMetric(&testMetric)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(metric)
		expectedType := "*api.Metric"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("Test with invalid CID")
	{
		expectedError := errors.New("Invalid metric CID [/invalid]")
		x := &Metric{CID: "/invalid"}
		_, err := apih.UpdateMetric(x)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}
}

func TestSearchMetrics(t *testing.T) {
	server := testMetricServer()
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

	search := SearchQueryType("vm`memory`used")
	filter := SearchFilterType(map[string][]string{"f_tags_has": []string{"service:cache"}})

	t.Log("no search, no filter")
	{
		metrics, err := apih.SearchMetrics(nil, nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(metrics)
		expectedType := "*[]api.Metric"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("search, no filter")
	{
		metrics, err := apih.SearchMetrics(&search, nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(metrics)
		expectedType := "*[]api.Metric"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("no search, filter")
	{
		metrics, err := apih.SearchMetrics(nil, &filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(metrics)
		expectedType := "*[]api.Metric"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("search, filter")
	{
		metrics, err := apih.SearchMetrics(&search, &filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(metrics)
		expectedType := "*[]api.Metric"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}
