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
	testFormula1 = "=A-B"
	testFormula2 = "=VAL/1000"
	testGraph    = Graph{
		CID:        "/graph/01234567-89ab-cdef-0123-456789abcdef",
		AccessKeys: []GraphAccessKey{},
		Composites: []GraphComposite{
			{
				Axis:        "l",
				Color:       "#000000",
				DataFormula: &testFormula1,
				Hidden:      false,
				Name:        "Time After First Byte",
			},
		},
		Datapoints: []GraphDatapoint{
			{
				Axis:        "l",
				CheckID:     1234,
				Color:       &[]string{"#ff0000"}[0],
				DataFormula: &testFormula2,
				Derive:      "gauge",
				Hidden:      false,
				MetricName:  "duration",
				MetricType:  "numeric",
				Name:        "Total Request Time",
			},
			{
				Axis:        "l",
				CheckID:     2345,
				Color:       &[]string{"#00ff00"}[0],
				DataFormula: &testFormula2,
				Derive:      "gauge",
				Hidden:      false,
				MetricName:  "tt_firstbyte",
				MetricType:  "numeric",
				Name:        "Time Till First Byte",
			},
		},
		Description: "Time to first byte verses time to whole thing",
		LineStyle:   &[]string{"interpolated"}[0],
		LogLeftY:    &[]int{10}[0],
		Notes:       &[]string{"This graph shows just the main webserver"}[0],
		Style:       &[]string{"line"}[0],
		Tags:        []string{"datacenter:primary"},
		Title:       "Slow Webserver",
	}
)

func testGraphServer() *httptest.Server {
	f := func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/graph/01234567-89ab-cdef-0123-456789abcdef" {
			switch r.Method {
			case "GET":
				ret, err := json.Marshal(testGraph)
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
			case "DELETE":
				w.WriteHeader(200)
				w.Header().Set("Content-Type", "application/json")
			default:
				w.WriteHeader(404)
				fmt.Fprintln(w, fmt.Sprintf("not found: %s %s", r.Method, path))
			}
		} else if path == "/graph" {
			switch r.Method {
			case "GET":
				reqURL := r.URL.String()
				var c []Graph
				if reqURL == "/graph?search=CPU+Utilization" {
					c = []Graph{testGraph}
				} else if reqURL == "/graph?f__tags_has=os%3Arhel7" {
					c = []Graph{testGraph}
				} else if reqURL == "/graph?f__tags_has=os%3Arhel7&search=CPU+Utilization" {
					c = []Graph{testGraph}
				} else if reqURL == "/graph" {
					c = []Graph{testGraph}
				} else {
					c = []Graph{}
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
			case "POST":
				defer r.Body.Close()
				_, err := ioutil.ReadAll(r.Body)
				if err != nil {
					panic(err)
				}
				ret, err := json.Marshal(testGraph)
				if err != nil {
					panic(err)
				}
				w.WriteHeader(200)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, string(ret))
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

func TestNewGraph(t *testing.T) {
	bundle := NewGraph()
	actualType := reflect.TypeOf(bundle)
	expectedType := "*api.Graph"
	if actualType.String() != expectedType {
		t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
	}
}

func TestFetchGraph(t *testing.T) {
	server := testGraphServer()
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
		expectedError := errors.New("Invalid graph CID [none]")
		_, err := apih.FetchGraph(nil)
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
		expectedError := errors.New("Invalid graph CID [none]")
		_, err := apih.FetchGraph(CIDType(&cid))
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
		expectedError := errors.New("Invalid graph CID [/invalid]")
		_, err := apih.FetchGraph(CIDType(&cid))
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID")
	{
		cid := "/graph/01234567-89ab-cdef-0123-456789abcdef"
		graph, err := apih.FetchGraph(CIDType(&cid))
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(graph)
		expectedType := "*api.Graph"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}

		if graph.CID != testGraph.CID {
			t.Fatalf("CIDs do not match: %+v != %+v\n", graph, testGraph)
		}
	}

}

func TestFetchGraphs(t *testing.T) {
	server := testGraphServer()
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

	graphs, err := apih.FetchGraphs()
	if err != nil {
		t.Fatalf("Expected no error, got '%v'", err)
	}

	actualType := reflect.TypeOf(graphs)
	expectedType := "*[]api.Graph"
	if actualType.String() != expectedType {
		t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
	}

}

func TestUpdateGraph(t *testing.T) {
	server := testGraphServer()
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
		expectedError := errors.New("Invalid graph config [nil]")
		_, err := apih.UpdateGraph(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid config [CID /invalid]")
	{
		expectedError := errors.New("Invalid graph CID [/invalid]")
		x := &Graph{CID: "/invalid"}
		_, err := apih.UpdateGraph(x)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid Graph")
	{
		graph, err := apih.UpdateGraph(&testGraph)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(graph)
		expectedType := "*api.Graph"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}

func TestCreateGraph(t *testing.T) {
	server := testGraphServer()
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
		expectedError := errors.New("Invalid graph config [nil]")
		_, err := apih.CreateGraph(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		graph, err := apih.CreateGraph(&testGraph)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(graph)
		expectedType := "*api.Graph"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}

func TestDeleteGraph(t *testing.T) {
	server := testGraphServer()
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
		expectedError := errors.New("Invalid graph config [nil]")
		_, err := apih.DeleteGraph(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid config [CID /invalid]")
	{
		expectedError := errors.New("Invalid graph CID [/invalid]")
		x := &Graph{CID: "/invalid"}
		_, err := apih.DeleteGraph(x)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		_, err := apih.DeleteGraph(&testGraph)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}
	}
}

func TestDeleteGraphByCID(t *testing.T) {
	server := testGraphServer()
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

	t.Log("invalid CID [nil]")
	{
		expectedError := errors.New("Invalid graph CID [none]")
		_, err := apih.DeleteGraphByCID(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid CID [\"\"]")
	{
		cid := ""
		expectedError := errors.New("Invalid graph CID [none]")
		_, err := apih.DeleteGraphByCID(CIDType(&cid))
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid CID [/invalid]")
	{
		cid := "/invalid"
		expectedError := errors.New("Invalid graph CID [/invalid]")
		_, err := apih.DeleteGraphByCID(CIDType(&cid))
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		cid := "/graph/01234567-89ab-cdef-0123-456789abcdef"
		_, err := apih.DeleteGraphByCID(CIDType(&cid))
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}
	}
}

func TestSearchGraphs(t *testing.T) {
	server := testGraphServer()
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

	search := SearchQueryType("CPU Utilization")
	filter := SearchFilterType(map[string][]string{"f__tags_has": {"os:rhel7"}})

	t.Log("no search, no filter")
	{
		graphs, err := apih.SearchGraphs(nil, nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(graphs)
		expectedType := "*[]api.Graph"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("search, no filter")
	{
		graphs, err := apih.SearchGraphs(&search, nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(graphs)
		expectedType := "*[]api.Graph"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("no search, filter")
	{
		graphs, err := apih.SearchGraphs(nil, &filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(graphs)
		expectedType := "*[]api.Graph"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("search, filter")
	{
		graphs, err := apih.SearchGraphs(&search, &filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(graphs)
		expectedType := "*[]api.Graph"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}
