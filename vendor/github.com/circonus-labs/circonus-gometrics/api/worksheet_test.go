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
	testWorksheet = Worksheet{
		CID:         "/worksheet/01234567-89ab-cdef-0123-456789abcdef",
		Description: &[]string{"One graph per active server in our primary data center"}[0],
		Favorite:    true,
		Graphs: []WorksheetGraph{
			{GraphCID: "/graph/aaaaaaaa-0000-1111-2222-0123456789ab"},
			{GraphCID: "/graph/bbbbbbbb-3333-4444-5555-0123456789ab"},
			{GraphCID: "/graph/cccccccc-6666-7777-8888-0123456789ab"},
		},
		Notes: &[]string{"Currently maintained by Oscar"}[0],
		SmartQueries: []WorksheetSmartQuery{
			{
				Name:  "Virtual Machines",
				Order: []string{"/graph/dddddddd-9999-aaaa-bbbb-0123456789ab"},
				Query: "virtual",
			},
		},
		Tags:  []string{"datacenter:primary"},
		Title: "Primary Datacenter Server Graphs",
	}
)

func testWorksheetServer() *httptest.Server {
	f := func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/worksheet/01234567-89ab-cdef-0123-456789abcdef" {
			switch r.Method {
			case "GET":
				ret, err := json.Marshal(testWorksheet)
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
		} else if path == "/worksheet" {
			switch r.Method {
			case "GET":
				reqURL := r.URL.String()
				var c []Worksheet
				if reqURL == "/worksheet?search=web+servers" {
					c = []Worksheet{testWorksheet}
				} else if reqURL == "/worksheet?f_favorite=true" {
					c = []Worksheet{testWorksheet}
				} else if reqURL == "/worksheet?f_favorite=true&search=web+servers" {
					c = []Worksheet{testWorksheet}
				} else if reqURL == "/worksheet" {
					c = []Worksheet{testWorksheet}
				} else {
					c = []Worksheet{}
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
				ret, err := json.Marshal(testWorksheet)
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

func TestNewWorksheet(t *testing.T) {
	bundle := NewWorksheet()
	actualType := reflect.TypeOf(bundle)
	expectedType := "*api.Worksheet"
	if actualType.String() != expectedType {
		t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
	}
}

func TestFetchWorksheet(t *testing.T) {
	server := testWorksheetServer()
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
		expectedError := errors.New("Invalid worksheet CID [none]")
		_, err := apih.FetchWorksheet(nil)
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
		expectedError := errors.New("Invalid worksheet CID [none]")
		_, err := apih.FetchWorksheet(CIDType(&cid))
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
		expectedError := errors.New("Invalid worksheet CID [/invalid]")
		_, err := apih.FetchWorksheet(CIDType(&cid))
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID")
	{
		cid := "/worksheet/01234567-89ab-cdef-0123-456789abcdef"
		worksheet, err := apih.FetchWorksheet(CIDType(&cid))
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(worksheet)
		expectedType := "*api.Worksheet"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}

		if worksheet.CID != testWorksheet.CID {
			t.Fatalf("CIDs do not match: %+v != %+v\n", worksheet, testWorksheet)
		}
	}
}

func TestFetchWorksheets(t *testing.T) {
	server := testWorksheetServer()
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

	worksheets, err := apih.FetchWorksheets()
	if err != nil {
		t.Fatalf("Expected no error, got '%v'", err)
	}

	actualType := reflect.TypeOf(worksheets)
	expectedType := "*[]api.Worksheet"
	if actualType.String() != expectedType {
		t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
	}

}

func TestUpdateWorksheet(t *testing.T) {
	server := testWorksheetServer()
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
		expectedError := errors.New("Invalid worksheet config [nil]")
		_, err := apih.UpdateWorksheet(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid config [CID /invalid]")
	{
		expectedError := errors.New("Invalid worksheet CID [/invalid]")
		x := &Worksheet{CID: "/invalid"}
		_, err := apih.UpdateWorksheet(x)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		worksheet, err := apih.UpdateWorksheet(&testWorksheet)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(worksheet)
		expectedType := "*api.Worksheet"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

}

func TestCreateWorksheet(t *testing.T) {
	server := testWorksheetServer()
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
		expectedError := errors.New("Invalid worksheet config [nil]")
		_, err := apih.CreateWorksheet(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		worksheet, err := apih.CreateWorksheet(&testWorksheet)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(worksheet)
		expectedType := "*api.Worksheet"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}

func TestDeleteWorksheet(t *testing.T) {
	server := testWorksheetServer()
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
		expectedError := errors.New("Invalid worksheet config [nil]")
		_, err := apih.DeleteWorksheet(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid config [CID /invalid]")
	{
		expectedError := errors.New("Invalid worksheet CID [/invalid]")
		x := &Worksheet{CID: "/invalid"}
		_, err := apih.DeleteWorksheet(x)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		_, err := apih.DeleteWorksheet(&testWorksheet)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}
	}

}

func TestDeleteWorksheetByCID(t *testing.T) {
	server := testWorksheetServer()
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
		expectedError := errors.New("Invalid worksheet CID [none]")
		_, err := apih.DeleteWorksheetByCID(nil)
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
		expectedError := errors.New("Invalid worksheet CID [none]")
		_, err := apih.DeleteWorksheetByCID(CIDType(&cid))
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
		expectedError := errors.New("Invalid worksheet CID [/invalid]")
		_, err := apih.DeleteWorksheetByCID(CIDType(&cid))
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID")
	{
		cid := "/worksheet/01234567-89ab-cdef-0123-456789abcdef"
		_, err := apih.DeleteWorksheetByCID(CIDType(&cid))
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}
	}
}

func TestSearchWorksheets(t *testing.T) {
	server := testWorksheetServer()
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

	search := SearchQueryType("web servers")
	filter := SearchFilterType(map[string][]string{"f_favorite": {"true"}})

	t.Log("no search, no filter")
	{
		worksheets, err := apih.SearchWorksheets(nil, nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(worksheets)
		expectedType := "*[]api.Worksheet"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("search, no filter")
	{
		worksheets, err := apih.SearchWorksheets(&search, nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(worksheets)
		expectedType := "*[]api.Worksheet"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("no search, filter")
	{
		worksheets, err := apih.SearchWorksheets(nil, &filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(worksheets)
		expectedType := "*[]api.Worksheet"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("search, filter")
	{
		worksheets, err := apih.SearchWorksheets(&search, &filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(worksheets)
		expectedType := "*[]api.Worksheet"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}
