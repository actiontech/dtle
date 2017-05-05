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

	"github.com/circonus-labs/circonus-gometrics/api/config"
)

var (
	testCheckBundle = CheckBundle{
		CheckUUIDs:         []string{"abc123-a1b2-c3d4-e5f6-123abc"},
		Checks:             []string{"/check/1234"},
		CID:                "/check_bundle/1234",
		Created:            0,
		LastModified:       0,
		LastModifedBy:      "",
		ReverseConnectURLs: []string{""},
		Config:             map[config.Key]string{},
		Brokers:            []string{"/broker/1234"},
		DisplayName:        "test check",
		Metrics:            []CheckBundleMetric{},
		MetricLimit:        0,
		Notes:              nil,
		Period:             60,
		Status:             "active",
		Target:             "127.0.0.1",
		Timeout:            10,
		Type:               "httptrap",
		Tags:               []string{},
	}
)

func testCheckBundleServer() *httptest.Server {
	f := func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/check_bundle/1234" {
			switch r.Method {
			case "PUT": // update
				defer r.Body.Close()
				b, err := ioutil.ReadAll(r.Body)
				if err != nil {
					panic(err)
				}
				w.WriteHeader(200)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, string(b))
			case "GET": // get by id/cid
				ret, err := json.Marshal(testCheckBundle)
				if err != nil {
					panic(err)
				}
				w.WriteHeader(200)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, string(ret))
			case "DELETE": // delete
				w.WriteHeader(200)
				fmt.Fprintln(w, "")
			default:
				w.WriteHeader(404)
				fmt.Fprintln(w, fmt.Sprintf("not found: %s %s", r.Method, path))
			}
		} else if path == "/check_bundle" {
			switch r.Method {
			case "GET":
				reqURL := r.URL.String()
				var c []CheckBundle
				if reqURL == "/check_bundle?search=test" {
					c = []CheckBundle{testCheckBundle}
				} else if reqURL == "/check_bundle?f__tags_has=cat%3Atag" {
					c = []CheckBundle{testCheckBundle}
				} else if reqURL == "/check_bundle?f__tags_has=cat%3Atag&search=test" {
					c = []CheckBundle{testCheckBundle}
				} else if reqURL == "/check_bundle" {
					c = []CheckBundle{testCheckBundle}
				} else {
					c = []CheckBundle{}
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
			case "POST": // create
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

func TestNewCheckBundle(t *testing.T) {
	bundle := NewCheckBundle()
	actualType := reflect.TypeOf(bundle)
	expectedType := "*api.CheckBundle"
	if actualType.String() != expectedType {
		t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
	}
}

func TestFetchCheckBundle(t *testing.T) {
	server := testCheckBundleServer()
	defer server.Close()

	ac := &Config{
		TokenKey: "abc123",
		TokenApp: "test",
		URL:      server.URL,
	}
	apih, err := New(ac)
	if err != nil {
		t.Errorf("Expected no error, got '%v'", err)
	}

	t.Log("invalid CID [nil]")
	{
		expectedError := errors.New("Invalid check bundle CID [none]")
		_, err := apih.FetchCheckBundle(nil)
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
		expectedError := errors.New("Invalid check bundle CID [none]")
		_, err := apih.FetchCheckBundle(CIDType(&cid))
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
		expectedError := errors.New("Invalid check bundle CID [/invalid]")
		_, err := apih.FetchCheckBundle(CIDType(&cid))
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID")
	{
		cid := CIDType(&testCheckBundle.CID)
		bundle, err := apih.FetchCheckBundle(cid)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(bundle)
		expectedType := "*api.CheckBundle"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}

		if bundle.CID != testCheckBundle.CID {
			t.Fatalf("CIDs do not match: %+v != %+v\n", bundle, testCheckBundle)
		}
	}
}

func TestUpdateCheckBundle(t *testing.T) {
	server := testCheckBundleServer()
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

	t.Log("invalid config [nil]")
	{
		expectedError := errors.New("Invalid check bundle config [nil]")
		_, err = apih.UpdateCheckBundle(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid config [CID /invalid]")
	{
		expectedError := errors.New("Invalid check bundle CID [/invalid]")
		x := &CheckBundle{CID: "/invalid"}
		_, err = apih.UpdateCheckBundle(x)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		bundle, err := apih.UpdateCheckBundle(&testCheckBundle)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(bundle)
		expectedType := "*api.CheckBundle"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}

func TestCreateCheckBundle(t *testing.T) {
	server := testCheckBundleServer()
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

	t.Log("invalid config [nil]")
	{
		expectedError := errors.New("Invalid check bundle config [nil]")
		_, err = apih.CreateCheckBundle(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		bundle, err := apih.CreateCheckBundle(&testCheckBundle)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(bundle)
		expectedType := "*api.CheckBundle"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}

func TestDeleteCheckBundle(t *testing.T) {
	server := testCheckBundleServer()
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

	t.Log("invalid config [nil]")
	{
		expectedError := errors.New("Invalid check bundle config [nil]")
		_, err := apih.DeleteCheckBundle(nil)
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid config [CID /invalid]")
	{
		cb := &CheckBundle{CID: "/invalid"}
		expectedError := errors.New("Invalid check bundle CID [/invalid]")
		_, err := apih.DeleteCheckBundle(cb)
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		success, err := apih.DeleteCheckBundle(&testCheckBundle)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		if !success {
			t.Fatalf("Expected success to be true")
		}
	}
}

func TestDeleteCheckBundleByCID(t *testing.T) {
	server := testCheckBundleServer()
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
		expectedError := errors.New("Invalid check bundle CID [none]")
		_, err := apih.DeleteCheckBundleByCID(nil)
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
		expectedError := errors.New("Invalid check bundle CID [none]")
		_, err := apih.DeleteCheckBundleByCID(CIDType(&cid))
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
		expectedError := errors.New("Invalid check bundle CID [/invalid]")
		_, err := apih.DeleteCheckBundleByCID(CIDType(&cid))
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID")
	{
		cid := CIDType(&testCheckBundle.CID)
		success, err := apih.DeleteCheckBundleByCID(cid)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		if !success {
			t.Fatalf("Expected success to be true")
		}
	}
}

func TestSearchCheckBundles(t *testing.T) {
	server := testCheckBundleServer()
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

	t.Log("no search, no filter")
	{
		bundles, err := apih.SearchCheckBundles(nil, nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(bundles)
		expectedType := "*[]api.CheckBundle"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("search, no filter")
	{
		search := SearchQueryType("test")
		bundles, err := apih.SearchCheckBundles(&search, nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(bundles)
		expectedType := "*[]api.CheckBundle"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("no search, filter")
	{
		filter := map[string][]string{"f__tags_has": {"cat:tag"}}
		bundles, err := apih.SearchCheckBundles(nil, &filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(bundles)
		expectedType := "*[]api.CheckBundle"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("search, filter")
	{
		search := SearchQueryType("test")
		filter := map[string][]string{"f__tags_has": {"cat:tag"}}
		bundles, err := apih.SearchCheckBundles(&search, &filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(bundles)
		expectedType := "*[]api.CheckBundle"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}
