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
	testRuleSet = RuleSet{
		CID:      "/rule_set/1234_tt_firstbyte",
		CheckCID: "/check/1234",
		ContactGroups: map[uint8][]string{
			1: {"/contact_group/1234", "/contact_group/5678"},
			2: {"/contact_group/1234"},
			3: {"/contact_group/1234"},
			4: {},
			5: {},
		},
		Derive:     nil,
		Link:       &[]string{"http://example.com/how2fix/webserver_down/"}[0],
		MetricName: "tt_firstbyte",
		MetricType: "numeric",
		Notes:      &[]string{"Determine if the HTTP request is taking too long to start (or is down.)  Don't fire if ping is already alerting"}[0],
		Parent:     &[]string{"1233_ping"}[0],
		Rules: []RuleSetRule{
			{
				Criteria:          "on absence",
				Severity:          1,
				Value:             "300",
				Wait:              5,
				WindowingDuration: 300,
				WindowingFunction: nil,
			},
			{
				Criteria: "max value",
				Severity: 2,
				Value:    "1000",
				Wait:     5,
			},
		},
	}
)

func testRuleSetServer() *httptest.Server {
	f := func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/rule_set/1234_tt_firstbyte" {
			switch r.Method {
			case "GET":
				ret, err := json.Marshal(testRuleSet)
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
		} else if path == "/rule_set" {
			switch r.Method {
			case "GET":
				reqURL := r.URL.String()
				var c []RuleSet
				if reqURL == "/rule_set?search=request%60latency_ms" {
					c = []RuleSet{testRuleSet}
				} else if reqURL == "/rule_set?f_tags_has=service%3Aweb" {
					c = []RuleSet{testRuleSet}
				} else if reqURL == "/rule_set?f_tags_has=service%3Aweb&search=request%60latency_ms" {
					c = []RuleSet{testRuleSet}
				} else if reqURL == "/rule_set" {
					c = []RuleSet{testRuleSet}
				} else {
					c = []RuleSet{}
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
				ret, err := json.Marshal(testRuleSet)
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

func TestNewRuleSet(t *testing.T) {
	bundle := NewRuleSet()
	actualType := reflect.TypeOf(bundle)
	expectedType := "*api.RuleSet"
	if actualType.String() != expectedType {
		t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
	}
}

func TestFetchRuleSet(t *testing.T) {
	server := testRuleSetServer()
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
		expectedError := errors.New("Invalid rule set CID [none]")
		_, err := apih.FetchRuleSet(nil)
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
		expectedError := errors.New("Invalid rule set CID [none]")
		_, err := apih.FetchRuleSet(CIDType(&cid))
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
		expectedError := errors.New("Invalid rule set CID [/invalid]")
		_, err := apih.FetchRuleSet(CIDType(&cid))
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID")
	{
		cid := "/rule_set/1234_tt_firstbyte"
		ruleset, err := apih.FetchRuleSet(CIDType(&cid))
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(ruleset)
		expectedType := "*api.RuleSet"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}

		if ruleset.CID != testRuleSet.CID {
			t.Fatalf("CIDs do not match: %+v != %+v\n", ruleset, testRuleSet)
		}
	}
}

func TestFetchRuleSets(t *testing.T) {
	server := testRuleSetServer()
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

	rulesets, err := apih.FetchRuleSets()
	if err != nil {
		t.Fatalf("Expected no error, got '%v'", err)
	}

	actualType := reflect.TypeOf(rulesets)
	expectedType := "*[]api.RuleSet"
	if actualType.String() != expectedType {
		t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
	}
}

func TestUpdateRuleSet(t *testing.T) {
	server := testRuleSetServer()
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
		expectedError := errors.New("Invalid rule set config [nil]")
		_, err := apih.UpdateRuleSet(nil)
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid config [CID /invalid]")
	{
		expectedError := errors.New("Invalid rule set CID [/invalid]")
		x := &RuleSet{CID: "/invalid"}
		_, err := apih.UpdateRuleSet(x)
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		ruleset, err := apih.UpdateRuleSet(&testRuleSet)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(ruleset)
		expectedType := "*api.RuleSet"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}

func TestCreateRuleSet(t *testing.T) {
	server := testRuleSetServer()
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
		expectedError := errors.New("Invalid rule set config [nil]")
		_, err := apih.CreateRuleSet(nil)
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		ruleset, err := apih.CreateRuleSet(&testRuleSet)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(ruleset)
		expectedType := "*api.RuleSet"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}

func TestDeleteRuleSet(t *testing.T) {
	server := testRuleSetServer()
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
		expectedError := errors.New("Invalid rule set config [nil]")
		_, err := apih.DeleteRuleSet(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid config [CID /invalid]")
	{
		expectedError := errors.New("Invalid rule set CID [/invalid]")
		x := &RuleSet{CID: "/invalid"}
		_, err := apih.DeleteRuleSet(x)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		_, err := apih.DeleteRuleSet(&testRuleSet)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}
	}
}

func TestDeleteRuleSetByCID(t *testing.T) {
	server := testRuleSetServer()
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
		expectedError := errors.New("Invalid rule set CID [none]")
		_, err := apih.DeleteRuleSetByCID(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid CID [\"\"]")
	{
		expectedError := errors.New("Invalid rule set CID [none]")
		cid := ""
		_, err := apih.DeleteRuleSetByCID(CIDType(&cid))
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid CID [/invalid]")
	{
		expectedError := errors.New("Invalid rule set CID [/invalid]")
		cid := "/invalid"
		_, err := apih.DeleteRuleSetByCID(CIDType(&cid))
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID")
	{
		cid := "/rule_set/1234_tt_firstbyte"
		_, err := apih.DeleteRuleSetByCID(CIDType(&cid))
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}
	}
}

func TestSearchRuleSets(t *testing.T) {
	server := testRuleSetServer()
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

	search := SearchQueryType("request`latency_ms")
	filter := SearchFilterType(map[string][]string{"f_tags_has": {"service:web"}})

	t.Log("no search, no filter")
	{
		rulesets, err := apih.SearchRuleSets(nil, nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(rulesets)
		expectedType := "*[]api.RuleSet"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("search, no filter")
	{
		rulesets, err := apih.SearchRuleSets(&search, nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(rulesets)
		expectedType := "*[]api.RuleSet"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("no search, filter")
	{
		rulesets, err := apih.SearchRuleSets(nil, &filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(rulesets)
		expectedType := "*[]api.RuleSet"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("search, filter")
	{
		rulesets, err := apih.SearchRuleSets(&search, &filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(rulesets)
		expectedType := "*[]api.RuleSet"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}
