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
	testRuleSetGroup = RuleSetGroup{
		CID: "/rule_set_group/1234",
		ContactGroups: map[uint8][]string{
			1: {"/contact_group/1234", "/contact_group/5678"},
			2: {"/contact_group/1234"},
			3: {"/contact_group/1234"},
			4: {},
			5: {},
		},
		Formulas: []RuleSetGroupFormula{
			{
				Expression:    "(A and B) and not C",
				RaiseSeverity: 2,
				Wait:          0,
			},
			{
				Expression:    "3",
				RaiseSeverity: 1,
				Wait:          5,
			},
		},
		Name: "Multiple webservers gone bad",
		RuleSetConditions: []RuleSetGroupCondition{
			{
				MatchingSeverities: []string{"1", "2"},
				RuleSetCID:         "/rule_set/1234_tt_firstbyte",
			},
			{
				MatchingSeverities: []string{"1", "2"},
				RuleSetCID:         "/rule_set/5678_tt_firstbyte",
			},
			{
				MatchingSeverities: []string{"1", "2"},
				RuleSetCID:         "/rule_set/9012_tt_firstbyte",
			},
		},
	}
)

func testRuleSetGroupServer() *httptest.Server {
	f := func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/rule_set_group/1234" {
			switch r.Method {
			case "GET":
				ret, err := json.Marshal(testRuleSetGroup)
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
		} else if path == "/rule_set_group" {
			switch r.Method {
			case "GET":
				reqURL := r.URL.String()
				var c []RuleSetGroup
				if reqURL == "/rule_set_group?search=web+requests" {
					c = []RuleSetGroup{testRuleSetGroup}
				} else if reqURL == "/rule_set_group?f_tags_has=location%3Aconus" {
					c = []RuleSetGroup{testRuleSetGroup}
				} else if reqURL == "/rule_set_group?f_tags_has=location%3Aconus&search=web+requests" {
					c = []RuleSetGroup{testRuleSetGroup}
				} else if reqURL == "/rule_set_group" {
					c = []RuleSetGroup{testRuleSetGroup}
				} else {
					c = []RuleSetGroup{}
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
				ret, err := json.Marshal(testRuleSetGroup)
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

func TestNewRuleSetGroup(t *testing.T) {
	bundle := NewRuleSetGroup()
	actualType := reflect.TypeOf(bundle)
	expectedType := "*api.RuleSetGroup"
	if actualType.String() != expectedType {
		t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
	}
}

func TestFetchRuleSetGroup(t *testing.T) {
	server := testRuleSetGroupServer()
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
		expectedError := errors.New("Invalid rule set group CID [none]")
		_, err := apih.FetchRuleSetGroup(nil)
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
		expectedError := errors.New("Invalid rule set group CID [none]")
		_, err := apih.FetchRuleSetGroup(CIDType(&cid))
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
		expectedError := errors.New("Invalid rule set group CID [/invalid]")
		_, err := apih.FetchRuleSetGroup(CIDType(&cid))
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID")
	{
		cid := "/rule_set_group/1234"
		ruleset, err := apih.FetchRuleSetGroup(CIDType(&cid))
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(ruleset)
		expectedType := "*api.RuleSetGroup"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}

		if ruleset.CID != testRuleSetGroup.CID {
			t.Fatalf("CIDs do not match: %+v != %+v\n", ruleset, testRuleSetGroup)
		}
	}
}

func TestFetchRuleSetGroups(t *testing.T) {
	server := testRuleSetGroupServer()
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

	rulesets, err := apih.FetchRuleSetGroups()
	if err != nil {
		t.Fatalf("Expected no error, got '%v'", err)
	}

	actualType := reflect.TypeOf(rulesets)
	expectedType := "*[]api.RuleSetGroup"
	if actualType.String() != expectedType {
		t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
	}
}

func TestUpdateRuleSetGroup(t *testing.T) {
	server := testRuleSetGroupServer()
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
		expectedError := errors.New("Invalid rule set group config [nil]")
		_, err := apih.UpdateRuleSetGroup(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid config [CID /invalid]")
	{
		expectedError := errors.New("Invalid rule set group CID [/invalid]")
		x := &RuleSetGroup{CID: "/invalid"}
		_, err := apih.UpdateRuleSetGroup(x)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		ruleset, err := apih.UpdateRuleSetGroup(&testRuleSetGroup)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(ruleset)
		expectedType := "*api.RuleSetGroup"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}

func TestCreateRuleSetGroup(t *testing.T) {
	server := testRuleSetGroupServer()
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
		expectedError := errors.New("Invalid rule set group config [nil]")
		_, err := apih.CreateRuleSetGroup(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		ruleset, err := apih.CreateRuleSetGroup(&testRuleSetGroup)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(ruleset)
		expectedType := "*api.RuleSetGroup"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}

func TestDeleteRuleSetGroup(t *testing.T) {
	server := testRuleSetGroupServer()
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
		expectedError := errors.New("Invalid rule set group config [nil]")
		_, err := apih.DeleteRuleSetGroup(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid config [CID /invalid]")
	{
		expectedError := errors.New("Invalid rule set group CID [/invalid]")
		x := &RuleSetGroup{CID: "/invalid"}
		_, err := apih.DeleteRuleSetGroup(x)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		_, err := apih.DeleteRuleSetGroup(&testRuleSetGroup)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}
	}
}

func TestDeleteRuleSetGroupByCID(t *testing.T) {
	server := testRuleSetGroupServer()
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
		expectedError := errors.New("Invalid rule set group CID [none]")
		_, err := apih.DeleteRuleSetGroupByCID(nil)
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
		expectedError := errors.New("Invalid rule set group CID [none]")
		_, err := apih.DeleteRuleSetGroupByCID(CIDType(&cid))
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
		expectedError := errors.New("Invalid rule set group CID [/invalid]")
		_, err := apih.DeleteRuleSetGroupByCID(CIDType(&cid))
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID")
	{
		cid := "/rule_set_group/1234"
		_, err := apih.DeleteRuleSetGroupByCID(CIDType(&cid))
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}
	}
}

func TestSearchRuleSetGroups(t *testing.T) {
	server := testRuleSetGroupServer()
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

	search := SearchQueryType("web requests")
	filter := SearchFilterType(map[string][]string{"f_tags_has": {"location:conus"}})

	t.Log("no search, no filter")
	{
		groups, err := apih.SearchRuleSetGroups(nil, nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(groups)
		expectedType := "*[]api.RuleSetGroup"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("search, no filter")
	{
		groups, err := apih.SearchRuleSetGroups(&search, nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(groups)
		expectedType := "*[]api.RuleSetGroup"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("no search, filter")
	{
		groups, err := apih.SearchRuleSetGroups(nil, &filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(groups)
		expectedType := "*[]api.RuleSetGroup"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("search, filter")
	{
		groups, err := apih.SearchRuleSetGroups(&search, &filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(groups)
		expectedType := "*[]api.RuleSetGroup"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}
