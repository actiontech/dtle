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
	testContactGroup = ContactGroup{
		CID:               "/contact_group/1234",
		LastModifiedBy:    "/user/1234",
		LastModified:      1483041636,
		AggregationWindow: 300,
		Contacts: ContactGroupContacts{
			External: []ContactGroupContactsExternal{
				{
					Info:   "12125550100",
					Method: "sms",
				},
				{
					Info:   "bert@example.com",
					Method: "xmpp",
				},
				{
					Info:   "ernie@example.com",
					Method: "email",
				},
			},
			Users: []ContactGroupContactsUser{
				{
					Info:    "snuffy@example.com",
					Method:  "email",
					UserCID: "/user/1234",
				},
				{
					Info:    "12125550199",
					Method:  "sms",
					UserCID: "/user/4567",
				},
			},
		},
		Escalations: []*ContactGroupEscalation{
			{
				After:           900,
				ContactGroupCID: "/contact_group/4567",
			},
			nil,
			nil,
			nil,
			nil,
		},
		Name:      "FooBar",
		Reminders: []uint{10, 0, 0, 15, 30},
	}
)

func testContactGroupServer() *httptest.Server {
	f := func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/contact_group/1234" {
			switch r.Method {
			case "GET":
				ret, err := json.Marshal(testContactGroup)
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
		} else if path == "/contact_group" {
			switch r.Method {
			case "GET":
				reqURL := r.URL.String()
				var c []ContactGroup
				if reqURL == "/contact_group?search=%28name%3D%22ops%22%29" {
					c = []ContactGroup{testContactGroup}
				} else if reqURL == "/contact_group?f__last_modified_gt=1483639916" {
					c = []ContactGroup{testContactGroup}
				} else if reqURL == "/contact_group?f__last_modified_gt=1483639916&search=%28name%3D%22ops%22%29" {
					c = []ContactGroup{testContactGroup}
				} else if reqURL == "/contact_group" {
					c = []ContactGroup{testContactGroup}
				} else {
					c = []ContactGroup{}
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
				ret, err := json.Marshal(testContactGroup)
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

func TestNewContactGroup(t *testing.T) {
	bundle := NewContactGroup()
	actualType := reflect.TypeOf(bundle)
	expectedType := "*api.ContactGroup"
	if actualType.String() != expectedType {
		t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
	}
}

func TestFetchContactGroup(t *testing.T) {
	server := testContactGroupServer()
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
		expectedError := errors.New("Invalid contact group CID [none]")
		_, err := apih.FetchContactGroup(nil)
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
		expectedError := errors.New("Invalid contact group CID [none]")
		_, err := apih.FetchContactGroup(CIDType(&cid))
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
		expectedError := errors.New("Invalid contact group CID [/invalid]")
		_, err := apih.FetchContactGroup(CIDType(&cid))
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID")
	{
		cid := "/contact_group/1234"
		contactGroup, err := apih.FetchContactGroup(CIDType(&cid))
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(contactGroup)
		expectedType := "*api.ContactGroup"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}

		if contactGroup.CID != testContactGroup.CID {
			t.Fatalf("CIDs do not match: %+v != %+v\n", contactGroup, testContactGroup)
		}
	}
}

func TestFetchContactGroups(t *testing.T) {
	server := testContactGroupServer()
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

	contactGroups, err := apih.FetchContactGroups()
	if err != nil {
		t.Fatalf("Expected no error, got '%v'", err)
	}

	actualType := reflect.TypeOf(contactGroups)
	expectedType := "*[]api.ContactGroup"
	if actualType.String() != expectedType {
		t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
	}

}

func TestUpdateContactGroup(t *testing.T) {
	server := testContactGroupServer()
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
		expectedError := errors.New("Invalid contact group config [nil]")
		_, err := apih.UpdateContactGroup(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid config [CID /invalid]")
	{
		expectedError := errors.New("Invalid contact group CID [/invalid]")
		x := &ContactGroup{CID: "/invalid"}
		_, err := apih.UpdateContactGroup(x)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID")
	{
		contactGroup, err := apih.UpdateContactGroup(&testContactGroup)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(contactGroup)
		expectedType := "*api.ContactGroup"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}

func TestCreateContactGroup(t *testing.T) {
	server := testContactGroupServer()
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
		expectedError := errors.New("Invalid contact group config [nil]")
		_, err := apih.CreateContactGroup(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		contactGroup, err := apih.CreateContactGroup(&testContactGroup)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(contactGroup)
		expectedType := "*api.ContactGroup"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}

func TestDeleteContactGroup(t *testing.T) {
	server := testContactGroupServer()
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
		expectedError := errors.New("Invalid contact group config [nil]")
		_, err := apih.DeleteContactGroup(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid config [CID /invalid]")
	{
		expectedError := errors.New("Invalid contact group CID [/invalid]")
		x := &ContactGroup{CID: "/invalid"}
		_, err := apih.DeleteContactGroup(x)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		_, err := apih.DeleteContactGroup(&testContactGroup)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}
	}
}

func TestDeleteContactGroupByCID(t *testing.T) {
	server := testContactGroupServer()
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
		expectedError := errors.New("Invalid contact group CID [none]")
		_, err := apih.DeleteContactGroupByCID(nil)
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
		expectedError := errors.New("Invalid contact group CID [none]")
		_, err := apih.DeleteContactGroupByCID(CIDType(&cid))
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
		expectedError := errors.New("Invalid contact group CID [/invalid]")
		_, err := apih.DeleteContactGroupByCID(CIDType(&cid))
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID")
	{
		cid := "/contact_group/1234"
		_, err := apih.DeleteContactGroupByCID(CIDType(&cid))
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}
	}
}

func TestSearchContactGroups(t *testing.T) {
	server := testContactGroupServer()
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

	t.Log("no search, no filter")
	{
		groups, err := apih.SearchContactGroups(nil, nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(groups)
		expectedType := "*[]api.ContactGroup"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("search, no filter")
	{
		search := SearchQueryType(`(name="ops")`)
		groups, err := apih.SearchContactGroups(&search, nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(groups)
		expectedType := "*[]api.ContactGroup"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("no search, filter")
	{
		filter := SearchFilterType(map[string][]string{"f__last_modified_gt": {"1483639916"}})
		groups, err := apih.SearchContactGroups(nil, &filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(groups)
		expectedType := "*[]api.ContactGroup"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("search, filter")
	{
		search := SearchQueryType(`(name="ops")`)
		filter := SearchFilterType(map[string][]string{"f__last_modified_gt": {"1483639916"}})
		groups, err := apih.SearchContactGroups(&search, &filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(groups)
		expectedType := "*[]api.ContactGroup"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}
