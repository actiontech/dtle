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
	testAccount = Account{
		CID: "/account/1234",
		ContactGroups: []string{
			"/contact_group/1701",
			"/contact_group/3141",
		},
		OwnerCID: "/user/42",
		Usage: []AccountLimit{
			{
				Limit: 50,
				Type:  "Host",
				Used:  7,
			},
		},
		Address1:    &[]string{"Hooper's Store"}[0],
		Address2:    &[]string{"Sesame Street"}[0],
		CCEmail:     &[]string{"accounts_payable@yourdomain.com"}[0],
		City:        &[]string{"New York City"}[0],
		Country:     "US",
		Description: &[]string{"Hooper's Store Account"}[0],
		Invites: []AccountInvite{
			{
				Email: "alan@example.com",
				Role:  "Admin",
			},
			{
				Email: "chris.robinson@example.com",
				Role:  "Normal",
			},
		},
		Name:      "hoopers-store",
		StateProv: &[]string{"NY"}[0],
		Timezone:  "America/New_York",
		Users: []AccountUser{
			{
				Role:    "Admin",
				UserCID: "/user/42",
			},
		},
	}
)

func testAccountServer() *httptest.Server {
	f := func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/account/1234" || path == "/account/current" {
			switch r.Method {
			case "GET": // get by id/cid
				ret, err := json.Marshal(testAccount)
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
				fmt.Fprintln(w, "not found")
			}
		} else if path == "/account" {
			switch r.Method {
			case "GET":
				reqURL := r.URL.String()
				var c []Account
				if reqURL == "/account?f_name_wildcard=%2Aops%2A" {
					c = []Account{testAccount}
				} else if reqURL == "/account" {
					c = []Account{testAccount}
				} else {
					c = []Account{}
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
				fmt.Fprintln(w, "not found")
			}
		} else {
			w.WriteHeader(404)
			fmt.Fprintln(w, "not found")
		}
	}

	return httptest.NewServer(http.HandlerFunc(f))
}

func TestFetchAccount(t *testing.T) {
	server := testAccountServer()
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
		account, err := apih.FetchAccount(nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(account)
		expectedType := "*api.Account"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("invalid CID [\"\"]")
	{
		cid := ""
		account, err := apih.FetchAccount(CIDType(&cid))
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(account)
		expectedType := "*api.Account"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("invalid CID [/invalid]")
	{
		cid := "/invalid"
		expectedError := errors.New("Invalid account CID [/invalid]")
		_, err := apih.FetchAccount(CIDType(&cid))
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID")
	{
		cid := "/account/1234"
		account, err := apih.FetchAccount(CIDType(&cid))
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(account)
		expectedType := "*api.Account"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}

		if account.CID != testAccount.CID {
			t.Fatalf("CIDs do not match: %+v != %+v\n", account, testAccount)
		}
	}
}

func TestFetchAccounts(t *testing.T) {
	server := testAccountServer()
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

	accounts, err := apih.FetchAccounts()
	if err != nil {
		t.Fatalf("Expected no error, got '%v'", err)
	}

	actualType := reflect.TypeOf(accounts)
	expectedType := "*[]api.Account"
	if actualType.String() != expectedType {
		t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
	}

}

func TestUpdateAccount(t *testing.T) {
	server := testAccountServer()
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
		expectedError := errors.New("Invalid account config [nil]")
		_, err := apih.UpdateAccount(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid config [CID /invalid]")
	{
		expectedError := errors.New("Invalid account CID [/invalid]")
		x := &Account{CID: "/invalid"}
		_, err := apih.UpdateAccount(x)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		account, err := apih.UpdateAccount(&testAccount)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(account)
		expectedType := "*api.Account"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}

func TestSearchAccounts(t *testing.T) {
	server := testAccountServer()
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

	filter := SearchFilterType(map[string][]string{"f_name_wildcard": {"*ops*"}})

	t.Log("no filter")
	{
		accounts, err := apih.SearchAccounts(nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(accounts)
		expectedType := "*[]api.Account"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("filter")
	{
		accounts, err := apih.SearchAccounts(&filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(accounts)
		expectedType := "*[]api.Account"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}
