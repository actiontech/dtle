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
	testProvisionBroker = ProvisionBroker{
		Cert: "...",
		Stratcons: []BrokerStratcon{
			{CN: "foobar", Host: "foobar.example.com", Port: "12345"},
		},
		CSR:          "...",
		ExternalHost: "abc-123.example.com",
		ExternalPort: "443",
		IPAddress:    "192.168.1.10",
		Latitude:     "",
		Longitude:    "",
		Name:         "abc123",
		Port:         "43191",
		PreferReverseConnection: true,
		Rebuild:                 false,
		Tags:                    []string{"cat:tag"},
	}
)

func testProvisionBrokerServer() *httptest.Server {
	f := func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/provision_broker/abc-1234" {
			switch r.Method {
			case "GET":
				ret, err := json.Marshal(testProvisionBroker)
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
		} else if path == "/provision_broker" {
			switch r.Method {
			case "POST":
				defer r.Body.Close()
				_, err := ioutil.ReadAll(r.Body)
				if err != nil {
					panic(err)
				}
				ret, err := json.Marshal(testProvisionBroker)
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

func TestNewProvisionBroker(t *testing.T) {
	bundle := NewProvisionBroker()
	actualType := reflect.TypeOf(bundle)
	expectedType := "*api.ProvisionBroker"
	if actualType.String() != expectedType {
		t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
	}
}

func TestFetchProvisionBroker(t *testing.T) {
	server := testProvisionBrokerServer()
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
		expectedError := errors.New("Invalid provision broker request CID [none]")
		_, err := apih.FetchProvisionBroker(nil)
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
		expectedError := errors.New("Invalid provision broker request CID [none]")
		_, err := apih.FetchProvisionBroker(CIDType(&cid))
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
		expectedError := errors.New("Invalid provision broker request CID [/invalid]")
		_, err := apih.FetchProvisionBroker(CIDType(&cid))
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID")
	{
		cid := "/provision_broker/abc-1234"
		broker, err := apih.FetchProvisionBroker(CIDType(&cid))
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(broker)
		expectedType := "*api.ProvisionBroker"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}

		if broker.CID != testProvisionBroker.CID {
			t.Fatalf("CIDs do not match: %+v != %+v\n", broker, testProvisionBroker)
		}
	}
}

func TestUpdateProvisionBroker(t *testing.T) {
	server := testProvisionBrokerServer()
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

	t.Log("invalid CID [nil], config n/a")
	{
		expectedError := errors.New("Invalid provision broker request CID [none]")
		x := &ProvisionBroker{}
		_, err := apih.UpdateProvisionBroker(nil, x)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid CID [/invalid], config n/a")
	{
		cid := "/invalid"
		expectedError := errors.New("Invalid provision broker request CID [/invalid]")
		x := &ProvisionBroker{}
		_, err := apih.UpdateProvisionBroker(CIDType(&cid), x)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID, invalid config [nil]")
	{
		cid := "/provision_broker/abc-1234"
		expectedError := errors.New("Invalid provision broker request config [nil]")
		_, err := apih.UpdateProvisionBroker(CIDType(&cid), nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID, valid config")
	{
		cid := "/provision_broker/abc-1234"
		broker, err := apih.UpdateProvisionBroker(CIDType(&cid), &testProvisionBroker)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(broker)
		expectedType := "*api.ProvisionBroker"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}

func TestCreateProvisionBroker(t *testing.T) {
	server := testProvisionBrokerServer()
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
		expectedError := errors.New("Invalid provision broker request config [nil]")
		_, err := apih.CreateProvisionBroker(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		broker, err := apih.CreateProvisionBroker(&testProvisionBroker)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(broker)
		expectedType := "*api.ProvisionBroker"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}
