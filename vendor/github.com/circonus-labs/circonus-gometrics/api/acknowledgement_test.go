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
	testAcknowledgement = Acknowledgement{
		CID:               "/acknowledgement/1234",
		AcknowledgedBy:    "/user/1234",
		AcknowledgedOn:    1483033102,
		Active:            true,
		LastModified:      1483033102,
		LastModifiedBy:    "/user/1234",
		AcknowledgedUntil: "1d",
		Notes:             "blah blah blah",
	}
)

func testAcknowledgementServer() *httptest.Server {
	f := func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/acknowledgement/1234" {
			switch r.Method {
			case "GET":
				ret, err := json.Marshal(testAcknowledgement)
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
		} else if path == "/acknowledgement" {
			switch r.Method {
			case "GET":
				reqURL := r.URL.String()
				var c []Acknowledgement
				if r.URL.String() == "/acknowledgement?search=%28notes%3D%22something%22%29" {
					c = []Acknowledgement{testAcknowledgement}
				} else if r.URL.String() == "/acknowledgement?f__active=true" {
					c = []Acknowledgement{testAcknowledgement}
				} else if r.URL.String() == "/acknowledgement?f__active=true&search=%28notes%3D%22something%22%29" {
					c = []Acknowledgement{testAcknowledgement}
				} else if reqURL == "/acknowledgement" {
					c = []Acknowledgement{testAcknowledgement}
				} else {
					c = []Acknowledgement{}
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
				ret, err := json.Marshal(testAcknowledgement)
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

func TestNewAcknowledgement(t *testing.T) {
	bundle := NewAcknowledgement()
	actualType := reflect.TypeOf(bundle)
	expectedType := "*api.Acknowledgement"
	if actualType.String() != expectedType {
		t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
	}
}

func TestFetchAcknowledgement(t *testing.T) {
	server := testAcknowledgementServer()
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
		expectedError := errors.New("Invalid acknowledgement CID [none]")
		_, err := apih.FetchAcknowledgement(nil)
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
		expectedError := errors.New("Invalid acknowledgement CID [none]")
		_, err := apih.FetchAcknowledgement(CIDType(&cid))
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
		expectedError := errors.New("Invalid acknowledgement CID [/invalid]")
		_, err := apih.FetchAcknowledgement(CIDType(&cid))
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID")
	{
		cid := "/acknowledgement/1234"
		acknowledgement, err := apih.FetchAcknowledgement(CIDType(&cid))
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(acknowledgement)
		expectedType := "*api.Acknowledgement"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}

		if acknowledgement.CID != testAcknowledgement.CID {
			t.Fatalf("CIDs do not match: %+v != %+v\n", acknowledgement, testAcknowledgement)
		}
	}
}

func TestFetchAcknowledgements(t *testing.T) {
	server := testAcknowledgementServer()
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

	acknowledgements, err := apih.FetchAcknowledgements()
	if err != nil {
		t.Fatalf("Expected no error, got '%v'", err)
	}

	actualType := reflect.TypeOf(acknowledgements)
	expectedType := "*[]api.Acknowledgement"
	if actualType.String() != expectedType {
		t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
	}

}

func TestUpdateAcknowledgement(t *testing.T) {
	server := testAcknowledgementServer()
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
		expectedError := errors.New("Invalid acknowledgement config [nil]")
		_, err := apih.UpdateAcknowledgement(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid config [CID /invalid]")
	{
		expectedError := errors.New("Invalid acknowledgement CID [/invalid]")
		x := &Acknowledgement{CID: "/invalid"}
		_, err := apih.UpdateAcknowledgement(x)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		acknowledgement, err := apih.UpdateAcknowledgement(&testAcknowledgement)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(acknowledgement)
		expectedType := "*api.Acknowledgement"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}

func TestCreateAcknowledgement(t *testing.T) {
	server := testAcknowledgementServer()
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
		expectedError := errors.New("Invalid acknowledgement config [nil]")
		_, err := apih.CreateAcknowledgement(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		acknowledgement, err := apih.CreateAcknowledgement(&testAcknowledgement)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(acknowledgement)
		expectedType := "*api.Acknowledgement"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}

func TestSearchAcknowledgement(t *testing.T) {
	server := testAcknowledgementServer()
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
		acknowledgements, err := apih.SearchAcknowledgements(nil, nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(acknowledgements)
		expectedType := "*[]api.Acknowledgement"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("search, no filter")
	{
		search := SearchQueryType(`(notes="something")`)
		acknowledgements, err := apih.SearchAcknowledgements(&search, nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(acknowledgements)
		expectedType := "*[]api.Acknowledgement"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("no search, filter")
	{
		filter := SearchFilterType(map[string][]string{"f__active": {"true"}})
		acknowledgements, err := apih.SearchAcknowledgements(nil, &filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(acknowledgements)
		expectedType := "*[]api.Acknowledgement"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("search, filter")
	{
		search := SearchQueryType(`(notes="something")`)
		filter := SearchFilterType(map[string][]string{"f__active": {"true"}})
		acknowledgements, err := apih.SearchAcknowledgements(&search, &filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(acknowledgements)
		expectedType := "*[]api.Acknowledgement"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}
