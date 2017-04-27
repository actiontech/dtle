// Copyright 2016 Circonus, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package checkmgr

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/circonus-labs/circonus-gometrics/api"
)

var (
	apiCert = CACert{
		Contents: string(circonusCA),
	}
)

func testCertServer() *httptest.Server {
	f := func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/pki/ca.crt":
			ret, err := json.Marshal(apiCert)
			if err != nil {
				panic(err)
			}
			w.WriteHeader(200)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, string(ret))
		default:
			w.WriteHeader(500)
			fmt.Fprintln(w, "unsupported")
		}
	}

	return httptest.NewServer(http.HandlerFunc(f))
}

func TestLoadCACert(t *testing.T) {
	t.Log("default cert, no fetch")

	cm := &CheckManager{
		enabled: false,
	}

	cm.loadCACert()

	if cm.certPool == nil {
		t.Errorf("Expected cert pool to be initialized, still nil.")
	}

	subjs := cm.certPool.Subjects()
	if len(subjs) == 0 {
		t.Errorf("Expected > 0 certs in pool")
	}
}

func TestFetchCert(t *testing.T) {
	server := testCertServer()
	defer server.Close()

	cm := &CheckManager{
		enabled: true,
	}
	ac := &api.Config{
		TokenApp: "abcd",
		TokenKey: "1234",
		URL:      server.URL,
	}
	apih, err := api.NewAPI(ac)
	if err != nil {
		t.Errorf("Expected no error, got '%v'", err)
	}
	cm.apih = apih

	_, err = cm.fetchCert()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	t.Log("load cert w/fetch")

	cm.loadCACert()

	if cm.certPool == nil {
		t.Errorf("Expected cert pool to be initialized, still nil.")
	}

	subjs := cm.certPool.Subjects()
	if len(subjs) == 0 {
		t.Errorf("Expected > 0 certs in pool")
	}

}
