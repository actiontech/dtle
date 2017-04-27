// Copyright 2016 Circonus, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package checkmgr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/circonus-labs/circonus-gometrics/api"
	"github.com/circonus-labs/circonus-gometrics/api/config"
)

var (
	testCMCheck = api.Check{
		CID:            "/check/1234",
		Active:         true,
		BrokerCID:      "/broker/1234",
		CheckBundleCID: "/check_bundle/1234",
		CheckUUID:      "abc123-a1b2-c3d4-e5f6-123abc",
		Details:        map[config.Key]string{config.SubmissionURL: "https://127.0.0.1:43191/module/httptrap/abc123-a1b2-c3d4-e5f6-123abc/blah"},
	}

	testCMCheckBundle = api.CheckBundle{
		CheckUUIDs:    []string{"abc123-a1b2-c3d4-e5f6-123abc"},
		Checks:        []string{"/check/1234"},
		CID:           "/check_bundle/1234",
		Created:       0,
		LastModified:  0,
		LastModifedBy: "",
		ReverseConnectURLs: []string{
			"mtev_reverse://127.0.0.1:43191/check/abc123-a1b2-c3d4-e5f6-123abc",
		},
		Brokers:     []string{"/broker/1234"},
		DisplayName: "test check",
		Config: map[config.Key]string{
			config.SubmissionURL: "https://127.0.0.1:43191/module/httptrap/abc123-a1b2-c3d4-e5f6-123abc/blah",
		},
		// Config: api.CheckBundleConfig{
		// 	SubmissionURL: "https://127.0.0.1:43191/module/httptrap/abc123-a1b2-c3d4-e5f6-123abc/blah",
		// 	ReverseSecret: "blah",
		// },
		Metrics: []api.CheckBundleMetric{
			api.CheckBundleMetric{
				Name:   "elmo",
				Type:   "numeric",
				Status: "active",
			},
		},
		MetricLimit: 0,
		Notes:       nil,
		Period:      60,
		Status:      "active",
		Target:      "127.0.0.1",
		Timeout:     10,
		Type:        "httptrap",
		Tags:        []string{},
	}

	testCMBroker = api.Broker{
		CID:  "/broker/1234",
		Name: "test broker",
		Type: "enterprise",
		Details: []api.BrokerDetail{
			api.BrokerDetail{
				CN:           "testbroker.example.com",
				ExternalHost: nil,
				ExternalPort: 43191,
				IP:           &[]string{"127.0.0.1"}[0],
				Modules:      []string{"httptrap"},
				Port:         &[]uint16{43191}[0],
				Status:       "active",
			},
		},
	}
)

func testCMServer() *httptest.Server {
	f := func(w http.ResponseWriter, r *http.Request) {
		// fmt.Printf("%s %s\n", r.Method, r.URL.String())
		switch r.URL.Path {
		case "/check_bundle/1234": // handle GET/PUT/DELETE
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
				ret, err := json.Marshal(testCMCheckBundle)
				if err != nil {
					panic(err)
				}
				w.WriteHeader(200)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, string(ret))
			default:
				w.WriteHeader(500)
				fmt.Fprintln(w, "unsupported method")
			}
		case "/check_bundle":
			switch r.Method {
			case "GET": // search
				if strings.HasPrefix(r.URL.String(), "/check_bundle?search=") {
					r := []api.CheckBundle{testCMCheckBundle}
					ret, err := json.Marshal(r)
					if err != nil {
						panic(err)
					}
					w.WriteHeader(200)
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprintln(w, string(ret))
				} else {
					w.WriteHeader(200)
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprintln(w, "[]")
				}
			case "POST": // create
				defer r.Body.Close()
				_, err := ioutil.ReadAll(r.Body)
				if err != nil {
					panic(err)
				}
				ret, err := json.Marshal(testCheckBundle)
				if err != nil {
					panic(err)
				}
				w.WriteHeader(200)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, string(ret))
			default:
				w.WriteHeader(405)
				fmt.Fprintf(w, "method not allowed %s", r.Method)
			}
		case "/broker":
			switch r.Method {
			case "GET":
				r := []api.Broker{testCMBroker}
				ret, err := json.Marshal(r)
				if err != nil {
					panic(err)
				}
				w.WriteHeader(200)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, string(ret))
			default:
				w.WriteHeader(405)
				fmt.Fprintf(w, "method not allowed %s", r.Method)
			}
		case "/broker/1234":
			switch r.Method {
			case "GET":
				ret, err := json.Marshal(testCMBroker)
				if err != nil {
					panic(err)
				}
				w.WriteHeader(200)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, string(ret))
			default:
				w.WriteHeader(405)
				fmt.Fprintf(w, "method not allowed %s", r.Method)
			}
		case "/check":
			switch r.Method {
			case "GET":
				r := []api.Check{testCMCheck}
				ret, err := json.Marshal(r)
				if err != nil {
					panic(err)
				}
				w.WriteHeader(200)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, string(ret))
			default:
				w.WriteHeader(405)
				fmt.Fprintf(w, "method not allowed %s", r.Method)
			}
		case "/check/1234":
			switch r.Method {
			case "GET":
				ret, err := json.Marshal(testCMCheck)
				if err != nil {
					panic(err)
				}
				w.WriteHeader(200)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, string(ret))
			default:
				w.WriteHeader(405)
				fmt.Fprintf(w, "method not allowed %s", r.Method)
			}
		case "/pki/ca.crt":
			w.WriteHeader(200)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, cert)
		default:
			msg := fmt.Sprintf("not found %s", r.URL.Path)
			w.WriteHeader(404)
			fmt.Fprintln(w, msg)
		}
	}

	return httptest.NewServer(http.HandlerFunc(f))
}

func TestNewCheckManager(t *testing.T) {

	t.Log("no config supplied")
	{
		expectedError := errors.New("invalid Check Manager configuration (nil)")
		_, err := NewCheckManager(nil)
		if err == nil || err.Error() != expectedError.Error() {
			t.Errorf("Expected an '%#v' error, got '%#v'", expectedError, err)
		}
	}

	t.Log("no API Token and no Submission URL supplied")
	{
		expectedError := errors.New("invalid check manager configuration (no API token AND no submission url)")
		cfg := &Config{}
		_, err := NewCheckManager(cfg)
		if err == nil || err.Error() != expectedError.Error() {
			t.Errorf("Expected an '%#v' error, got '%#v'", expectedError, err)
		}
	}

	t.Log("no API Token, Submission URL (http) only")
	{
		cfg := &Config{}
		cfg.Check.SubmissionURL = "http://127.0.0.1:56104"
		cm, err := NewCheckManager(cfg)
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}

		cm.Initialize()

		for !cm.IsReady() {
			t.Log("\twaiting for cm to init")
			time.Sleep(1 * time.Second)
		}

		trap, err := cm.GetSubmissionURL()
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}

		if trap.URL.String() != cfg.Check.SubmissionURL {
			t.Errorf("Expected '%s' == '%s'", trap.URL.String(), cfg.Check.SubmissionURL)
		}

		if trap.TLS != nil {
			t.Errorf("Expected nil found %#v", trap.TLS)
		}
	}

	t.Log("no API Token, Submission URL (https) only")
	{
		cfg := &Config{}
		cfg.Check.SubmissionURL = "https://127.0.0.1/v2"

		cm, err := NewCheckManager(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		cm.Initialize()

		for !cm.IsReady() {
			t.Log("\twaiting for cm to init")
			time.Sleep(1 * time.Second)
		}

		trap, err := cm.GetSubmissionURL()
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		if trap.URL.String() != cfg.Check.SubmissionURL {
			t.Fatalf("Expected '%s' == '%s'", trap.URL.String(), cfg.Check.SubmissionURL)
		}

		if trap.TLS == nil {
			t.Fatalf("Expected a x509 cert pool, found nil")
		}
	}

	server := testCMServer()
	defer server.Close()

	testURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("Error parsing temporary url %v", err)
	}

	hostParts := strings.Split(testURL.Host, ":")
	hostPort, err := strconv.Atoi(hostParts[1])
	if err != nil {
		t.Fatalf("Error converting port to numeric %v", err)
	}

	testCMBroker.Details[0].ExternalHost = &hostParts[0]
	testCMBroker.Details[0].ExternalPort = uint16(hostPort)

	t.Log("Defaults")
	{
		cfg := &Config{
			Log: log.New(os.Stderr, "", log.LstdFlags),
			API: api.Config{
				TokenKey: "1234",
				TokenApp: "abc",
				URL:      server.URL,
			},
		}

		cm, err := NewCheckManager(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		cm.Initialize()

		for !cm.IsReady() {
			t.Log("\twaiting for cm to init")
			time.Sleep(1 * time.Second)
		}

		trap, err := cm.GetSubmissionURL()
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}
		suburl, found := testCMCheckBundle.Config["submission_url"]
		if !found {
			t.Fatalf("Exected submission_url in check bundle config %+v", testCMCheckBundle)
		}
		if trap.URL.String() != suburl {
			t.Fatalf("Expected '%s' got '%s'", suburl, trap.URL.String())
		}
	}
}
