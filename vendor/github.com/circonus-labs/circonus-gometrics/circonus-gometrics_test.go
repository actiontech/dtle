// Copyright 2016 Circonus, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package circonusgometrics

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func testServer() *httptest.Server {
	f := func(w http.ResponseWriter, r *http.Request) {
		// fmt.Printf("%s %s\n", r.Method, r.URL.String())
		switch r.URL.Path {
		case "/metrics_endpoint": // submit metrics
			switch r.Method {
			case "POST":
				fallthrough
			case "PUT":
				defer r.Body.Close()
				b, err := ioutil.ReadAll(r.Body)
				if err != nil {
					panic(err)
				}
				var ret []byte
				var r interface{}
				err = json.Unmarshal(b, &r)
				if err != nil {
					ret, err = json.Marshal(err)
					if err != nil {
						panic(err)
					}
				} else {
					ret, err = json.Marshal(r)
					if err != nil {
						panic(err)
					}
				}
				w.WriteHeader(200)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, string(ret))
			default:
				w.WriteHeader(500)
				fmt.Fprintln(w, "unsupported method")
			}
		default:
			msg := fmt.Sprintf("not found %s", r.URL.Path)
			w.WriteHeader(404)
			fmt.Fprintln(w, msg)
		}
	}

	return httptest.NewServer(http.HandlerFunc(f))
}

func TestNew(t *testing.T) {

	t.Log("invalid config (none)")
	{
		expectedError := errors.New("invalid configuration (nil)")
		_, err := New(nil)
		if err == nil || err.Error() != expectedError.Error() {
			t.Fatalf("Expected an '%#v' error, got '%#v'", expectedError, err)
		}
	}

	t.Log("no API token, no submission URL")
	{
		cfg := &Config{}
		expectedError := errors.New("invalid check manager configuration (no API token AND no submission url)")
		_, err := New(cfg)
		if err == nil || err.Error() != expectedError.Error() {
			t.Fatalf("Expected an '%#v' error, got '%#v'", expectedError, err)
		}
	}

	t.Log("no API token, submission URL only")
	{
		cfg := &Config{}
		cfg.CheckManager.Check.SubmissionURL = "http://127.0.0.1:56104/blah/blah"

		cm, err := New(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		for !cm.check.IsReady() {
			t.Log("\twaiting for cm to init")
			time.Sleep(1 * time.Second)
		}

		trap, err := cm.check.GetSubmissionURL()
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		if trap.URL.String() != cfg.CheckManager.Check.SubmissionURL {
			t.Fatalf("Expected '%s' == '%s'", trap.URL.String(), cfg.CheckManager.Check.SubmissionURL)
		}
	}

	t.Log("no Log, Debug = true")
	{
		cfg := &Config{
			Debug: true,
		}
		cfg.CheckManager.Check.SubmissionURL = "http://127.0.0.1:56104/blah/blah"
		_, err := New(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}
	}

	t.Log("flush interval [good]")
	{
		cfg := &Config{
			Interval: "30s",
		}
		cfg.CheckManager.Check.SubmissionURL = "http://127.0.0.1:56104/blah/blah"
		_, err := New(cfg)
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}
	}
	t.Log("flush interval [bad]")
	{
		cfg := &Config{
			Interval: "thirty seconds",
		}
		expectedError := errors.New("time: invalid duration thirty seconds")
		_, err := New(cfg)
		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %v got '%v'", expectedError, err)
		}
	}

	t.Log("reset counters [good(true)]")
	{
		cfg := &Config{
			ResetCounters: "true",
		}
		cfg.CheckManager.Check.SubmissionURL = "http://127.0.0.1:56104/blah/blah"
		_, err := New(cfg)
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}
	}
	t.Log("reset counters [good(1)]")
	{
		cfg := &Config{
			ResetCounters: "1",
		}
		cfg.CheckManager.Check.SubmissionURL = "http://127.0.0.1:56104/blah/blah"
		_, err := New(cfg)
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}
	}
	t.Log("reset counters [bad(yes)]")
	{
		cfg := &Config{
			ResetCounters: "yes",
		}
		expectedError := errors.New("strconv.ParseBool: parsing \"yes\": invalid syntax")
		_, err := New(cfg)
		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %v got '%v'", expectedError, err)
		}
	}

	t.Log("reset gauges [good(true)]")
	{
		cfg := &Config{
			ResetGauges: "true",
		}
		cfg.CheckManager.Check.SubmissionURL = "http://127.0.0.1:56104/blah/blah"
		_, err := New(cfg)
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}
	}
	t.Log("reset gauges [good(1)]")
	{
		cfg := &Config{
			ResetGauges: "1",
		}
		cfg.CheckManager.Check.SubmissionURL = "http://127.0.0.1:56104/blah/blah"
		_, err := New(cfg)
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}
	}
	t.Log("reset gauges [bad(yes)]")
	{
		cfg := &Config{
			ResetGauges: "yes",
		}
		expectedError := errors.New("strconv.ParseBool: parsing \"yes\": invalid syntax")
		_, err := New(cfg)
		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %v got '%v'", expectedError, err)
		}
	}

	t.Log("reset histograms [good(true)]")
	{
		cfg := &Config{
			ResetHistograms: "true",
		}
		cfg.CheckManager.Check.SubmissionURL = "http://127.0.0.1:56104/blah/blah"
		_, err := New(cfg)
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}
	}
	t.Log("reset histograms [good(1)]")
	{
		cfg := &Config{
			ResetHistograms: "1",
		}
		cfg.CheckManager.Check.SubmissionURL = "http://127.0.0.1:56104/blah/blah"
		_, err := New(cfg)
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}
	}
	t.Log("reset histograms [bad(yes)]")
	{
		cfg := &Config{
			ResetHistograms: "yes",
		}
		expectedError := errors.New("strconv.ParseBool: parsing \"yes\": invalid syntax")
		_, err := New(cfg)
		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %v got '%v'", expectedError, err)
		}
	}

	t.Log("reset text metrics [good(true)]")
	{
		cfg := &Config{
			ResetText: "true",
		}
		cfg.CheckManager.Check.SubmissionURL = "http://127.0.0.1:56104/blah/blah"
		_, err := New(cfg)
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}
	}
	t.Log("reset text metrics [good(1)]")
	{
		cfg := &Config{
			ResetText: "1",
		}
		cfg.CheckManager.Check.SubmissionURL = "http://127.0.0.1:56104/blah/blah"
		_, err := New(cfg)
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}
	}
	t.Log("reset text metrics [bad(yes)]")
	{
		cfg := &Config{
			ResetText: "yes",
		}
		expectedError := errors.New("strconv.ParseBool: parsing \"yes\": invalid syntax")
		_, err := New(cfg)
		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %v got '%v'", expectedError, err)
		}
	}
}

func TestFlush(t *testing.T) {
	server := testServer()
	defer server.Close()

	submissionURL := server.URL + "/metrics_endpoint"

	t.Log("Already flushing")
	{
		cfg := &Config{}
		cfg.CheckManager.Check.SubmissionURL = submissionURL
		cm, err := NewCirconusMetrics(cfg)
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}

		cm.flushing = true
		cm.Flush()
	}

	t.Log("No metrics")
	{
		cfg := &Config{}
		cfg.CheckManager.Check.SubmissionURL = submissionURL
		cm, err := NewCirconusMetrics(cfg)
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}

		cm.Flush()
	}

	t.Log("counter")
	{
		cfg := &Config{}
		cfg.CheckManager.Check.SubmissionURL = submissionURL
		cm, err := NewCirconusMetrics(cfg)
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}

		cm.Set("foo", 30)

		cm.Flush()
	}

	t.Log("gauge")
	{
		cfg := &Config{}
		cfg.CheckManager.Check.SubmissionURL = submissionURL
		cm, err := NewCirconusMetrics(cfg)
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}

		cm.SetGauge("foo", 30)

		cm.Flush()
	}

	t.Log("histogram")
	{
		cfg := &Config{}
		cfg.CheckManager.Check.SubmissionURL = submissionURL
		cm, err := NewCirconusMetrics(cfg)
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}

		cm.Timing("foo", 30.28)

		cm.Flush()
	}

	t.Log("text")
	{
		cfg := &Config{}
		cfg.CheckManager.Check.SubmissionURL = submissionURL
		cm, err := NewCirconusMetrics(cfg)
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}

		cm.SetText("foo", "bar")

		cm.Flush()
	}
}
