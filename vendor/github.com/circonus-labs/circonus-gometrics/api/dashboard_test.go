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
	"os"
	"reflect"
	"testing"
)

var (
	testDashboard = Dashboard{}
)

var jsondash = `{
  "_active": true,
  "_cid": "/dashboard/1234",
  "_created": 1483193930,
  "_created_by": "/user/1234",
  "_dashboard_uuid": "01234567-89ab-cdef-0123-456789abcdef",
  "_last_modified": 1483450351,
  "account_default": false,
  "grid_layout": {
    "height": 4,
    "width": 4
  },
  "options": {
    "access_configs": [
    ],
    "fullscreen_hide_title": false,
    "hide_grid": false,
    "linkages": [
    ],
    "scale_text": true,
    "text_size": 16
  },
  "shared": false,
  "title": "foo bar baz",
  "widgets": [
    {
      "active": true,
      "height": 1,
      "name": "Cluster",
      "origin": "d0",
      "settings": {
        "account_id": "1234",
        "algorithm": "cor",
        "cluster_id": 1234,
        "cluster_name": "test",
        "layout": "compact",
        "size": "medium",
        "threshold": 0.7
      },
      "type": "cluster",
      "widget_id": "w4",
      "width": 1
    },
    {
      "active": true,
      "height": 1,
      "name": "HTML",
      "origin": "d1",
      "settings": {
        "markup": "<h1>foo</h1>",
        "title": "html"
      },
      "type": "html",
      "widget_id": "w9",
      "width": 1
    },
    {
      "active": true,
      "height": 1,
      "name": "Chart",
      "origin": "c0",
      "settings": {
        "chart_type": "bar",
        "datapoints": [
          {
            "_check_id": 1234,
            "_metric_type": "numeric",
            "account_id": "1234",
            "label": "Used",
            "metric": "01234567-89ab-cdef-0123-456789abcdef:vm.memory.used"
          },
          {
            "_check_id": 1234,
            "_metric_type": "numeric",
            "account_id": "1234",
            "label": "Free",
            "metric": "01234567-89ab-cdef-0123-456789abcdef:vm.memory.free"
          }
        ],
        "definition": {
          "datasource": "realtime",
          "derive": "gauge",
          "disable_autoformat": false,
          "formula": "",
          "legend": {
            "show": false,
            "type": "html"
          },
          "period": 0,
          "pop_onhover": false,
          "wedge_labels": {
            "on_chart": true,
            "tooltips": false
          },
          "wedge_values": {
            "angle": "0",
            "color": "background",
            "show": true
          }
        },
        "title": "chart graph"
      },
      "type": "chart",
      "widget_id": "w5",
      "width": 1
    },
    {
      "active": true,
      "height": 1,
      "name": "Alerts",
      "origin": "a0",
      "settings": {
        "account_id": "1234",
        "acknowledged": "all",
        "cleared": "all",
        "contact_groups": [
        ],
        "dependents": "all",
        "display": "list",
        "maintenance": "all",
        "min_age": "0",
        "off_hours": [
          17,
          9
        ],
        "search": "",
        "severity": "12345",
        "tag_filter_set": [
        ],
        "time_window": "30M",
        "title": "alerts",
        "week_days": [
          "sun",
          "mon",
          "tue",
          "wed",
          "thu",
          "fri",
          "sat"
        ]
      },
      "type": "alerts",
      "widget_id": "w2",
      "width": 1
    },
    {
      "active": true,
      "height": 1,
      "name": "Graph",
      "origin": "c1",
      "settings": {
        "_graph_title": "foo bar / %Used",
        "account_id": "1234",
        "date_window": "2w",
        "graph_id": "01234567-89ab-cdef-0123-456789abcdef",
        "hide_xaxis": false,
        "hide_yaxis": false,
        "key_inline": false,
        "key_loc": "noop",
        "key_size": 1,
        "key_wrap": false,
        "label": "",
        "overlay_set_id": "",
        "period": 2000,
        "previous_graph_id": "null",
        "realtime": false,
        "show_flags": false
      },
      "type": "graph",
      "widget_id": "w8",
      "width": 1
    },
    {
      "active": true,
      "height": 1,
      "name": "List",
      "origin": "a2",
      "settings": {
        "account_id": "1234",
        "limit": 10,
        "search": "",
        "type": "graph"
      },
      "type": "list",
      "widget_id": "w10",
      "width": 1
    },
    {
      "active": true,
      "height": 1,
      "name": "Status",
      "origin": "b2",
      "settings": {
        "account_id": "1234",
        "agent_status_settings": {
          "search": "",
          "show_agent_types": "both",
          "show_contact": false,
          "show_feeds": true,
          "show_setup": false,
          "show_skew": true,
          "show_updates": true
        },
        "content_type": "agent_status",
        "host_status_settings": {
          "layout_style": "grid",
          "search": "",
          "sort_by": "alerts",
          "tag_filter_set": [
          ]
        }
      },
      "type": "status",
      "widget_id": "w11",
      "width": 1
    },
    {
      "active": true,
      "height": 1,
      "name": "Text",
      "origin": "d2",
      "settings": {
        "autoformat": false,
        "body_format": "<p>{metric_name} ({value_type})<br /><strong>{metric_value}</strong><br /><span class=\"date\">{value_date}</span></p>",
        "datapoints": [
          {
            "_cluster_title": "test",
            "_label": "Cluster: test",
            "account_id": "1234",
            "cluster_id": 1234,
            "numeric_only": false
          }
        ],
        "period": 0,
        "title_format": "Metric Status",
        "use_default": true,
        "value_type": "gauge"
      },
      "type": "text",
      "widget_id": "w13",
      "width": 1
    },
    {
      "active": true,
      "height": 1,
      "name": "Chart",
      "origin": "b0",
      "settings": {
        "chart_type": "bar",
        "datapoints": [
          {
            "_cluster_title": "test",
            "_label": "Cluster: test",
            "account_id": "1234",
            "cluster_id": 1234,
            "numeric_only": true
          }
        ],
        "definition": {
          "datasource": "realtime",
          "derive": "gauge",
          "disable_autoformat": false,
          "formula": "",
          "legend": {
            "show": false,
            "type": "html"
          },
          "period": 0,
          "pop_onhover": false,
          "wedge_labels": {
            "on_chart": true,
            "tooltips": false
          },
          "wedge_values": {
            "angle": "0",
            "color": "background",
            "show": true
          }
        },
        "title": "chart metric cluster"
      },
      "type": "chart",
      "widget_id": "w3",
      "width": 1
    },
    {
      "active": true,
      "height": 1,
      "name": "Gauge",
      "origin": "b1",
      "settings": {
        "_check_id": 1234,
        "account_id": "1234",
        "check_uuid": "01234567-89ab-cdef-0123-456789abcdef",
        "disable_autoformat": false,
        "formula": "",
        "metric_display_name": "%Used",
        "metric_name": "fs./foo.df_used_percent",
        "period": 0,
        "range_high": 100,
        "range_low": 0,
        "thresholds": {
          "colors": [
            "#008000",
            "#ffcc00",
            "#ee0000"
          ],
          "flip": false,
          "values": [
            "75%",
            "87.5%"
          ]
        },
        "title": "Metric Gauge",
        "type": "bar",
        "value_type": "gauge"
      },
      "type": "gauge",
      "widget_id": "w7",
      "width": 1
    },
    {
      "active": true,
      "height": 1,
      "name": "Text",
      "origin": "c2",
      "settings": {
        "autoformat": false,
        "body_format": "<p>{metric_name} ({value_type})<br /><strong>{metric_value}</strong><br /><span class=\"date\">{value_date}</span></p>",
        "datapoints": [
          {
            "_check_id": 1234,
            "_metric_type": "numeric",
            "account_id": "1234",
            "label": "cache entries",
            "metric": "01234567-89ab-cdef-0123-456789abcdef:foo.cache_entries"
          },
          {
            "_check_id": 1234,
            "_metric_type": "numeric",
            "account_id": "1234",
            "label": "cache capacity",
            "metric": "01234567-89ab-cdef-0123-456789abcdef:foo.cache_capacity"
          },
          {
            "_check_id": 1234,
            "_metric_type": "numeric",
            "account_id": "1234",
            "label": "cache size",
            "metric": "01234567-89ab-cdef-0123-456789abcdef:foo.cache_size"
          }
        ],
        "period": 0,
        "title_format": "Metric Status",
        "use_default": true,
        "value_type": "gauge"
      },
      "type": "text",
      "widget_id": "w12",
      "width": 1
    },
    {
      "active": true,
      "height": 1,
      "name": "Forecast",
      "origin": "a1",
      "settings": {
        "format": "standard",
        "resource_limit": "0",
        "resource_usage": "metric:average(\"01234567-89ab-cdef-0123-456789abcdef\",p\"fs%60/foo%60df_used_percent\")",
        "thresholds": {
          "colors": [
            "#008000",
            "#ffcc00",
            "#ee0000"
          ],
          "values": [
            "1d",
            "1h"
          ]
        },
        "title": "Resource Forecast",
        "trend": "auto"
      },
      "type": "forecast",
      "widget_id": "w6",
      "width": 1
    }
  ]
}
`

func init() {
	err := json.Unmarshal([]byte(jsondash), &testDashboard)
	if err != nil {
		fmt.Printf("Unable to unmarshal inline json (%s)\n", err)
		os.Exit(1)
	}
}

func testDashboardServer() *httptest.Server {
	f := func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/dashboard/1234" {
			switch r.Method {
			case "GET":
				w.WriteHeader(200)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, jsondash)
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
		} else if path == "/dashboard" {
			switch r.Method {
			case "GET":
				reqURL := r.URL.String()
				var c []Dashboard
				if reqURL == "/dashboard?search=my+dashboard" {
					c = []Dashboard{testDashboard}
				} else if reqURL == "/dashboard?f__created_gt=1483639916" {
					c = []Dashboard{testDashboard}
				} else if reqURL == "/dashboard?f__created_gt=1483639916&search=my+dashboard" {
					c = []Dashboard{testDashboard}
				} else if reqURL == "/dashboard" {
					c = []Dashboard{testDashboard}
				} else {
					c = []Dashboard{}
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
				ret, err := json.Marshal(testDashboard)
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

func TestNewDashboard(t *testing.T) {
	bundle := NewDashboard()
	actualType := reflect.TypeOf(bundle)
	expectedType := "*api.Dashboard"
	if actualType.String() != expectedType {
		t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
	}
}

func TestFetchDashboard(t *testing.T) {
	server := testDashboardServer()
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
		expectedError := errors.New("Invalid dashboard CID [none]")
		_, err := apih.FetchDashboard(nil)
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
		expectedError := errors.New("Invalid dashboard CID [none]")
		_, err := apih.FetchDashboard(CIDType(&cid))
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
		expectedError := errors.New("Invalid dashboard CID [/invalid]")
		_, err := apih.FetchDashboard(CIDType(&cid))
		if err == nil {
			t.Fatalf("Expected error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID")
	{
		cid := "/dashboard/1234"
		dashboard, err := apih.FetchDashboard(CIDType(&cid))
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(dashboard)
		expectedType := "*api.Dashboard"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}

		if dashboard.CID != testDashboard.CID {
			t.Fatalf("CIDs do not match: %+v != %+v\n", dashboard, testDashboard)
		}
	}
}

func TestFetchDashboards(t *testing.T) {
	server := testDashboardServer()
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

	dashboards, err := apih.FetchDashboards()
	if err != nil {
		t.Fatalf("Expected no error, got '%v'", err)
	}

	actualType := reflect.TypeOf(dashboards)
	expectedType := "*[]api.Dashboard"
	if actualType.String() != expectedType {
		t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
	}

}

func TestUpdateDashboard(t *testing.T) {
	server := testDashboardServer()
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
		expectedError := errors.New("Invalid dashboard config [nil]")
		_, err := apih.UpdateDashboard(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid config [CID /invalid]")
	{
		expectedError := errors.New("Invalid dashboard CID [/invalid]")
		x := &Dashboard{CID: "/invalid"}
		_, err := apih.UpdateDashboard(x)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		dashboard, err := apih.UpdateDashboard(&testDashboard)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(dashboard)
		expectedType := "*api.Dashboard"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}

func TestCreateDashboard(t *testing.T) {
	server := testDashboardServer()
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
		expectedError := errors.New("Invalid dashboard config [nil]")
		_, err := apih.CreateDashboard(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		dashboard, err := apih.CreateDashboard(&testDashboard)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(dashboard)
		expectedType := "*api.Dashboard"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}

func TestDeleteDashboard(t *testing.T) {
	server := testDashboardServer()
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
		expectedError := errors.New("Invalid dashboard config [nil]")
		_, err := apih.DeleteDashboard(nil)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("invalid config [CID /invalid]")
	{
		expectedError := errors.New("Invalid dashboard CID [/invalid]")
		x := &Dashboard{CID: "/invalid"}
		_, err := apih.DeleteDashboard(x)
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid config")
	{
		_, err := apih.DeleteDashboard(&testDashboard)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}
	}
}

func TestDeleteDashboardByCID(t *testing.T) {
	server := testDashboardServer()
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
		expectedError := errors.New("Invalid dashboard CID [none]")
		_, err := apih.DeleteDashboardByCID(nil)
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
		expectedError := errors.New("Invalid dashboard CID [none]")
		_, err := apih.DeleteDashboardByCID(CIDType(&cid))
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
		expectedError := errors.New("Invalid dashboard CID [/invalid]")
		_, err := apih.DeleteDashboardByCID(CIDType(&cid))
		if err == nil {
			t.Fatal("Expected an error")
		}
		if err.Error() != expectedError.Error() {
			t.Fatalf("Expected %+v got '%+v'", expectedError, err)
		}
	}

	t.Log("valid CID")
	{
		cid := "/dashboard/1234"
		_, err := apih.DeleteDashboardByCID(CIDType(&cid))
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}
	}
}

func TestSearchDashboards(t *testing.T) {
	server := testDashboardServer()
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

	search := SearchQueryType("my dashboard")
	filter := SearchFilterType(map[string][]string{"f__created_gt": {"1483639916"}})

	t.Log("no search, no filter")
	{
		dashboards, err := apih.SearchDashboards(nil, nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(dashboards)
		expectedType := "*[]api.Dashboard"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("search, no filter")
	{
		dashboards, err := apih.SearchDashboards(&search, nil)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(dashboards)
		expectedType := "*[]api.Dashboard"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("no search, filter")
	{
		dashboards, err := apih.SearchDashboards(nil, &filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(dashboards)
		expectedType := "*[]api.Dashboard"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}

	t.Log("search, filter")
	{
		dashboards, err := apih.SearchDashboards(&search, &filter)
		if err != nil {
			t.Fatalf("Expected no error, got '%v'", err)
		}

		actualType := reflect.TypeOf(dashboards)
		expectedType := "*[]api.Dashboard"
		if actualType.String() != expectedType {
			t.Fatalf("Expected %s, got %s", expectedType, actualType.String())
		}
	}
}
