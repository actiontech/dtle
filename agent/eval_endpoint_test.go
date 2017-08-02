package agent

import (
	"log"
	"net"
	"net/http"
	"reflect"
	"testing"
)

func TestHTTPServer_EvalsRequest(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	type args struct {
		resp http.ResponseWriter
		req  *http.Request
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &HTTPServer{
				agent:    tt.fields.agent,
				mux:      tt.fields.mux,
				listener: tt.fields.listener,
				logger:   tt.fields.logger,
				addr:     tt.fields.addr,
			}
			got, err := s.EvalsRequest(tt.args.resp, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.EvalsRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.EvalsRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_EvalSpecificRequest(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	type args struct {
		resp http.ResponseWriter
		req  *http.Request
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &HTTPServer{
				agent:    tt.fields.agent,
				mux:      tt.fields.mux,
				listener: tt.fields.listener,
				logger:   tt.fields.logger,
				addr:     tt.fields.addr,
			}
			got, err := s.EvalSpecificRequest(tt.args.resp, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.EvalSpecificRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.EvalSpecificRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_evalAllocations(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	type args struct {
		resp   http.ResponseWriter
		req    *http.Request
		evalID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &HTTPServer{
				agent:    tt.fields.agent,
				mux:      tt.fields.mux,
				listener: tt.fields.listener,
				logger:   tt.fields.logger,
				addr:     tt.fields.addr,
			}
			got, err := s.evalAllocations(tt.args.resp, tt.args.req, tt.args.evalID)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.evalAllocations() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.evalAllocations() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_evalQuery(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	type args struct {
		resp   http.ResponseWriter
		req    *http.Request
		evalID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &HTTPServer{
				agent:    tt.fields.agent,
				mux:      tt.fields.mux,
				listener: tt.fields.listener,
				logger:   tt.fields.logger,
				addr:     tt.fields.addr,
			}
			got, err := s.evalQuery(tt.args.resp, tt.args.req, tt.args.evalID)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.evalQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.evalQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}
