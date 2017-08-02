package agent

import (
	"log"
	"net"
	"net/http"
	"reflect"
	"testing"
)

func TestHTTPServer_AllocsRequest(t *testing.T) {
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
			got, err := s.AllocsRequest(tt.args.resp, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.AllocsRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.AllocsRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_AllocSpecificRequest(t *testing.T) {
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
			got, err := s.AllocSpecificRequest(tt.args.resp, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.AllocSpecificRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.AllocSpecificRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_ClientAllocRequest(t *testing.T) {
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
			got, err := s.ClientAllocRequest(tt.args.resp, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.ClientAllocRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.ClientAllocRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_allocStats(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	type args struct {
		allocID string
		resp    http.ResponseWriter
		req     *http.Request
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
			got, err := s.allocStats(tt.args.allocID, tt.args.resp, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.allocStats() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.allocStats() = %v, want %v", got, tt.want)
			}
		})
	}
}
