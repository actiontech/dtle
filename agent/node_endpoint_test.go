package agent

import (
	"log"
	"net"
	"net/http"
	"reflect"
	"testing"
)

func TestHTTPServer_NodesRequest(t *testing.T) {
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
			got, err := s.NodesRequest(tt.args.resp, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.NodesRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.NodesRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_NodeSpecificRequest(t *testing.T) {
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
			got, err := s.NodeSpecificRequest(tt.args.resp, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.NodeSpecificRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.NodeSpecificRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_nodeForceEvaluate(t *testing.T) {
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
		nodeID string
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
			got, err := s.nodeForceEvaluate(tt.args.resp, tt.args.req, tt.args.nodeID)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.nodeForceEvaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.nodeForceEvaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_nodeAllocations(t *testing.T) {
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
		nodeID string
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
			got, err := s.nodeAllocations(tt.args.resp, tt.args.req, tt.args.nodeID)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.nodeAllocations() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.nodeAllocations() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_nodeQuery(t *testing.T) {
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
		nodeID string
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
			got, err := s.nodeQuery(tt.args.resp, tt.args.req, tt.args.nodeID)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.nodeQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.nodeQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}
