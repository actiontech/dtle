package agent

import (
	"log"
	"net"
	"net/http"
	"reflect"
	"testing"

	"github.com/hashicorp/serf/serf"
)

func Test_udupMember(t *testing.T) {
	type args struct {
		m serf.Member
	}
	tests := []struct {
		name string
		args args
		want Member
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := udupMember(tt.args.m); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("udupMember() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_AgentSelfRequest(t *testing.T) {
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
			got, err := s.AgentSelfRequest(tt.args.resp, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.AgentSelfRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.AgentSelfRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_AgentJoinRequest(t *testing.T) {
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
			got, err := s.AgentJoinRequest(tt.args.resp, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.AgentJoinRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.AgentJoinRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_AgentMembersRequest(t *testing.T) {
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
			got, err := s.AgentMembersRequest(tt.args.resp, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.AgentMembersRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.AgentMembersRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_AgentForceLeaveRequest(t *testing.T) {
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
			got, err := s.AgentForceLeaveRequest(tt.args.resp, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.AgentForceLeaveRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.AgentForceLeaveRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_AgentServersRequest(t *testing.T) {
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
			got, err := s.AgentServersRequest(tt.args.resp, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.AgentServersRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.AgentServersRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_listServers(t *testing.T) {
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
			got, err := s.listServers(tt.args.resp, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.listServers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.listServers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_updateServers(t *testing.T) {
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
			got, err := s.updateServers(tt.args.resp, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.updateServers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.updateServers() = %v, want %v", got, tt.want)
			}
		})
	}
}
