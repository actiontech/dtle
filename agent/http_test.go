/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package agent

import (
	"bytes"
	"net"
	"net/http"
	_ "net/http/pprof"
	"reflect"
	"testing"
	"time"
	log "github.com/actiontech/dts/internal/logger"
	umodel "github.com/actiontech/dts/internal/models"
)

func TestNewHTTPServer(t *testing.T) {
	type args struct {
		agent  *Agent
		config *Config
	}
	tests := []struct {
		name          string
		args          args
		want          *HTTPServer
		wantLogOutput string
		wantErr       bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logOutput := &bytes.Buffer{}
			got, err := NewHTTPServer(tt.args.agent, tt.args.config, logOutput)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewHTTPServer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewHTTPServer() = %v, want %v", got, tt.want)
			}
			if gotLogOutput := logOutput.String(); gotLogOutput != tt.wantLogOutput {
				t.Errorf("NewHTTPServer() = %v, want %v", gotLogOutput, tt.wantLogOutput)
			}
		})
	}
}

func Test_tcpKeepAliveListener_Accept(t *testing.T) {
	type fields struct {
		TCPListener *net.TCPListener
	}
	tests := []struct {
		name    string
		fields  fields
		wantC   net.Conn
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ln := tcpKeepAliveListener{
				TCPListener: tt.fields.TCPListener,
			}
			gotC, err := ln.Accept()
			if (err != nil) != tt.wantErr {
				t.Errorf("tcpKeepAliveListener.Accept() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotC, tt.wantC) {
				t.Errorf("tcpKeepAliveListener.Accept() = %v, want %v", gotC, tt.wantC)
			}
		})
	}
}

func TestHTTPServer_Shutdown(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	tests := []struct {
		name   string
		fields fields
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
			s.Shutdown()
		})
	}
}

func TestHTTPServer_registerHandlers(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	tests := []struct {
		name   string
		fields fields
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
			s.registerHandlers()
		})
	}
}

func TestCodedError(t *testing.T) {
	type args struct {
		c int
		s string
	}
	tests := []struct {
		name string
		args args
		want HTTPCodedError
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CodedError(tt.args.c, tt.args.s); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CodedError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_codedError_Error(t *testing.T) {
	type fields struct {
		s    string
		code int
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &codedError{
				s:    tt.fields.s,
				code: tt.fields.code,
			}
			if got := e.Error(); got != tt.want {
				t.Errorf("codedError.Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_codedError_Code(t *testing.T) {
	type fields struct {
		s    string
		code int
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &codedError{
				s:    tt.fields.s,
				code: tt.fields.code,
			}
			if got := e.Code(); got != tt.want {
				t.Errorf("codedError.Code() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_wrap(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	type args struct {
		handler func(resp http.ResponseWriter, req *http.Request) (interface{}, error)
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   func(resp http.ResponseWriter, req *http.Request)
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
			if got := s.wrap(tt.args.handler); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.wrap() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_decodeBody(t *testing.T) {
	type args struct {
		req *http.Request
		out interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := decodeBody(tt.args.req, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("decodeBody() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_setIndex(t *testing.T) {
	type args struct {
		resp  http.ResponseWriter
		index uint64
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setIndex(tt.args.resp, tt.args.index)
		})
	}
}

func Test_setKnownLeader(t *testing.T) {
	type args struct {
		resp  http.ResponseWriter
		known bool
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setKnownLeader(tt.args.resp, tt.args.known)
		})
	}
}

func Test_setLastContact(t *testing.T) {
	type args struct {
		resp http.ResponseWriter
		last time.Duration
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setLastContact(tt.args.resp, tt.args.last)
		})
	}
}

func Test_setMeta(t *testing.T) {
	type args struct {
		resp http.ResponseWriter
		m    *umodel.QueryMeta
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setMeta(tt.args.resp, tt.args.m)
		})
	}
}

func Test_setHeaders(t *testing.T) {
	type args struct {
		resp    http.ResponseWriter
		headers map[string]string
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setHeaders(tt.args.resp, tt.args.headers)
		})
	}
}

func Test_parseWait(t *testing.T) {
	type args struct {
		resp http.ResponseWriter
		req  *http.Request
		b    *umodel.QueryOptions
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseWait(tt.args.resp, tt.args.req, tt.args.b); got != tt.want {
				t.Errorf("parseWait() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseConsistency(t *testing.T) {
	type args struct {
		req *http.Request
		b   *umodel.QueryOptions
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parseConsistency(tt.args.req, tt.args.b)
		})
	}
}

func Test_parsePrefix(t *testing.T) {
	type args struct {
		req *http.Request
		b   *umodel.QueryOptions
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsePrefix(tt.args.req, tt.args.b)
		})
	}
}

func TestHTTPServer_parseRegion(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	type args struct {
		req *http.Request
		r   *string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
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
			s.parseRegion(tt.args.req, tt.args.r)
		})
	}
}

func TestHTTPServer_parse(t *testing.T) {
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
		r    *string
		b    *umodel.QueryOptions
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
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
			if got := s.parse(tt.args.resp, tt.args.req, tt.args.r, tt.args.b); got != tt.want {
				t.Errorf("HTTPServer.parse() = %v, want %v", got, tt.want)
			}
		})
	}
}
