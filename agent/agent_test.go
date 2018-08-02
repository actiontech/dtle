/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package agent

import (
	"bytes"
	"io"
	"reflect"
	"sync"
	"testing"
	ucli "udup/internal/client"
	uconf "udup/internal/config"
	ulog "udup/internal/logger"
	usrv "udup/internal/server"
)

func TestNewAgent(t *testing.T) {
	type args struct {
		config *Config
		log    *ulog.Logger
	}
	tests := []struct {
		name          string
		args          args
		want          *Agent
		wantLogOutput string
		wantErr       bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logOutput := &bytes.Buffer{}
			got, err := NewAgent(tt.args.config, logOutput, tt.args.log)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewAgent() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewAgent() = %v, want %v", got, tt.want)
			}
			if gotLogOutput := logOutput.String(); gotLogOutput != tt.wantLogOutput {
				t.Errorf("NewAgent() = %v, want %v", gotLogOutput, tt.wantLogOutput)
			}
		})
	}
}

func Test_convertServerConfig(t *testing.T) {
	type args struct {
		agentConfig *Config
	}
	tests := []struct {
		name          string
		args          args
		want          *uconf.ServerConfig
		wantLogOutput string
		wantErr       bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logOutput := &bytes.Buffer{}
			got, err := convertServerConfig(tt.args.agentConfig, logOutput)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertServerConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertServerConfig() = %v, want %v", got, tt.want)
			}
			if gotLogOutput := logOutput.String(); gotLogOutput != tt.wantLogOutput {
				t.Errorf("convertServerConfig() = %v, want %v", gotLogOutput, tt.wantLogOutput)
			}
		})
	}
}

func TestAgent_serverConfig(t *testing.T) {
	type fields struct {
		config       *Config
		logger       *ulog.Logger
		logOutput    io.Writer
		client       *ucli.Client
		server       *usrv.Server
		shutdown     bool
		shutdownCh   chan struct{}
		shutdownLock sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		want    *uconf.ServerConfig
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Agent{
				config:       tt.fields.config,
				logger:       tt.fields.logger,
				logOutput:    tt.fields.logOutput,
				client:       tt.fields.client,
				server:       tt.fields.server,
				shutdown:     tt.fields.shutdown,
				shutdownCh:   tt.fields.shutdownCh,
				shutdownLock: tt.fields.shutdownLock,
			}
			got, err := a.serverConfig()
			if (err != nil) != tt.wantErr {
				t.Errorf("Agent.serverConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Agent.serverConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAgent_clientConfig(t *testing.T) {
	type fields struct {
		config       *Config
		logger       *ulog.Logger
		logOutput    io.Writer
		client       *ucli.Client
		server       *usrv.Server
		shutdown     bool
		shutdownCh   chan struct{}
		shutdownLock sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		want    *uconf.ClientConfig
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Agent{
				config:       tt.fields.config,
				logger:       tt.fields.logger,
				logOutput:    tt.fields.logOutput,
				client:       tt.fields.client,
				server:       tt.fields.server,
				shutdown:     tt.fields.shutdown,
				shutdownCh:   tt.fields.shutdownCh,
				shutdownLock: tt.fields.shutdownLock,
			}
			got, err := a.clientConfig()
			if (err != nil) != tt.wantErr {
				t.Errorf("Agent.clientConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Agent.clientConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAgent_setupServer(t *testing.T) {
	type fields struct {
		config       *Config
		logger       *ulog.Logger
		logOutput    io.Writer
		client       *ucli.Client
		server       *usrv.Server
		shutdown     bool
		shutdownCh   chan struct{}
		shutdownLock sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Agent{
				config:       tt.fields.config,
				logger:       tt.fields.logger,
				logOutput:    tt.fields.logOutput,
				client:       tt.fields.client,
				server:       tt.fields.server,
				shutdown:     tt.fields.shutdown,
				shutdownCh:   tt.fields.shutdownCh,
				shutdownLock: tt.fields.shutdownLock,
			}
			if err := a.setupServer(); (err != nil) != tt.wantErr {
				t.Errorf("Agent.setupServer() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAgent_setupClient(t *testing.T) {
	type fields struct {
		config       *Config
		logger       *ulog.Logger
		logOutput    io.Writer
		client       *ucli.Client
		server       *usrv.Server
		shutdown     bool
		shutdownCh   chan struct{}
		shutdownLock sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Agent{
				config:       tt.fields.config,
				logger:       tt.fields.logger,
				logOutput:    tt.fields.logOutput,
				client:       tt.fields.client,
				server:       tt.fields.server,
				shutdown:     tt.fields.shutdown,
				shutdownCh:   tt.fields.shutdownCh,
				shutdownLock: tt.fields.shutdownLock,
			}
			if err := a.setupClient(); (err != nil) != tt.wantErr {
				t.Errorf("Agent.setupClient() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAgent_Leave(t *testing.T) {
	type fields struct {
		config       *Config
		logger       *ulog.Logger
		logOutput    io.Writer
		client       *ucli.Client
		server       *usrv.Server
		shutdown     bool
		shutdownCh   chan struct{}
		shutdownLock sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Agent{
				config:       tt.fields.config,
				logger:       tt.fields.logger,
				logOutput:    tt.fields.logOutput,
				client:       tt.fields.client,
				server:       tt.fields.server,
				shutdown:     tt.fields.shutdown,
				shutdownCh:   tt.fields.shutdownCh,
				shutdownLock: tt.fields.shutdownLock,
			}
			if err := a.Leave(); (err != nil) != tt.wantErr {
				t.Errorf("Agent.Leave() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAgent_Shutdown(t *testing.T) {
	type fields struct {
		config       *Config
		logger       *ulog.Logger
		logOutput    io.Writer
		client       *ucli.Client
		server       *usrv.Server
		shutdown     bool
		shutdownCh   chan struct{}
		shutdownLock sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Agent{
				config:       tt.fields.config,
				logger:       tt.fields.logger,
				logOutput:    tt.fields.logOutput,
				client:       tt.fields.client,
				server:       tt.fields.server,
				shutdown:     tt.fields.shutdown,
				shutdownCh:   tt.fields.shutdownCh,
				shutdownLock: tt.fields.shutdownLock,
			}
			if err := a.Shutdown(); (err != nil) != tt.wantErr {
				t.Errorf("Agent.Shutdown() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAgent_RPC(t *testing.T) {
	type fields struct {
		config       *Config
		logger       *ulog.Logger
		logOutput    io.Writer
		client       *ucli.Client
		server       *usrv.Server
		shutdown     bool
		shutdownCh   chan struct{}
		shutdownLock sync.Mutex
	}
	type args struct {
		method string
		args   interface{}
		reply  interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Agent{
				config:       tt.fields.config,
				logger:       tt.fields.logger,
				logOutput:    tt.fields.logOutput,
				client:       tt.fields.client,
				server:       tt.fields.server,
				shutdown:     tt.fields.shutdown,
				shutdownCh:   tt.fields.shutdownCh,
				shutdownLock: tt.fields.shutdownLock,
			}
			if err := a.RPC(tt.args.method, tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Agent.RPC() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAgent_Client(t *testing.T) {
	type fields struct {
		config       *Config
		logger       *ulog.Logger
		logOutput    io.Writer
		client       *ucli.Client
		server       *usrv.Server
		shutdown     bool
		shutdownCh   chan struct{}
		shutdownLock sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   *ucli.Client
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Agent{
				config:       tt.fields.config,
				logger:       tt.fields.logger,
				logOutput:    tt.fields.logOutput,
				client:       tt.fields.client,
				server:       tt.fields.server,
				shutdown:     tt.fields.shutdown,
				shutdownCh:   tt.fields.shutdownCh,
				shutdownLock: tt.fields.shutdownLock,
			}
			if got := a.Client(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Agent.Client() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAgent_Server(t *testing.T) {
	type fields struct {
		config       *Config
		logger       *ulog.Logger
		logOutput    io.Writer
		client       *ucli.Client
		server       *usrv.Server
		shutdown     bool
		shutdownCh   chan struct{}
		shutdownLock sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   *usrv.Server
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Agent{
				config:       tt.fields.config,
				logger:       tt.fields.logger,
				logOutput:    tt.fields.logOutput,
				client:       tt.fields.client,
				server:       tt.fields.server,
				shutdown:     tt.fields.shutdown,
				shutdownCh:   tt.fields.shutdownCh,
				shutdownLock: tt.fields.shutdownLock,
			}
			if got := a.Server(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Agent.Server() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAgent_Stats(t *testing.T) {
	type fields struct {
		config       *Config
		logger       *ulog.Logger
		logOutput    io.Writer
		client       *ucli.Client
		server       *usrv.Server
		shutdown     bool
		shutdownCh   chan struct{}
		shutdownLock sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string]map[string]string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Agent{
				config:       tt.fields.config,
				logger:       tt.fields.logger,
				logOutput:    tt.fields.logOutput,
				client:       tt.fields.client,
				server:       tt.fields.server,
				shutdown:     tt.fields.shutdown,
				shutdownCh:   tt.fields.shutdownCh,
				shutdownLock: tt.fields.shutdownLock,
			}
			if got := a.Stats(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Agent.Stats() = %v, want %v", got, tt.want)
			}
		})
	}
}
