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
	"testing"
	ulog "github.com/actiontech/dts/internal/logger"

	"github.com/mitchellh/cli"
)

func TestCommand_readConfig(t *testing.T) {
	type fields struct {
		Version        string
		Ui             cli.Ui
		ShutdownCh     <-chan struct{}
		args           []string
		agent          *Agent
		httpServer     *HTTPServer
		logger         *ulog.Logger
		logOutput      io.Writer
		retryJoinErrCh chan struct{}
	}
	tests := []struct {
		name   string
		fields fields
		want   *Config
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Command{
				Version:        tt.fields.Version,
				Ui:             tt.fields.Ui,
				ShutdownCh:     tt.fields.ShutdownCh,
				args:           tt.fields.args,
				agent:          tt.fields.agent,
				httpServer:     tt.fields.httpServer,
				logger:         tt.fields.logger,
				logOutput:      tt.fields.logOutput,
				retryJoinErrCh: tt.fields.retryJoinErrCh,
			}
			if got := c.readConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Command.readConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStringFlag_String(t *testing.T) {
	tests := []struct {
		name string
		s    *StringFlag
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.String(); got != tt.want {
				t.Errorf("StringFlag.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStringFlag_Set(t *testing.T) {
	type args struct {
		value string
	}
	tests := []struct {
		name    string
		s       *StringFlag
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.s.Set(tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("StringFlag.Set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCommand_setupLoggers(t *testing.T) {
	type fields struct {
		Version        string
		Ui             cli.Ui
		ShutdownCh     <-chan struct{}
		args           []string
		agent          *Agent
		httpServer     *HTTPServer
		logger         *ulog.Logger
		logOutput      io.Writer
		retryJoinErrCh chan struct{}
	}
	type args struct {
		config *Config
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    io.Writer
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Command{
				Version:        tt.fields.Version,
				Ui:             tt.fields.Ui,
				ShutdownCh:     tt.fields.ShutdownCh,
				args:           tt.fields.args,
				agent:          tt.fields.agent,
				httpServer:     tt.fields.httpServer,
				logger:         tt.fields.logger,
				logOutput:      tt.fields.logOutput,
				retryJoinErrCh: tt.fields.retryJoinErrCh,
			}
			got, err := c.setupLoggers(tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("Command.setupLoggers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got := in0.String(); got != tt.want {
				t.Errorf("Command.setupLoggers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCommand_setupAgent(t *testing.T) {
	type fields struct {
		Version        string
		Ui             cli.Ui
		ShutdownCh     <-chan struct{}
		args           []string
		agent          *Agent
		httpServer     *HTTPServer
		logger         *ulog.Logger
		logOutput      io.Writer
		retryJoinErrCh chan struct{}
	}
	type args struct {
		config *Config
	}
	tests := []struct {
		name          string
		fields        fields
		args          args
		wantLogOutput string
		wantErr       bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Command{
				Version:        tt.fields.Version,
				Ui:             tt.fields.Ui,
				ShutdownCh:     tt.fields.ShutdownCh,
				args:           tt.fields.args,
				agent:          tt.fields.agent,
				httpServer:     tt.fields.httpServer,
				logger:         tt.fields.logger,
				logOutput:      tt.fields.logOutput,
				retryJoinErrCh: tt.fields.retryJoinErrCh,
			}
			logOutput := &bytes.Buffer{}
			if err := c.setupAgent(tt.args.config, logOutput); (err != nil) != tt.wantErr {
				t.Errorf("Command.setupAgent() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotLogOutput := logOutput.String(); gotLogOutput != tt.wantLogOutput {
				t.Errorf("Command.setupAgent() = %v, want %v", gotLogOutput, tt.wantLogOutput)
			}
		})
	}
}

func TestCommand_Run(t *testing.T) {
	type fields struct {
		Version        string
		Ui             cli.Ui
		ShutdownCh     <-chan struct{}
		args           []string
		agent          *Agent
		httpServer     *HTTPServer
		logger         *ulog.Logger
		logOutput      io.Writer
		retryJoinErrCh chan struct{}
	}
	type args struct {
		args []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Command{
				Version:        tt.fields.Version,
				Ui:             tt.fields.Ui,
				ShutdownCh:     tt.fields.ShutdownCh,
				args:           tt.fields.args,
				agent:          tt.fields.agent,
				httpServer:     tt.fields.httpServer,
				logger:         tt.fields.logger,
				logOutput:      tt.fields.logOutput,
				retryJoinErrCh: tt.fields.retryJoinErrCh,
			}
			if got := c.Run(tt.args.args); got != tt.want {
				t.Errorf("Command.Run() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCommand_handleSignals(t *testing.T) {
	type fields struct {
		Version        string
		Ui             cli.Ui
		ShutdownCh     <-chan struct{}
		args           []string
		agent          *Agent
		httpServer     *HTTPServer
		logger         *ulog.Logger
		logOutput      io.Writer
		retryJoinErrCh chan struct{}
	}
	type args struct {
		config *Config
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Command{
				Version:        tt.fields.Version,
				Ui:             tt.fields.Ui,
				ShutdownCh:     tt.fields.ShutdownCh,
				args:           tt.fields.args,
				agent:          tt.fields.agent,
				httpServer:     tt.fields.httpServer,
				logger:         tt.fields.logger,
				logOutput:      tt.fields.logOutput,
				retryJoinErrCh: tt.fields.retryJoinErrCh,
			}
			if got := c.handleSignals(tt.args.config); got != tt.want {
				t.Errorf("Command.handleSignals() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCommand_handleReload(t *testing.T) {
	type fields struct {
		Version        string
		Ui             cli.Ui
		ShutdownCh     <-chan struct{}
		args           []string
		agent          *Agent
		httpServer     *HTTPServer
		logger         *ulog.Logger
		logOutput      io.Writer
		retryJoinErrCh chan struct{}
	}
	type args struct {
		config *Config
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Config
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Command{
				Version:        tt.fields.Version,
				Ui:             tt.fields.Ui,
				ShutdownCh:     tt.fields.ShutdownCh,
				args:           tt.fields.args,
				agent:          tt.fields.agent,
				httpServer:     tt.fields.httpServer,
				logger:         tt.fields.logger,
				logOutput:      tt.fields.logOutput,
				retryJoinErrCh: tt.fields.retryJoinErrCh,
			}
			if got := c.handleReload(tt.args.config); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Command.handleReload() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCommand_setupMetric(t *testing.T) {
	type fields struct {
		Version        string
		Ui             cli.Ui
		ShutdownCh     <-chan struct{}
		args           []string
		agent          *Agent
		httpServer     *HTTPServer
		logger         *ulog.Logger
		logOutput      io.Writer
		retryJoinErrCh chan struct{}
	}
	type args struct {
		config *Config
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
			c := &Command{
				Version:        tt.fields.Version,
				Ui:             tt.fields.Ui,
				ShutdownCh:     tt.fields.ShutdownCh,
				args:           tt.fields.args,
				agent:          tt.fields.agent,
				httpServer:     tt.fields.httpServer,
				logger:         tt.fields.logger,
				logOutput:      tt.fields.logOutput,
				retryJoinErrCh: tt.fields.retryJoinErrCh,
			}
			if err := c.setupMetric(tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("Command.setupMetric() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCommand_startupJoin(t *testing.T) {
	type fields struct {
		Version        string
		Ui             cli.Ui
		ShutdownCh     <-chan struct{}
		args           []string
		agent          *Agent
		httpServer     *HTTPServer
		logger         *ulog.Logger
		logOutput      io.Writer
		retryJoinErrCh chan struct{}
	}
	type args struct {
		config *Config
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
			c := &Command{
				Version:        tt.fields.Version,
				Ui:             tt.fields.Ui,
				ShutdownCh:     tt.fields.ShutdownCh,
				args:           tt.fields.args,
				agent:          tt.fields.agent,
				httpServer:     tt.fields.httpServer,
				logger:         tt.fields.logger,
				logOutput:      tt.fields.logOutput,
				retryJoinErrCh: tt.fields.retryJoinErrCh,
			}
			if err := c.startupJoin(tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("Command.startupJoin() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCommand_retryJoin(t *testing.T) {
	type fields struct {
		Version        string
		Ui             cli.Ui
		ShutdownCh     <-chan struct{}
		args           []string
		agent          *Agent
		httpServer     *HTTPServer
		logger         *ulog.Logger
		logOutput      io.Writer
		retryJoinErrCh chan struct{}
	}
	type args struct {
		config *Config
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
			c := &Command{
				Version:        tt.fields.Version,
				Ui:             tt.fields.Ui,
				ShutdownCh:     tt.fields.ShutdownCh,
				args:           tt.fields.args,
				agent:          tt.fields.agent,
				httpServer:     tt.fields.httpServer,
				logger:         tt.fields.logger,
				logOutput:      tt.fields.logOutput,
				retryJoinErrCh: tt.fields.retryJoinErrCh,
			}
			c.retryJoin(tt.args.config)
		})
	}
}

func TestCommand_Synopsis(t *testing.T) {
	type fields struct {
		Version        string
		Ui             cli.Ui
		ShutdownCh     <-chan struct{}
		args           []string
		agent          *Agent
		httpServer     *HTTPServer
		logger         *ulog.Logger
		logOutput      io.Writer
		retryJoinErrCh chan struct{}
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
			c := &Command{
				Version:        tt.fields.Version,
				Ui:             tt.fields.Ui,
				ShutdownCh:     tt.fields.ShutdownCh,
				args:           tt.fields.args,
				agent:          tt.fields.agent,
				httpServer:     tt.fields.httpServer,
				logger:         tt.fields.logger,
				logOutput:      tt.fields.logOutput,
				retryJoinErrCh: tt.fields.retryJoinErrCh,
			}
			if got := c.Synopsis(); got != tt.want {
				t.Errorf("Command.Synopsis() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCommand_Help(t *testing.T) {
	type fields struct {
		Version        string
		Ui             cli.Ui
		ShutdownCh     <-chan struct{}
		args           []string
		agent          *Agent
		httpServer     *HTTPServer
		logger         *ulog.Logger
		logOutput      io.Writer
		retryJoinErrCh chan struct{}
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
			c := &Command{
				Version:        tt.fields.Version,
				Ui:             tt.fields.Ui,
				ShutdownCh:     tt.fields.ShutdownCh,
				args:           tt.fields.args,
				agent:          tt.fields.agent,
				httpServer:     tt.fields.httpServer,
				logger:         tt.fields.logger,
				logOutput:      tt.fields.logOutput,
				retryJoinErrCh: tt.fields.retryJoinErrCh,
			}
			if got := c.Help(); got != tt.want {
				t.Errorf("Command.Help() = %v, want %v", got, tt.want)
			}
		})
	}
}
