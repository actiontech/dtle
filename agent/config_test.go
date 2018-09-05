/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package agent

import (
	"net"
	"reflect"
	"testing"
	"time"
	uconf "udup/internal/config"
)

func TestDefaultConfig(t *testing.T) {
	tests := []struct {
		name string
		want *Config
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DefaultConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DefaultConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_Listener(t *testing.T) {
	type fields struct {
		Region                 string
		Datacenter             string
		NodeName               string
		DataDir                string
		LogLevel               string
		PidFile                string
		LogFile                string
		BindAddr               string
		Ports                  *Ports
		Addresses              *Addresses
		normalizedAddrs        *Addresses
		AdvertiseAddrs         *AdvertiseAddrs
		Client                 *ClientConfig
		Server                 *ServerConfig
		Metric                 *Metric
		Network                *Network
		LeaveOnInt             bool
		LeaveOnTerm            bool
		Consul                 *uconf.ConsulConfig
		UdupConfig             *uconf.ServerConfig
		ClientConfig           *uconf.ClientConfig
		Version                string
		Files                  []string
		HTTPAPIResponseHeaders map[string]string
	}
	type args struct {
		proto string
		addr  string
		port  int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    net.Listener
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{
				Region:          tt.fields.Region,
				Datacenter:      tt.fields.Datacenter,
				NodeName:        tt.fields.NodeName,
				DataDir:         tt.fields.DataDir,
				LogLevel:        tt.fields.LogLevel,
				PidFile:         tt.fields.PidFile,
				LogFile:         tt.fields.LogFile,
				BindAddr:        tt.fields.BindAddr,
				Ports:           tt.fields.Ports,
				Addresses:       tt.fields.Addresses,
				normalizedAddrs: tt.fields.normalizedAddrs,
				AdvertiseAddrs:  tt.fields.AdvertiseAddrs,
				Client:          tt.fields.Client,
				Server:          tt.fields.Server,
				Metric:          tt.fields.Metric,
				Network:         tt.fields.Network,
				LeaveOnInt:      tt.fields.LeaveOnInt,
				LeaveOnTerm:     tt.fields.LeaveOnTerm,
				Consul:          tt.fields.Consul,
				UdupConfig:      tt.fields.UdupConfig,
				ClientConfig:    tt.fields.ClientConfig,
				Version:         tt.fields.Version,
				Files:           tt.fields.Files,
				HTTPAPIResponseHeaders: tt.fields.HTTPAPIResponseHeaders,
			}
			got, err := c.Listener(tt.args.proto, tt.args.addr, tt.args.port)
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Listener() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Config.Listener() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_Merge(t *testing.T) {
	type fields struct {
		Region                 string
		Datacenter             string
		NodeName               string
		DataDir                string
		LogLevel               string
		PidFile                string
		LogFile                string
		BindAddr               string
		Ports                  *Ports
		Addresses              *Addresses
		normalizedAddrs        *Addresses
		AdvertiseAddrs         *AdvertiseAddrs
		Client                 *ClientConfig
		Server                 *ServerConfig
		Metric                 *Metric
		Network                *Network
		LeaveOnInt             bool
		LeaveOnTerm            bool
		Consul                 *uconf.ConsulConfig
		UdupConfig             *uconf.ServerConfig
		ClientConfig           *uconf.ClientConfig
		Version                string
		Files                  []string
		HTTPAPIResponseHeaders map[string]string
	}
	type args struct {
		b *Config
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
			c := &Config{
				Region:          tt.fields.Region,
				Datacenter:      tt.fields.Datacenter,
				NodeName:        tt.fields.NodeName,
				DataDir:         tt.fields.DataDir,
				LogLevel:        tt.fields.LogLevel,
				PidFile:         tt.fields.PidFile,
				LogFile:         tt.fields.LogFile,
				BindAddr:        tt.fields.BindAddr,
				Ports:           tt.fields.Ports,
				Addresses:       tt.fields.Addresses,
				normalizedAddrs: tt.fields.normalizedAddrs,
				AdvertiseAddrs:  tt.fields.AdvertiseAddrs,
				Client:          tt.fields.Client,
				Server:          tt.fields.Server,
				Metric:          tt.fields.Metric,
				Network:         tt.fields.Network,
				LeaveOnInt:      tt.fields.LeaveOnInt,
				LeaveOnTerm:     tt.fields.LeaveOnTerm,
				Consul:          tt.fields.Consul,
				UdupConfig:      tt.fields.UdupConfig,
				ClientConfig:    tt.fields.ClientConfig,
				Version:         tt.fields.Version,
				Files:           tt.fields.Files,
				HTTPAPIResponseHeaders: tt.fields.HTTPAPIResponseHeaders,
			}
			if got := c.Merge(tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Config.Merge() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_normalizeAddrs(t *testing.T) {
	type fields struct {
		Region                 string
		Datacenter             string
		NodeName               string
		DataDir                string
		LogLevel               string
		PidFile                string
		LogFile                string
		BindAddr               string
		Ports                  *Ports
		Addresses              *Addresses
		normalizedAddrs        *Addresses
		AdvertiseAddrs         *AdvertiseAddrs
		Client                 *ClientConfig
		Server                 *ServerConfig
		Metric                 *Metric
		Network                *Network
		LeaveOnInt             bool
		LeaveOnTerm            bool
		Consul                 *uconf.ConsulConfig
		UdupConfig             *uconf.ServerConfig
		ClientConfig           *uconf.ClientConfig
		Version                string
		Files                  []string
		HTTPAPIResponseHeaders map[string]string
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
			c := &Config{
				Region:          tt.fields.Region,
				Datacenter:      tt.fields.Datacenter,
				NodeName:        tt.fields.NodeName,
				DataDir:         tt.fields.DataDir,
				LogLevel:        tt.fields.LogLevel,
				PidFile:         tt.fields.PidFile,
				LogFile:         tt.fields.LogFile,
				BindAddr:        tt.fields.BindAddr,
				Ports:           tt.fields.Ports,
				Addresses:       tt.fields.Addresses,
				normalizedAddrs: tt.fields.normalizedAddrs,
				AdvertiseAddrs:  tt.fields.AdvertiseAddrs,
				Client:          tt.fields.Client,
				Server:          tt.fields.Server,
				Metric:          tt.fields.Metric,
				Network:         tt.fields.Network,
				LeaveOnInt:      tt.fields.LeaveOnInt,
				LeaveOnTerm:     tt.fields.LeaveOnTerm,
				Consul:          tt.fields.Consul,
				UdupConfig:      tt.fields.UdupConfig,
				ClientConfig:    tt.fields.ClientConfig,
				Version:         tt.fields.Version,
				Files:           tt.fields.Files,
				HTTPAPIResponseHeaders: tt.fields.HTTPAPIResponseHeaders,
			}
			if err := c.normalizeAddrs(); (err != nil) != tt.wantErr {
				t.Errorf("Config.normalizeAddrs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_normalizeBind(t *testing.T) {
	type args struct {
		addr string
		bind string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeBind(tt.args.addr, tt.args.bind); got != tt.want {
				t.Errorf("normalizeBind() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_normalizeAdvertise(t *testing.T) {
	type args struct {
		addr    string
		bind    string
		defport int
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeAdvertise(tt.args.addr, tt.args.bind, tt.args.defport)
			if (err != nil) != tt.wantErr {
				t.Errorf("normalizeAdvertise() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("normalizeAdvertise() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_isMissingPort(t *testing.T) {
	type args struct {
		err error
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
			if got := isMissingPort(tt.args.err); got != tt.want {
				t.Errorf("isMissingPort() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServerConfig_Merge(t *testing.T) {
	type fields struct {
		Enabled           bool
		BootstrapExpect   int
		DataDir           string
		NumSchedulers     int
		EnabledSchedulers []string
		HeartbeatGrace    string
		StartJoin         []string
		RetryMaxAttempts  int
		RetryInterval     string
		retryInterval     time.Duration
	}
	type args struct {
		b *ServerConfig
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ServerConfig
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &ServerConfig{
				Enabled:           tt.fields.Enabled,
				BootstrapExpect:   tt.fields.BootstrapExpect,
				DataDir:           tt.fields.DataDir,
				NumSchedulers:     tt.fields.NumSchedulers,
				EnabledSchedulers: tt.fields.EnabledSchedulers,
				HeartbeatGrace:    tt.fields.HeartbeatGrace,
				StartJoin:         tt.fields.StartJoin,
				RetryMaxAttempts:  tt.fields.RetryMaxAttempts,
				RetryInterval:     tt.fields.RetryInterval,
				retryInterval:     tt.fields.retryInterval,
			}
			if got := a.Merge(tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ServerConfig.Merge() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientConfig_Merge(t *testing.T) {
	type fields struct {
		Enabled    bool
		StateDir   string
		Servers    []string
		NoHostUUID bool
	}
	type args struct {
		b *ClientConfig
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ClientConfig
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &ClientConfig{
				Enabled:    tt.fields.Enabled,
				StateDir:   tt.fields.StateDir,
				Servers:    tt.fields.Servers,
				NoHostUUID: tt.fields.NoHostUUID,
			}
			if got := a.Merge(tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientConfig.Merge() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNetwork_Merge(t *testing.T) {
	type fields struct {
		MaxPayload int
	}
	type args struct {
		b *Network
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Network
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Network{
				MaxPayload: tt.fields.MaxPayload,
			}
			if got := a.Merge(tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Network.Merge() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMetric_Merge(t *testing.T) {
	type fields struct {
		DisableHostname          bool
		UseNodeName              bool
		CollectionInterval       string
		collectionInterval       time.Duration
		PublishAllocationMetrics bool
		PublishNodeMetrics       bool
	}
	type args struct {
		b *Metric
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Metric
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Metric{
				DisableHostname:          tt.fields.DisableHostname,
				UseNodeName:              tt.fields.UseNodeName,
				CollectionInterval:       tt.fields.CollectionInterval,
				collectionInterval:       tt.fields.collectionInterval,
				PublishAllocationMetrics: tt.fields.PublishAllocationMetrics,
				PublishNodeMetrics:       tt.fields.PublishNodeMetrics,
			}
			if got := a.Merge(tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Metric.Merge() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPorts_Merge(t *testing.T) {
	type fields struct {
		HTTP int
		RPC  int
		Serf int
		Nats int
	}
	type args struct {
		b *Ports
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Ports
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Ports{
				HTTP: tt.fields.HTTP,
				RPC:  tt.fields.RPC,
				Serf: tt.fields.Serf,
				Nats: tt.fields.Nats,
			}
			if got := a.Merge(tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Ports.Merge() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAddresses_Merge(t *testing.T) {
	type fields struct {
		HTTP string
		RPC  string
		Serf string
		Nats string
	}
	type args struct {
		b *Addresses
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Addresses
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Addresses{
				HTTP: tt.fields.HTTP,
				RPC:  tt.fields.RPC,
				Serf: tt.fields.Serf,
				Nats: tt.fields.Nats,
			}
			if got := a.Merge(tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Addresses.Merge() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAdvertiseAddrs_Merge(t *testing.T) {
	type fields struct {
		HTTP string
		RPC  string
		Serf string
		Nats string
	}
	type args struct {
		b *AdvertiseAddrs
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *AdvertiseAddrs
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &AdvertiseAddrs{
				HTTP: tt.fields.HTTP,
				RPC:  tt.fields.RPC,
				Serf: tt.fields.Serf,
				Nats: tt.fields.Nats,
			}
			if got := a.Merge(tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AdvertiseAddrs.Merge() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		want    *Config
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadConfig(tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LoadConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLoadConfigDir(t *testing.T) {
	type args struct {
		dir string
	}
	tests := []struct {
		name    string
		args    args
		want    *Config
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadConfigDir(tt.args.dir)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadConfigDir() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LoadConfigDir() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_isTemporaryFile(t *testing.T) {
	type args struct {
		name string
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
			if got := isTemporaryFile(tt.args.name); got != tt.want {
				t.Errorf("isTemporaryFile() = %v, want %v", got, tt.want)
			}
		})
	}
}
