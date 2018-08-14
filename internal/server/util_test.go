package server

import (
	"net"
	"reflect"
	"testing"

	"github.com/hashicorp/serf/serf"
)

func Test_ensurePath(t *testing.T) {
	type args struct {
		path string
		dir  bool
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
			if err := ensurePath(tt.args.path, tt.args.dir); (err != nil) != tt.wantErr {
				t.Errorf("ensurePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_serverParts_String(t *testing.T) {
	type fields struct {
		Name       string
		Region     string
		Datacenter string
		Port       int
		Bootstrap  bool
		Expect     int
		Addr       net.Addr
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
			s := &serverParts{
				Name:       tt.fields.Name,
				Region:     tt.fields.Region,
				Datacenter: tt.fields.Datacenter,
				Port:       tt.fields.Port,
				Bootstrap:  tt.fields.Bootstrap,
				Expect:     tt.fields.Expect,
				Addr:       tt.fields.Addr,
			}
			if got := s.String(); got != tt.want {
				t.Errorf("serverParts.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_isUdupServer(t *testing.T) {
	type args struct {
		m serf.Member
	}
	tests := []struct {
		name  string
		args  args
		want  bool
		want1 *serverParts
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := isUdupServer(tt.args.m)
			if got != tt.want {
				t.Errorf("isUdupServer() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("isUdupServer() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_shuffleStrings(t *testing.T) {
	type args struct {
		list []string
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shuffleStrings(tt.args.list)
		})
	}
}

func Test_maxUint64(t *testing.T) {
	type args struct {
		a uint64
		b uint64
	}
	tests := []struct {
		name string
		args args
		want uint64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := maxUint64(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("maxUint64() = %v, want %v", got, tt.want)
			}
		})
	}
}
