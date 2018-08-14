package server

import (
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestNewRaftLayer(t *testing.T) {
	type args struct {
		addr net.Addr
	}
	tests := []struct {
		name string
		args args
		want *RaftLayer
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewRaftLayer(tt.args.addr); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewRaftLayer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRaftLayer_Handoff(t *testing.T) {
	type fields struct {
		addr      net.Addr
		connCh    chan net.Conn
		closed    bool
		closeCh   chan struct{}
		closeLock sync.Mutex
	}
	type args struct {
		c net.Conn
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
			l := &RaftLayer{
				addr:      tt.fields.addr,
				connCh:    tt.fields.connCh,
				closed:    tt.fields.closed,
				closeCh:   tt.fields.closeCh,
				closeLock: tt.fields.closeLock,
			}
			if err := l.Handoff(tt.args.c); (err != nil) != tt.wantErr {
				t.Errorf("RaftLayer.Handoff() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRaftLayer_Accept(t *testing.T) {
	type fields struct {
		addr      net.Addr
		connCh    chan net.Conn
		closed    bool
		closeCh   chan struct{}
		closeLock sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		want    net.Conn
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &RaftLayer{
				addr:      tt.fields.addr,
				connCh:    tt.fields.connCh,
				closed:    tt.fields.closed,
				closeCh:   tt.fields.closeCh,
				closeLock: tt.fields.closeLock,
			}
			got, err := l.Accept()
			if (err != nil) != tt.wantErr {
				t.Errorf("RaftLayer.Accept() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RaftLayer.Accept() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRaftLayer_Close(t *testing.T) {
	type fields struct {
		addr      net.Addr
		connCh    chan net.Conn
		closed    bool
		closeCh   chan struct{}
		closeLock sync.Mutex
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
			l := &RaftLayer{
				addr:      tt.fields.addr,
				connCh:    tt.fields.connCh,
				closed:    tt.fields.closed,
				closeCh:   tt.fields.closeCh,
				closeLock: tt.fields.closeLock,
			}
			if err := l.Close(); (err != nil) != tt.wantErr {
				t.Errorf("RaftLayer.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRaftLayer_Addr(t *testing.T) {
	type fields struct {
		addr      net.Addr
		connCh    chan net.Conn
		closed    bool
		closeCh   chan struct{}
		closeLock sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   net.Addr
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &RaftLayer{
				addr:      tt.fields.addr,
				connCh:    tt.fields.connCh,
				closed:    tt.fields.closed,
				closeCh:   tt.fields.closeCh,
				closeLock: tt.fields.closeLock,
			}
			if got := l.Addr(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RaftLayer.Addr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRaftLayer_Dial(t *testing.T) {
	type fields struct {
		addr      net.Addr
		connCh    chan net.Conn
		closed    bool
		closeCh   chan struct{}
		closeLock sync.Mutex
	}
	type args struct {
		address raft.ServerAddress
		timeout time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    net.Conn
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &RaftLayer{
				addr:      tt.fields.addr,
				connCh:    tt.fields.connCh,
				closed:    tt.fields.closed,
				closeCh:   tt.fields.closeCh,
				closeLock: tt.fields.closeLock,
			}
			got, err := l.Dial(tt.args.address, tt.args.timeout)
			if (err != nil) != tt.wantErr {
				t.Errorf("RaftLayer.Dial() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RaftLayer.Dial() = %v, want %v", got, tt.want)
			}
		})
	}
}
