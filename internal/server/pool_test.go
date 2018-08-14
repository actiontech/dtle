package server

import (
	"bytes"
	"container/list"
	"io"
	"net"
	"net/rpc"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
)

func TestStreamClient_Close(t *testing.T) {
	type fields struct {
		stream net.Conn
		codec  rpc.ClientCodec
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &StreamClient{
				stream: tt.fields.stream,
				codec:  tt.fields.codec,
			}
			sc.Close()
		})
	}
}

func TestConn_markForUse(t *testing.T) {
	type fields struct {
		refCount    int32
		shouldClose int32
		addr        net.Addr
		session     *yamux.Session
		lastUsed    time.Time
		pool        *ConnPool
		clients     *list.List
		clientLock  sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Conn{
				refCount:    tt.fields.refCount,
				shouldClose: tt.fields.shouldClose,
				addr:        tt.fields.addr,
				session:     tt.fields.session,
				lastUsed:    tt.fields.lastUsed,
				pool:        tt.fields.pool,
				clients:     tt.fields.clients,
				clientLock:  tt.fields.clientLock,
			}
			c.markForUse()
		})
	}
}

func TestConn_Close(t *testing.T) {
	type fields struct {
		refCount    int32
		shouldClose int32
		addr        net.Addr
		session     *yamux.Session
		lastUsed    time.Time
		pool        *ConnPool
		clients     *list.List
		clientLock  sync.Mutex
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
			c := &Conn{
				refCount:    tt.fields.refCount,
				shouldClose: tt.fields.shouldClose,
				addr:        tt.fields.addr,
				session:     tt.fields.session,
				lastUsed:    tt.fields.lastUsed,
				pool:        tt.fields.pool,
				clients:     tt.fields.clients,
				clientLock:  tt.fields.clientLock,
			}
			if err := c.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Conn.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConn_getClient(t *testing.T) {
	type fields struct {
		refCount    int32
		shouldClose int32
		addr        net.Addr
		session     *yamux.Session
		lastUsed    time.Time
		pool        *ConnPool
		clients     *list.List
		clientLock  sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		want    *StreamClient
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Conn{
				refCount:    tt.fields.refCount,
				shouldClose: tt.fields.shouldClose,
				addr:        tt.fields.addr,
				session:     tt.fields.session,
				lastUsed:    tt.fields.lastUsed,
				pool:        tt.fields.pool,
				clients:     tt.fields.clients,
				clientLock:  tt.fields.clientLock,
			}
			got, err := c.getClient()
			if (err != nil) != tt.wantErr {
				t.Errorf("Conn.getClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Conn.getClient() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConn_returnClient(t *testing.T) {
	type fields struct {
		refCount    int32
		shouldClose int32
		addr        net.Addr
		session     *yamux.Session
		lastUsed    time.Time
		pool        *ConnPool
		clients     *list.List
		clientLock  sync.Mutex
	}
	type args struct {
		client *StreamClient
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
			c := &Conn{
				refCount:    tt.fields.refCount,
				shouldClose: tt.fields.shouldClose,
				addr:        tt.fields.addr,
				session:     tt.fields.session,
				lastUsed:    tt.fields.lastUsed,
				pool:        tt.fields.pool,
				clients:     tt.fields.clients,
				clientLock:  tt.fields.clientLock,
			}
			c.returnClient(tt.args.client)
		})
	}
}

func TestNewPool(t *testing.T) {
	type args struct {
		maxTime    time.Duration
		maxStreams int
	}
	tests := []struct {
		name          string
		args          args
		want          *ConnPool
		wantLogOutput string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logOutput := &bytes.Buffer{}
			if got := NewPool(logOutput, tt.args.maxTime, tt.args.maxStreams); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewPool() = %v, want %v", got, tt.want)
			}
			if gotLogOutput := logOutput.String(); gotLogOutput != tt.wantLogOutput {
				t.Errorf("NewPool() = %v, want %v", gotLogOutput, tt.wantLogOutput)
			}
		})
	}
}

func TestConnPool_Shutdown(t *testing.T) {
	type fields struct {
		Mutex      sync.Mutex
		logOutput  io.Writer
		maxTime    time.Duration
		maxStreams int
		pool       map[string]*Conn
		limiter    map[string]chan struct{}
		shutdown   bool
		shutdownCh chan struct{}
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
			p := &ConnPool{
				Mutex:      tt.fields.Mutex,
				logOutput:  tt.fields.logOutput,
				maxTime:    tt.fields.maxTime,
				maxStreams: tt.fields.maxStreams,
				pool:       tt.fields.pool,
				limiter:    tt.fields.limiter,
				shutdown:   tt.fields.shutdown,
				shutdownCh: tt.fields.shutdownCh,
			}
			if err := p.Shutdown(); (err != nil) != tt.wantErr {
				t.Errorf("ConnPool.Shutdown() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConnPool_acquire(t *testing.T) {
	type fields struct {
		Mutex      sync.Mutex
		logOutput  io.Writer
		maxTime    time.Duration
		maxStreams int
		pool       map[string]*Conn
		limiter    map[string]chan struct{}
		shutdown   bool
		shutdownCh chan struct{}
	}
	type args struct {
		region string
		addr   net.Addr
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Conn
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ConnPool{
				Mutex:      tt.fields.Mutex,
				logOutput:  tt.fields.logOutput,
				maxTime:    tt.fields.maxTime,
				maxStreams: tt.fields.maxStreams,
				pool:       tt.fields.pool,
				limiter:    tt.fields.limiter,
				shutdown:   tt.fields.shutdown,
				shutdownCh: tt.fields.shutdownCh,
			}
			got, err := p.acquire(tt.args.region, tt.args.addr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConnPool.acquire() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConnPool.acquire() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnPool_getNewConn(t *testing.T) {
	type fields struct {
		Mutex      sync.Mutex
		logOutput  io.Writer
		maxTime    time.Duration
		maxStreams int
		pool       map[string]*Conn
		limiter    map[string]chan struct{}
		shutdown   bool
		shutdownCh chan struct{}
	}
	type args struct {
		region string
		addr   net.Addr
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Conn
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ConnPool{
				Mutex:      tt.fields.Mutex,
				logOutput:  tt.fields.logOutput,
				maxTime:    tt.fields.maxTime,
				maxStreams: tt.fields.maxStreams,
				pool:       tt.fields.pool,
				limiter:    tt.fields.limiter,
				shutdown:   tt.fields.shutdown,
				shutdownCh: tt.fields.shutdownCh,
			}
			got, err := p.getNewConn(tt.args.region, tt.args.addr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConnPool.getNewConn() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConnPool.getNewConn() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnPool_clearConn(t *testing.T) {
	type fields struct {
		Mutex      sync.Mutex
		logOutput  io.Writer
		maxTime    time.Duration
		maxStreams int
		pool       map[string]*Conn
		limiter    map[string]chan struct{}
		shutdown   bool
		shutdownCh chan struct{}
	}
	type args struct {
		conn *Conn
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
			p := &ConnPool{
				Mutex:      tt.fields.Mutex,
				logOutput:  tt.fields.logOutput,
				maxTime:    tt.fields.maxTime,
				maxStreams: tt.fields.maxStreams,
				pool:       tt.fields.pool,
				limiter:    tt.fields.limiter,
				shutdown:   tt.fields.shutdown,
				shutdownCh: tt.fields.shutdownCh,
			}
			p.clearConn(tt.args.conn)
		})
	}
}

func TestConnPool_releaseConn(t *testing.T) {
	type fields struct {
		Mutex      sync.Mutex
		logOutput  io.Writer
		maxTime    time.Duration
		maxStreams int
		pool       map[string]*Conn
		limiter    map[string]chan struct{}
		shutdown   bool
		shutdownCh chan struct{}
	}
	type args struct {
		conn *Conn
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
			p := &ConnPool{
				Mutex:      tt.fields.Mutex,
				logOutput:  tt.fields.logOutput,
				maxTime:    tt.fields.maxTime,
				maxStreams: tt.fields.maxStreams,
				pool:       tt.fields.pool,
				limiter:    tt.fields.limiter,
				shutdown:   tt.fields.shutdown,
				shutdownCh: tt.fields.shutdownCh,
			}
			p.releaseConn(tt.args.conn)
		})
	}
}

func TestConnPool_getClient(t *testing.T) {
	type fields struct {
		Mutex      sync.Mutex
		logOutput  io.Writer
		maxTime    time.Duration
		maxStreams int
		pool       map[string]*Conn
		limiter    map[string]chan struct{}
		shutdown   bool
		shutdownCh chan struct{}
	}
	type args struct {
		region string
		addr   net.Addr
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Conn
		want1   *StreamClient
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ConnPool{
				Mutex:      tt.fields.Mutex,
				logOutput:  tt.fields.logOutput,
				maxTime:    tt.fields.maxTime,
				maxStreams: tt.fields.maxStreams,
				pool:       tt.fields.pool,
				limiter:    tt.fields.limiter,
				shutdown:   tt.fields.shutdown,
				shutdownCh: tt.fields.shutdownCh,
			}
			got, got1, err := p.getClient(tt.args.region, tt.args.addr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConnPool.getClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConnPool.getClient() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ConnPool.getClient() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestConnPool_RPC(t *testing.T) {
	type fields struct {
		Mutex      sync.Mutex
		logOutput  io.Writer
		maxTime    time.Duration
		maxStreams int
		pool       map[string]*Conn
		limiter    map[string]chan struct{}
		shutdown   bool
		shutdownCh chan struct{}
	}
	type args struct {
		region string
		addr   net.Addr
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
			p := &ConnPool{
				Mutex:      tt.fields.Mutex,
				logOutput:  tt.fields.logOutput,
				maxTime:    tt.fields.maxTime,
				maxStreams: tt.fields.maxStreams,
				pool:       tt.fields.pool,
				limiter:    tt.fields.limiter,
				shutdown:   tt.fields.shutdown,
				shutdownCh: tt.fields.shutdownCh,
			}
			if err := p.RPC(tt.args.region, tt.args.addr, tt.args.method, tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("ConnPool.RPC() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConnPool_reap(t *testing.T) {
	type fields struct {
		Mutex      sync.Mutex
		logOutput  io.Writer
		maxTime    time.Duration
		maxStreams int
		pool       map[string]*Conn
		limiter    map[string]chan struct{}
		shutdown   bool
		shutdownCh chan struct{}
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ConnPool{
				Mutex:      tt.fields.Mutex,
				logOutput:  tt.fields.logOutput,
				maxTime:    tt.fields.maxTime,
				maxStreams: tt.fields.maxStreams,
				pool:       tt.fields.pool,
				limiter:    tt.fields.limiter,
				shutdown:   tt.fields.shutdown,
				shutdownCh: tt.fields.shutdownCh,
			}
			p.reap()
		})
	}
}
