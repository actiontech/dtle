/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package server

import (
	"io"
	"net"
	"net/rpc"
	"reflect"
	"sync"
	"testing"
	"time"
	uconf "github.com/actiontech/udup/internal/config"
	ulog "github.com/actiontech/udup/internal/logger"
	"github.com/actiontech/udup/internal/models"
	"github.com/actiontech/udup/internal/server/store"

	"github.com/docker/leadership"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
)

func TestNewClientCodec(t *testing.T) {
	type args struct {
		conn io.ReadWriteCloser
	}
	tests := []struct {
		name string
		args args
		want rpc.ClientCodec
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewClientCodec(tt.args.conn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewClientCodec() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewServerCodec(t *testing.T) {
	type args struct {
		conn io.ReadWriteCloser
	}
	tests := []struct {
		name string
		args args
		want rpc.ServerCodec
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewServerCodec(tt.args.conn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewServerCodec() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_listen(t *testing.T) {
	type fields struct {
		config              *uconf.ServerConfig
		logger              *ulog.Logger
		connPool            *ConnPool
		endpoints           endpoints
		leaderCh            <-chan bool
		raft                *raft.Raft
		raftLayer           *RaftLayer
		raftStore           *raftboltdb.BoltStore
		raftInmem           *raft.InmemStore
		raftTransport       *raft.NetworkTransport
		fsm                 *udupFSM
		store               *store.Store
		candidate           *leadership.Candidate
		rpcListener         net.Listener
		rpcServer           *rpc.Server
		rpcAdvertise        net.Addr
		peers               map[string][]*serverParts
		localPeers          map[raft.ServerAddress]*serverParts
		peerLock            sync.RWMutex
		serf                *serf.Serf
		reconcileCh         chan serf.Member
		eventCh             chan serf.Event
		evalBroker          *EvalBroker
		blockedEvals        *BlockedEvals
		planQueue           *PlanQueue
		heartbeatTimers     map[string]*time.Timer
		heartbeatTimersLock sync.Mutex
		workers             []*Worker
		left                bool
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				config:              tt.fields.config,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				endpoints:           tt.fields.endpoints,
				leaderCh:            tt.fields.leaderCh,
				raft:                tt.fields.raft,
				raftLayer:           tt.fields.raftLayer,
				raftStore:           tt.fields.raftStore,
				raftInmem:           tt.fields.raftInmem,
				raftTransport:       tt.fields.raftTransport,
				fsm:                 tt.fields.fsm,
				store:               tt.fields.store,
				candidate:           tt.fields.candidate,
				rpcListener:         tt.fields.rpcListener,
				rpcServer:           tt.fields.rpcServer,
				rpcAdvertise:        tt.fields.rpcAdvertise,
				peers:               tt.fields.peers,
				localPeers:          tt.fields.localPeers,
				peerLock:            tt.fields.peerLock,
				serf:                tt.fields.serf,
				reconcileCh:         tt.fields.reconcileCh,
				eventCh:             tt.fields.eventCh,
				evalBroker:          tt.fields.evalBroker,
				blockedEvals:        tt.fields.blockedEvals,
				planQueue:           tt.fields.planQueue,
				heartbeatTimers:     tt.fields.heartbeatTimers,
				heartbeatTimersLock: tt.fields.heartbeatTimersLock,
				workers:             tt.fields.workers,
				left:                tt.fields.left,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			s.listen()
		})
	}
}

func TestServer_handleConn(t *testing.T) {
	type fields struct {
		config              *uconf.ServerConfig
		logger              *ulog.Logger
		connPool            *ConnPool
		endpoints           endpoints
		leaderCh            <-chan bool
		raft                *raft.Raft
		raftLayer           *RaftLayer
		raftStore           *raftboltdb.BoltStore
		raftInmem           *raft.InmemStore
		raftTransport       *raft.NetworkTransport
		fsm                 *udupFSM
		store               *store.Store
		candidate           *leadership.Candidate
		rpcListener         net.Listener
		rpcServer           *rpc.Server
		rpcAdvertise        net.Addr
		peers               map[string][]*serverParts
		localPeers          map[raft.ServerAddress]*serverParts
		peerLock            sync.RWMutex
		serf                *serf.Serf
		reconcileCh         chan serf.Member
		eventCh             chan serf.Event
		evalBroker          *EvalBroker
		blockedEvals        *BlockedEvals
		planQueue           *PlanQueue
		heartbeatTimers     map[string]*time.Timer
		heartbeatTimersLock sync.Mutex
		workers             []*Worker
		left                bool
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		conn net.Conn
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
			s := &Server{
				config:              tt.fields.config,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				endpoints:           tt.fields.endpoints,
				leaderCh:            tt.fields.leaderCh,
				raft:                tt.fields.raft,
				raftLayer:           tt.fields.raftLayer,
				raftStore:           tt.fields.raftStore,
				raftInmem:           tt.fields.raftInmem,
				raftTransport:       tt.fields.raftTransport,
				fsm:                 tt.fields.fsm,
				store:               tt.fields.store,
				candidate:           tt.fields.candidate,
				rpcListener:         tt.fields.rpcListener,
				rpcServer:           tt.fields.rpcServer,
				rpcAdvertise:        tt.fields.rpcAdvertise,
				peers:               tt.fields.peers,
				localPeers:          tt.fields.localPeers,
				peerLock:            tt.fields.peerLock,
				serf:                tt.fields.serf,
				reconcileCh:         tt.fields.reconcileCh,
				eventCh:             tt.fields.eventCh,
				evalBroker:          tt.fields.evalBroker,
				blockedEvals:        tt.fields.blockedEvals,
				planQueue:           tt.fields.planQueue,
				heartbeatTimers:     tt.fields.heartbeatTimers,
				heartbeatTimersLock: tt.fields.heartbeatTimersLock,
				workers:             tt.fields.workers,
				left:                tt.fields.left,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			s.handleConn(tt.args.conn)
		})
	}
}

func TestServer_handleMultiplex(t *testing.T) {
	type fields struct {
		config              *uconf.ServerConfig
		logger              *ulog.Logger
		connPool            *ConnPool
		endpoints           endpoints
		leaderCh            <-chan bool
		raft                *raft.Raft
		raftLayer           *RaftLayer
		raftStore           *raftboltdb.BoltStore
		raftInmem           *raft.InmemStore
		raftTransport       *raft.NetworkTransport
		fsm                 *udupFSM
		store               *store.Store
		candidate           *leadership.Candidate
		rpcListener         net.Listener
		rpcServer           *rpc.Server
		rpcAdvertise        net.Addr
		peers               map[string][]*serverParts
		localPeers          map[raft.ServerAddress]*serverParts
		peerLock            sync.RWMutex
		serf                *serf.Serf
		reconcileCh         chan serf.Member
		eventCh             chan serf.Event
		evalBroker          *EvalBroker
		blockedEvals        *BlockedEvals
		planQueue           *PlanQueue
		heartbeatTimers     map[string]*time.Timer
		heartbeatTimersLock sync.Mutex
		workers             []*Worker
		left                bool
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		conn net.Conn
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
			s := &Server{
				config:              tt.fields.config,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				endpoints:           tt.fields.endpoints,
				leaderCh:            tt.fields.leaderCh,
				raft:                tt.fields.raft,
				raftLayer:           tt.fields.raftLayer,
				raftStore:           tt.fields.raftStore,
				raftInmem:           tt.fields.raftInmem,
				raftTransport:       tt.fields.raftTransport,
				fsm:                 tt.fields.fsm,
				store:               tt.fields.store,
				candidate:           tt.fields.candidate,
				rpcListener:         tt.fields.rpcListener,
				rpcServer:           tt.fields.rpcServer,
				rpcAdvertise:        tt.fields.rpcAdvertise,
				peers:               tt.fields.peers,
				localPeers:          tt.fields.localPeers,
				peerLock:            tt.fields.peerLock,
				serf:                tt.fields.serf,
				reconcileCh:         tt.fields.reconcileCh,
				eventCh:             tt.fields.eventCh,
				evalBroker:          tt.fields.evalBroker,
				blockedEvals:        tt.fields.blockedEvals,
				planQueue:           tt.fields.planQueue,
				heartbeatTimers:     tt.fields.heartbeatTimers,
				heartbeatTimersLock: tt.fields.heartbeatTimersLock,
				workers:             tt.fields.workers,
				left:                tt.fields.left,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			s.handleMultiplex(tt.args.conn)
		})
	}
}

func TestServer_handleUdupConn(t *testing.T) {
	type fields struct {
		config              *uconf.ServerConfig
		logger              *ulog.Logger
		connPool            *ConnPool
		endpoints           endpoints
		leaderCh            <-chan bool
		raft                *raft.Raft
		raftLayer           *RaftLayer
		raftStore           *raftboltdb.BoltStore
		raftInmem           *raft.InmemStore
		raftTransport       *raft.NetworkTransport
		fsm                 *udupFSM
		store               *store.Store
		candidate           *leadership.Candidate
		rpcListener         net.Listener
		rpcServer           *rpc.Server
		rpcAdvertise        net.Addr
		peers               map[string][]*serverParts
		localPeers          map[raft.ServerAddress]*serverParts
		peerLock            sync.RWMutex
		serf                *serf.Serf
		reconcileCh         chan serf.Member
		eventCh             chan serf.Event
		evalBroker          *EvalBroker
		blockedEvals        *BlockedEvals
		planQueue           *PlanQueue
		heartbeatTimers     map[string]*time.Timer
		heartbeatTimersLock sync.Mutex
		workers             []*Worker
		left                bool
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		conn net.Conn
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
			s := &Server{
				config:              tt.fields.config,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				endpoints:           tt.fields.endpoints,
				leaderCh:            tt.fields.leaderCh,
				raft:                tt.fields.raft,
				raftLayer:           tt.fields.raftLayer,
				raftStore:           tt.fields.raftStore,
				raftInmem:           tt.fields.raftInmem,
				raftTransport:       tt.fields.raftTransport,
				fsm:                 tt.fields.fsm,
				store:               tt.fields.store,
				candidate:           tt.fields.candidate,
				rpcListener:         tt.fields.rpcListener,
				rpcServer:           tt.fields.rpcServer,
				rpcAdvertise:        tt.fields.rpcAdvertise,
				peers:               tt.fields.peers,
				localPeers:          tt.fields.localPeers,
				peerLock:            tt.fields.peerLock,
				serf:                tt.fields.serf,
				reconcileCh:         tt.fields.reconcileCh,
				eventCh:             tt.fields.eventCh,
				evalBroker:          tt.fields.evalBroker,
				blockedEvals:        tt.fields.blockedEvals,
				planQueue:           tt.fields.planQueue,
				heartbeatTimers:     tt.fields.heartbeatTimers,
				heartbeatTimersLock: tt.fields.heartbeatTimersLock,
				workers:             tt.fields.workers,
				left:                tt.fields.left,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			s.handleUdupConn(tt.args.conn)
		})
	}
}

func TestServer_forward(t *testing.T) {
	type fields struct {
		config              *uconf.ServerConfig
		logger              *ulog.Logger
		connPool            *ConnPool
		endpoints           endpoints
		leaderCh            <-chan bool
		raft                *raft.Raft
		raftLayer           *RaftLayer
		raftStore           *raftboltdb.BoltStore
		raftInmem           *raft.InmemStore
		raftTransport       *raft.NetworkTransport
		fsm                 *udupFSM
		store               *store.Store
		candidate           *leadership.Candidate
		rpcListener         net.Listener
		rpcServer           *rpc.Server
		rpcAdvertise        net.Addr
		peers               map[string][]*serverParts
		localPeers          map[raft.ServerAddress]*serverParts
		peerLock            sync.RWMutex
		serf                *serf.Serf
		reconcileCh         chan serf.Member
		eventCh             chan serf.Event
		evalBroker          *EvalBroker
		blockedEvals        *BlockedEvals
		planQueue           *PlanQueue
		heartbeatTimers     map[string]*time.Timer
		heartbeatTimersLock sync.Mutex
		workers             []*Worker
		left                bool
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		method string
		info   models.RPCInfo
		args   interface{}
		reply  interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				config:              tt.fields.config,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				endpoints:           tt.fields.endpoints,
				leaderCh:            tt.fields.leaderCh,
				raft:                tt.fields.raft,
				raftLayer:           tt.fields.raftLayer,
				raftStore:           tt.fields.raftStore,
				raftInmem:           tt.fields.raftInmem,
				raftTransport:       tt.fields.raftTransport,
				fsm:                 tt.fields.fsm,
				store:               tt.fields.store,
				candidate:           tt.fields.candidate,
				rpcListener:         tt.fields.rpcListener,
				rpcServer:           tt.fields.rpcServer,
				rpcAdvertise:        tt.fields.rpcAdvertise,
				peers:               tt.fields.peers,
				localPeers:          tt.fields.localPeers,
				peerLock:            tt.fields.peerLock,
				serf:                tt.fields.serf,
				reconcileCh:         tt.fields.reconcileCh,
				eventCh:             tt.fields.eventCh,
				evalBroker:          tt.fields.evalBroker,
				blockedEvals:        tt.fields.blockedEvals,
				planQueue:           tt.fields.planQueue,
				heartbeatTimers:     tt.fields.heartbeatTimers,
				heartbeatTimersLock: tt.fields.heartbeatTimersLock,
				workers:             tt.fields.workers,
				left:                tt.fields.left,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			got, err := s.forward(tt.args.method, tt.args.info, tt.args.args, tt.args.reply)
			if (err != nil) != tt.wantErr {
				t.Errorf("Server.forward() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Server.forward() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_getLeader(t *testing.T) {
	type fields struct {
		config              *uconf.ServerConfig
		logger              *ulog.Logger
		connPool            *ConnPool
		endpoints           endpoints
		leaderCh            <-chan bool
		raft                *raft.Raft
		raftLayer           *RaftLayer
		raftStore           *raftboltdb.BoltStore
		raftInmem           *raft.InmemStore
		raftTransport       *raft.NetworkTransport
		fsm                 *udupFSM
		store               *store.Store
		candidate           *leadership.Candidate
		rpcListener         net.Listener
		rpcServer           *rpc.Server
		rpcAdvertise        net.Addr
		peers               map[string][]*serverParts
		localPeers          map[raft.ServerAddress]*serverParts
		peerLock            sync.RWMutex
		serf                *serf.Serf
		reconcileCh         chan serf.Member
		eventCh             chan serf.Event
		evalBroker          *EvalBroker
		blockedEvals        *BlockedEvals
		planQueue           *PlanQueue
		heartbeatTimers     map[string]*time.Timer
		heartbeatTimersLock sync.Mutex
		workers             []*Worker
		left                bool
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
		want1  *serverParts
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				config:              tt.fields.config,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				endpoints:           tt.fields.endpoints,
				leaderCh:            tt.fields.leaderCh,
				raft:                tt.fields.raft,
				raftLayer:           tt.fields.raftLayer,
				raftStore:           tt.fields.raftStore,
				raftInmem:           tt.fields.raftInmem,
				raftTransport:       tt.fields.raftTransport,
				fsm:                 tt.fields.fsm,
				store:               tt.fields.store,
				candidate:           tt.fields.candidate,
				rpcListener:         tt.fields.rpcListener,
				rpcServer:           tt.fields.rpcServer,
				rpcAdvertise:        tt.fields.rpcAdvertise,
				peers:               tt.fields.peers,
				localPeers:          tt.fields.localPeers,
				peerLock:            tt.fields.peerLock,
				serf:                tt.fields.serf,
				reconcileCh:         tt.fields.reconcileCh,
				eventCh:             tt.fields.eventCh,
				evalBroker:          tt.fields.evalBroker,
				blockedEvals:        tt.fields.blockedEvals,
				planQueue:           tt.fields.planQueue,
				heartbeatTimers:     tt.fields.heartbeatTimers,
				heartbeatTimersLock: tt.fields.heartbeatTimersLock,
				workers:             tt.fields.workers,
				left:                tt.fields.left,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			got, got1 := s.getLeader()
			if got != tt.want {
				t.Errorf("Server.getLeader() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("Server.getLeader() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestServer_forwardLeader(t *testing.T) {
	type fields struct {
		config              *uconf.ServerConfig
		logger              *ulog.Logger
		connPool            *ConnPool
		endpoints           endpoints
		leaderCh            <-chan bool
		raft                *raft.Raft
		raftLayer           *RaftLayer
		raftStore           *raftboltdb.BoltStore
		raftInmem           *raft.InmemStore
		raftTransport       *raft.NetworkTransport
		fsm                 *udupFSM
		store               *store.Store
		candidate           *leadership.Candidate
		rpcListener         net.Listener
		rpcServer           *rpc.Server
		rpcAdvertise        net.Addr
		peers               map[string][]*serverParts
		localPeers          map[raft.ServerAddress]*serverParts
		peerLock            sync.RWMutex
		serf                *serf.Serf
		reconcileCh         chan serf.Member
		eventCh             chan serf.Event
		evalBroker          *EvalBroker
		blockedEvals        *BlockedEvals
		planQueue           *PlanQueue
		heartbeatTimers     map[string]*time.Timer
		heartbeatTimersLock sync.Mutex
		workers             []*Worker
		left                bool
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		server *serverParts
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
			s := &Server{
				config:              tt.fields.config,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				endpoints:           tt.fields.endpoints,
				leaderCh:            tt.fields.leaderCh,
				raft:                tt.fields.raft,
				raftLayer:           tt.fields.raftLayer,
				raftStore:           tt.fields.raftStore,
				raftInmem:           tt.fields.raftInmem,
				raftTransport:       tt.fields.raftTransport,
				fsm:                 tt.fields.fsm,
				store:               tt.fields.store,
				candidate:           tt.fields.candidate,
				rpcListener:         tt.fields.rpcListener,
				rpcServer:           tt.fields.rpcServer,
				rpcAdvertise:        tt.fields.rpcAdvertise,
				peers:               tt.fields.peers,
				localPeers:          tt.fields.localPeers,
				peerLock:            tt.fields.peerLock,
				serf:                tt.fields.serf,
				reconcileCh:         tt.fields.reconcileCh,
				eventCh:             tt.fields.eventCh,
				evalBroker:          tt.fields.evalBroker,
				blockedEvals:        tt.fields.blockedEvals,
				planQueue:           tt.fields.planQueue,
				heartbeatTimers:     tt.fields.heartbeatTimers,
				heartbeatTimersLock: tt.fields.heartbeatTimersLock,
				workers:             tt.fields.workers,
				left:                tt.fields.left,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if err := s.forwardLeader(tt.args.server, tt.args.method, tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Server.forwardLeader() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_forwardRegion(t *testing.T) {
	type fields struct {
		config              *uconf.ServerConfig
		logger              *ulog.Logger
		connPool            *ConnPool
		endpoints           endpoints
		leaderCh            <-chan bool
		raft                *raft.Raft
		raftLayer           *RaftLayer
		raftStore           *raftboltdb.BoltStore
		raftInmem           *raft.InmemStore
		raftTransport       *raft.NetworkTransport
		fsm                 *udupFSM
		store               *store.Store
		candidate           *leadership.Candidate
		rpcListener         net.Listener
		rpcServer           *rpc.Server
		rpcAdvertise        net.Addr
		peers               map[string][]*serverParts
		localPeers          map[raft.ServerAddress]*serverParts
		peerLock            sync.RWMutex
		serf                *serf.Serf
		reconcileCh         chan serf.Member
		eventCh             chan serf.Event
		evalBroker          *EvalBroker
		blockedEvals        *BlockedEvals
		planQueue           *PlanQueue
		heartbeatTimers     map[string]*time.Timer
		heartbeatTimersLock sync.Mutex
		workers             []*Worker
		left                bool
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		region string
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
			s := &Server{
				config:              tt.fields.config,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				endpoints:           tt.fields.endpoints,
				leaderCh:            tt.fields.leaderCh,
				raft:                tt.fields.raft,
				raftLayer:           tt.fields.raftLayer,
				raftStore:           tt.fields.raftStore,
				raftInmem:           tt.fields.raftInmem,
				raftTransport:       tt.fields.raftTransport,
				fsm:                 tt.fields.fsm,
				store:               tt.fields.store,
				candidate:           tt.fields.candidate,
				rpcListener:         tt.fields.rpcListener,
				rpcServer:           tt.fields.rpcServer,
				rpcAdvertise:        tt.fields.rpcAdvertise,
				peers:               tt.fields.peers,
				localPeers:          tt.fields.localPeers,
				peerLock:            tt.fields.peerLock,
				serf:                tt.fields.serf,
				reconcileCh:         tt.fields.reconcileCh,
				eventCh:             tt.fields.eventCh,
				evalBroker:          tt.fields.evalBroker,
				blockedEvals:        tt.fields.blockedEvals,
				planQueue:           tt.fields.planQueue,
				heartbeatTimers:     tt.fields.heartbeatTimers,
				heartbeatTimersLock: tt.fields.heartbeatTimersLock,
				workers:             tt.fields.workers,
				left:                tt.fields.left,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if err := s.forwardRegion(tt.args.region, tt.args.method, tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Server.forwardRegion() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_raftApplyFuture(t *testing.T) {
	type fields struct {
		config              *uconf.ServerConfig
		logger              *ulog.Logger
		connPool            *ConnPool
		endpoints           endpoints
		leaderCh            <-chan bool
		raft                *raft.Raft
		raftLayer           *RaftLayer
		raftStore           *raftboltdb.BoltStore
		raftInmem           *raft.InmemStore
		raftTransport       *raft.NetworkTransport
		fsm                 *udupFSM
		store               *store.Store
		candidate           *leadership.Candidate
		rpcListener         net.Listener
		rpcServer           *rpc.Server
		rpcAdvertise        net.Addr
		peers               map[string][]*serverParts
		localPeers          map[raft.ServerAddress]*serverParts
		peerLock            sync.RWMutex
		serf                *serf.Serf
		reconcileCh         chan serf.Member
		eventCh             chan serf.Event
		evalBroker          *EvalBroker
		blockedEvals        *BlockedEvals
		planQueue           *PlanQueue
		heartbeatTimers     map[string]*time.Timer
		heartbeatTimersLock sync.Mutex
		workers             []*Worker
		left                bool
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		t   models.MessageType
		msg interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    raft.ApplyFuture
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				config:              tt.fields.config,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				endpoints:           tt.fields.endpoints,
				leaderCh:            tt.fields.leaderCh,
				raft:                tt.fields.raft,
				raftLayer:           tt.fields.raftLayer,
				raftStore:           tt.fields.raftStore,
				raftInmem:           tt.fields.raftInmem,
				raftTransport:       tt.fields.raftTransport,
				fsm:                 tt.fields.fsm,
				store:               tt.fields.store,
				candidate:           tt.fields.candidate,
				rpcListener:         tt.fields.rpcListener,
				rpcServer:           tt.fields.rpcServer,
				rpcAdvertise:        tt.fields.rpcAdvertise,
				peers:               tt.fields.peers,
				localPeers:          tt.fields.localPeers,
				peerLock:            tt.fields.peerLock,
				serf:                tt.fields.serf,
				reconcileCh:         tt.fields.reconcileCh,
				eventCh:             tt.fields.eventCh,
				evalBroker:          tt.fields.evalBroker,
				blockedEvals:        tt.fields.blockedEvals,
				planQueue:           tt.fields.planQueue,
				heartbeatTimers:     tt.fields.heartbeatTimers,
				heartbeatTimersLock: tt.fields.heartbeatTimersLock,
				workers:             tt.fields.workers,
				left:                tt.fields.left,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			got, err := s.raftApplyFuture(tt.args.t, tt.args.msg)
			if (err != nil) != tt.wantErr {
				t.Errorf("Server.raftApplyFuture() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Server.raftApplyFuture() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_raftApply(t *testing.T) {
	type fields struct {
		config              *uconf.ServerConfig
		logger              *ulog.Logger
		connPool            *ConnPool
		endpoints           endpoints
		leaderCh            <-chan bool
		raft                *raft.Raft
		raftLayer           *RaftLayer
		raftStore           *raftboltdb.BoltStore
		raftInmem           *raft.InmemStore
		raftTransport       *raft.NetworkTransport
		fsm                 *udupFSM
		store               *store.Store
		candidate           *leadership.Candidate
		rpcListener         net.Listener
		rpcServer           *rpc.Server
		rpcAdvertise        net.Addr
		peers               map[string][]*serverParts
		localPeers          map[raft.ServerAddress]*serverParts
		peerLock            sync.RWMutex
		serf                *serf.Serf
		reconcileCh         chan serf.Member
		eventCh             chan serf.Event
		evalBroker          *EvalBroker
		blockedEvals        *BlockedEvals
		planQueue           *PlanQueue
		heartbeatTimers     map[string]*time.Timer
		heartbeatTimersLock sync.Mutex
		workers             []*Worker
		left                bool
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		t   models.MessageType
		msg interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		want1   uint64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				config:              tt.fields.config,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				endpoints:           tt.fields.endpoints,
				leaderCh:            tt.fields.leaderCh,
				raft:                tt.fields.raft,
				raftLayer:           tt.fields.raftLayer,
				raftStore:           tt.fields.raftStore,
				raftInmem:           tt.fields.raftInmem,
				raftTransport:       tt.fields.raftTransport,
				fsm:                 tt.fields.fsm,
				store:               tt.fields.store,
				candidate:           tt.fields.candidate,
				rpcListener:         tt.fields.rpcListener,
				rpcServer:           tt.fields.rpcServer,
				rpcAdvertise:        tt.fields.rpcAdvertise,
				peers:               tt.fields.peers,
				localPeers:          tt.fields.localPeers,
				peerLock:            tt.fields.peerLock,
				serf:                tt.fields.serf,
				reconcileCh:         tt.fields.reconcileCh,
				eventCh:             tt.fields.eventCh,
				evalBroker:          tt.fields.evalBroker,
				blockedEvals:        tt.fields.blockedEvals,
				planQueue:           tt.fields.planQueue,
				heartbeatTimers:     tt.fields.heartbeatTimers,
				heartbeatTimersLock: tt.fields.heartbeatTimersLock,
				workers:             tt.fields.workers,
				left:                tt.fields.left,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			got, got1, err := s.raftApply(tt.args.t, tt.args.msg)
			if (err != nil) != tt.wantErr {
				t.Errorf("Server.raftApply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Server.raftApply() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("Server.raftApply() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestServer_setQueryMeta(t *testing.T) {
	type fields struct {
		config              *uconf.ServerConfig
		logger              *ulog.Logger
		connPool            *ConnPool
		endpoints           endpoints
		leaderCh            <-chan bool
		raft                *raft.Raft
		raftLayer           *RaftLayer
		raftStore           *raftboltdb.BoltStore
		raftInmem           *raft.InmemStore
		raftTransport       *raft.NetworkTransport
		fsm                 *udupFSM
		store               *store.Store
		candidate           *leadership.Candidate
		rpcListener         net.Listener
		rpcServer           *rpc.Server
		rpcAdvertise        net.Addr
		peers               map[string][]*serverParts
		localPeers          map[raft.ServerAddress]*serverParts
		peerLock            sync.RWMutex
		serf                *serf.Serf
		reconcileCh         chan serf.Member
		eventCh             chan serf.Event
		evalBroker          *EvalBroker
		blockedEvals        *BlockedEvals
		planQueue           *PlanQueue
		heartbeatTimers     map[string]*time.Timer
		heartbeatTimersLock sync.Mutex
		workers             []*Worker
		left                bool
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		m *models.QueryMeta
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
			s := &Server{
				config:              tt.fields.config,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				endpoints:           tt.fields.endpoints,
				leaderCh:            tt.fields.leaderCh,
				raft:                tt.fields.raft,
				raftLayer:           tt.fields.raftLayer,
				raftStore:           tt.fields.raftStore,
				raftInmem:           tt.fields.raftInmem,
				raftTransport:       tt.fields.raftTransport,
				fsm:                 tt.fields.fsm,
				store:               tt.fields.store,
				candidate:           tt.fields.candidate,
				rpcListener:         tt.fields.rpcListener,
				rpcServer:           tt.fields.rpcServer,
				rpcAdvertise:        tt.fields.rpcAdvertise,
				peers:               tt.fields.peers,
				localPeers:          tt.fields.localPeers,
				peerLock:            tt.fields.peerLock,
				serf:                tt.fields.serf,
				reconcileCh:         tt.fields.reconcileCh,
				eventCh:             tt.fields.eventCh,
				evalBroker:          tt.fields.evalBroker,
				blockedEvals:        tt.fields.blockedEvals,
				planQueue:           tt.fields.planQueue,
				heartbeatTimers:     tt.fields.heartbeatTimers,
				heartbeatTimersLock: tt.fields.heartbeatTimersLock,
				workers:             tt.fields.workers,
				left:                tt.fields.left,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			s.setQueryMeta(tt.args.m)
		})
	}
}

func TestServer_blockingRPC(t *testing.T) {
	type fields struct {
		config              *uconf.ServerConfig
		logger              *ulog.Logger
		connPool            *ConnPool
		endpoints           endpoints
		leaderCh            <-chan bool
		raft                *raft.Raft
		raftLayer           *RaftLayer
		raftStore           *raftboltdb.BoltStore
		raftInmem           *raft.InmemStore
		raftTransport       *raft.NetworkTransport
		fsm                 *udupFSM
		store               *store.Store
		candidate           *leadership.Candidate
		rpcListener         net.Listener
		rpcServer           *rpc.Server
		rpcAdvertise        net.Addr
		peers               map[string][]*serverParts
		localPeers          map[raft.ServerAddress]*serverParts
		peerLock            sync.RWMutex
		serf                *serf.Serf
		reconcileCh         chan serf.Member
		eventCh             chan serf.Event
		evalBroker          *EvalBroker
		blockedEvals        *BlockedEvals
		planQueue           *PlanQueue
		heartbeatTimers     map[string]*time.Timer
		heartbeatTimersLock sync.Mutex
		workers             []*Worker
		left                bool
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		opts *blockingOptions
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
			s := &Server{
				config:              tt.fields.config,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				endpoints:           tt.fields.endpoints,
				leaderCh:            tt.fields.leaderCh,
				raft:                tt.fields.raft,
				raftLayer:           tt.fields.raftLayer,
				raftStore:           tt.fields.raftStore,
				raftInmem:           tt.fields.raftInmem,
				raftTransport:       tt.fields.raftTransport,
				fsm:                 tt.fields.fsm,
				store:               tt.fields.store,
				candidate:           tt.fields.candidate,
				rpcListener:         tt.fields.rpcListener,
				rpcServer:           tt.fields.rpcServer,
				rpcAdvertise:        tt.fields.rpcAdvertise,
				peers:               tt.fields.peers,
				localPeers:          tt.fields.localPeers,
				peerLock:            tt.fields.peerLock,
				serf:                tt.fields.serf,
				reconcileCh:         tt.fields.reconcileCh,
				eventCh:             tt.fields.eventCh,
				evalBroker:          tt.fields.evalBroker,
				blockedEvals:        tt.fields.blockedEvals,
				planQueue:           tt.fields.planQueue,
				heartbeatTimers:     tt.fields.heartbeatTimers,
				heartbeatTimersLock: tt.fields.heartbeatTimersLock,
				workers:             tt.fields.workers,
				left:                tt.fields.left,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if err := s.blockingRPC(tt.args.opts); (err != nil) != tt.wantErr {
				t.Errorf("Server.blockingRPC() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
