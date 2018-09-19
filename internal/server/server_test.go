/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package server

import (
	"net"
	"net/rpc"
	"reflect"
	"sync"
	"testing"
	"time"
	uconf "github.com/actiontech/udup/internal/config"
	ulog "github.com/actiontech/udup/internal/logger"
	"github.com/actiontech/udup/internal/server/store"

	"github.com/docker/leadership"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
)

func TestNewServer(t *testing.T) {
	type args struct {
		config *uconf.ServerConfig
		logger *ulog.Logger
	}
	tests := []struct {
		name    string
		args    args
		want    *Server
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewServer(tt.args.config, tt.args.logger)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewServer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewServer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_Shutdown(t *testing.T) {
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
		name    string
		fields  fields
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
			if err := s.Shutdown(); (err != nil) != tt.wantErr {
				t.Errorf("Server.Shutdown() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_IsShutdown(t *testing.T) {
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
			if got := s.IsShutdown(); got != tt.want {
				t.Errorf("Server.IsShutdown() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_Leave(t *testing.T) {
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
		name    string
		fields  fields
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
			if err := s.Leave(); (err != nil) != tt.wantErr {
				t.Errorf("Server.Leave() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_setupRPC(t *testing.T) {
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
		name    string
		fields  fields
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
			if err := s.setupRPC(); (err != nil) != tt.wantErr {
				t.Errorf("Server.setupRPC() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_setupRaft(t *testing.T) {
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
		name    string
		fields  fields
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
			if err := s.setupRaft(); (err != nil) != tt.wantErr {
				t.Errorf("Server.setupRaft() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_setupSerf(t *testing.T) {
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
		conf *serf.Config
		ch   chan serf.Event
		path string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *serf.Serf
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
			got, err := s.setupSerf(tt.args.conf, tt.args.ch, tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("Server.setupSerf() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Server.setupSerf() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_setupWorkers(t *testing.T) {
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
		name    string
		fields  fields
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
			if err := s.setupWorkers(); (err != nil) != tt.wantErr {
				t.Errorf("Server.setupWorkers() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_numPeers(t *testing.T) {
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
		name    string
		fields  fields
		want    int
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
			got, err := s.numPeers()
			if (err != nil) != tt.wantErr {
				t.Errorf("Server.numPeers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Server.numPeers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_IsLeader(t *testing.T) {
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
			if got := s.IsLeader(); got != tt.want {
				t.Errorf("Server.IsLeader() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_Join(t *testing.T) {
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
		addrs []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
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
			got, err := s.Join(tt.args.addrs)
			if (err != nil) != tt.wantErr {
				t.Errorf("Server.Join() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Server.Join() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_LocalMember(t *testing.T) {
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
		want   serf.Member
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Server{
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
			if got := c.LocalMember(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Server.LocalMember() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_Members(t *testing.T) {
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
		want   []serf.Member
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
			if got := s.Members(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Server.Members() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_RemoveFailedNode(t *testing.T) {
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
		node string
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
			if err := s.RemoveFailedNode(tt.args.node); (err != nil) != tt.wantErr {
				t.Errorf("Server.RemoveFailedNode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_Encrypted(t *testing.T) {
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
			if got := s.Encrypted(); got != tt.want {
				t.Errorf("Server.Encrypted() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_State(t *testing.T) {
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
		want   *store.StateStore
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
			if got := s.State(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Server.State() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_Regions(t *testing.T) {
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
		want   []string
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
			if got := s.Regions(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Server.Regions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_inmemCodec_ReadRequestHeader(t *testing.T) {
	type fields struct {
		method string
		args   interface{}
		reply  interface{}
		err    error
	}
	type args struct {
		req *rpc.Request
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
			i := &inmemCodec{
				method: tt.fields.method,
				args:   tt.fields.args,
				reply:  tt.fields.reply,
				err:    tt.fields.err,
			}
			if err := i.ReadRequestHeader(tt.args.req); (err != nil) != tt.wantErr {
				t.Errorf("inmemCodec.ReadRequestHeader() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_inmemCodec_ReadRequestBody(t *testing.T) {
	type fields struct {
		method string
		args   interface{}
		reply  interface{}
		err    error
	}
	type args struct {
		args interface{}
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
			i := &inmemCodec{
				method: tt.fields.method,
				args:   tt.fields.args,
				reply:  tt.fields.reply,
				err:    tt.fields.err,
			}
			if err := i.ReadRequestBody(tt.args.args); (err != nil) != tt.wantErr {
				t.Errorf("inmemCodec.ReadRequestBody() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_inmemCodec_WriteResponse(t *testing.T) {
	type fields struct {
		method string
		args   interface{}
		reply  interface{}
		err    error
	}
	type args struct {
		resp  *rpc.Response
		reply interface{}
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
			i := &inmemCodec{
				method: tt.fields.method,
				args:   tt.fields.args,
				reply:  tt.fields.reply,
				err:    tt.fields.err,
			}
			if err := i.WriteResponse(tt.args.resp, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("inmemCodec.WriteResponse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_inmemCodec_Close(t *testing.T) {
	type fields struct {
		method string
		args   interface{}
		reply  interface{}
		err    error
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
			i := &inmemCodec{
				method: tt.fields.method,
				args:   tt.fields.args,
				reply:  tt.fields.reply,
				err:    tt.fields.err,
			}
			if err := i.Close(); (err != nil) != tt.wantErr {
				t.Errorf("inmemCodec.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_RPC(t *testing.T) {
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
			if err := s.RPC(tt.args.method, tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Server.RPC() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_Stats(t *testing.T) {
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
		want   map[string]map[string]string
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
			if got := s.Stats(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Server.Stats() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_Region(t *testing.T) {
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
		want   string
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
			if got := s.Region(); got != tt.want {
				t.Errorf("Server.Region() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_Datacenter(t *testing.T) {
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
		want   string
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
			if got := s.Datacenter(); got != tt.want {
				t.Errorf("Server.Datacenter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_GetConfig(t *testing.T) {
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
		want   *uconf.ServerConfig
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
			if got := s.GetConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Server.GetConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}
