/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package server

import (
	"net"
	"net/rpc"
	"sync"
	"testing"
	"time"
	uconf "github.com/actiontech/dtle/internal/config"
	ulog "github.com/actiontech/dtle/internal/logger"
	"github.com/actiontech/dtle/internal/server/store"

	"github.com/docker/leadership"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
)

func TestServer_monitorLeadership(t *testing.T) {
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
			s.monitorLeadership()
		})
	}
}

func TestServer_leaderLoop(t *testing.T) {
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
		stopCh chan struct{}
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
			s.leaderLoop(tt.args.stopCh)
		})
	}
}

func TestServer_establishLeadership(t *testing.T) {
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
		stopCh chan struct{}
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
			if err := s.establishLeadership(tt.args.stopCh); (err != nil) != tt.wantErr {
				t.Errorf("Server.establishLeadership() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_restoreEvals(t *testing.T) {
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
			if err := s.restoreEvals(); (err != nil) != tt.wantErr {
				t.Errorf("Server.restoreEvals() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_reapFailedEvaluations(t *testing.T) {
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
		stopCh chan struct{}
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
			s.reapFailedEvaluations(tt.args.stopCh)
		})
	}
}

func TestServer_reapDupBlockedEvaluations(t *testing.T) {
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
		stopCh chan struct{}
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
			s.reapDupBlockedEvaluations(tt.args.stopCh)
		})
	}
}

func TestServer_periodicUnblockFailedEvals(t *testing.T) {
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
		stopCh chan struct{}
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
			s.periodicUnblockFailedEvals(tt.args.stopCh)
		})
	}
}

func TestServer_revokeLeadership(t *testing.T) {
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
			if err := s.revokeLeadership(); (err != nil) != tt.wantErr {
				t.Errorf("Server.revokeLeadership() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_reconcile(t *testing.T) {
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
			if err := s.reconcile(); (err != nil) != tt.wantErr {
				t.Errorf("Server.reconcile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_reconcileMember(t *testing.T) {
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
		member serf.Member
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
			if err := s.reconcileMember(tt.args.member); (err != nil) != tt.wantErr {
				t.Errorf("Server.reconcileMember() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_addRaftPeer(t *testing.T) {
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
		m     serf.Member
		parts *serverParts
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
			if err := s.addRaftPeer(tt.args.m, tt.args.parts); (err != nil) != tt.wantErr {
				t.Errorf("Server.addRaftPeer() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_removeRaftPeer(t *testing.T) {
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
		m     serf.Member
		parts *serverParts
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
			if err := s.removeRaftPeer(tt.args.m, tt.args.parts); (err != nil) != tt.wantErr {
				t.Errorf("Server.removeRaftPeer() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
