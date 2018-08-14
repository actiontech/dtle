package server

import (
	"net"
	"net/rpc"
	"sync"
	"testing"
	"time"
	uconf "udup/internal/config"
	ulog "udup/internal/logger"
	"udup/internal/server/store"

	"github.com/docker/leadership"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
)

func TestServer_initializeHeartbeatTimers(t *testing.T) {
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
			if err := s.initializeHeartbeatTimers(); (err != nil) != tt.wantErr {
				t.Errorf("Server.initializeHeartbeatTimers() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_resetHeartbeatTimer(t *testing.T) {
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
		id string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    time.Duration
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
			got, err := s.resetHeartbeatTimer(tt.args.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("Server.resetHeartbeatTimer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Server.resetHeartbeatTimer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_resetHeartbeatTimerLocked(t *testing.T) {
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
		id  string
		ttl time.Duration
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
			s.resetHeartbeatTimerLocked(tt.args.id, tt.args.ttl)
		})
	}
}

func TestServer_invalidateHeartbeat(t *testing.T) {
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
		id string
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
			s.invalidateHeartbeat(tt.args.id)
		})
	}
}

func TestServer_clearHeartbeatTimer(t *testing.T) {
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
		id string
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
			if err := s.clearHeartbeatTimer(tt.args.id); (err != nil) != tt.wantErr {
				t.Errorf("Server.clearHeartbeatTimer() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_clearAllHeartbeatTimers(t *testing.T) {
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
			if err := s.clearAllHeartbeatTimers(); (err != nil) != tt.wantErr {
				t.Errorf("Server.clearAllHeartbeatTimers() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_heartbeatStats(t *testing.T) {
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
			s.heartbeatStats()
		})
	}
}
