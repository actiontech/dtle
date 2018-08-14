package server

import (
	"net"
	"net/rpc"
	"reflect"
	"sync"
	"testing"
	"time"
	uconf "udup/internal/config"
	ulog "udup/internal/logger"
	"udup/internal/models"
	"udup/internal/server/store"

	"github.com/docker/leadership"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
)

func TestServer_planApply(t *testing.T) {
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
			s.planApply()
		})
	}
}

func TestServer_applyPlan(t *testing.T) {
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
		job    *models.Job
		result *models.PlanResult
		snap   *store.StateSnapshot
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
			got, err := s.applyPlan(tt.args.job, tt.args.result, tt.args.snap)
			if (err != nil) != tt.wantErr {
				t.Errorf("Server.applyPlan() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Server.applyPlan() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_asyncPlanWait(t *testing.T) {
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
		waitCh  chan struct{}
		future  raft.ApplyFuture
		result  *models.PlanResult
		pending *pendingPlan
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
			s.asyncPlanWait(tt.args.waitCh, tt.args.future, tt.args.result, tt.args.pending)
		})
	}
}

func Test_evaluatePlan(t *testing.T) {
	type args struct {
		pool *EvaluatePool
		snap *store.StateSnapshot
		plan *models.Plan
	}
	tests := []struct {
		name    string
		args    args
		want    *models.PlanResult
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evaluatePlan(tt.args.pool, tt.args.snap, tt.args.plan)
			if (err != nil) != tt.wantErr {
				t.Errorf("evaluatePlan() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("evaluatePlan() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_evaluateNodePlan(t *testing.T) {
	type args struct {
		snap   *store.StateSnapshot
		plan   *models.Plan
		nodeID string
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evaluateNodePlan(tt.args.snap, tt.args.plan, tt.args.nodeID)
			if (err != nil) != tt.wantErr {
				t.Errorf("evaluateNodePlan() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("evaluateNodePlan() = %v, want %v", got, tt.want)
			}
		})
	}
}
