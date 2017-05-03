package server

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"

	uconf "udup/internal/config"
	"udup/internal/server/store"
	"udup/internal"
)

const (
	raftState         = "raft/"
	serfSnapshot      = "serf/snapshot"
	snapshotsRetained = 2

	// serverRPCCache controls how long we keep an idle connection open to a server
	serverRPCCache = 2 * time.Minute

	// serverMaxStreams controsl how many idle streams we keep open to a server
	serverMaxStreams = 64

	// raftLogCacheSize is the maximum number of logs to cache in-memory.
	// This is used to reduce disk I/O for the recently committed entries.
	raftLogCacheSize = 512

	// raftRemoveGracePeriod is how long we wait to allow a RemovePeer
	// to replicate to gracefully leave the cluster.
	raftRemoveGracePeriod = 5 * time.Second
)

// Server is Udup server which manages the job queues,
// schedulers, and notification bus for agents.
type Server struct {
	config *uconf.ServerConfig
	logger *log.Logger

	// Connection pool to other Udup servers
	connPool *ConnPool

	// Endpoints holds our RPC endpoints
	endpoints endpoints

	// The raft instance is used among Udup nodes within the
	// region to protect operations that require strong consistency
	leaderCh      <-chan bool
	raft          *raft.Raft
	raftLayer     *RaftLayer
	raftStore     *raftboltdb.BoltStore
	raftInmem     *raft.InmemStore
	raftTransport *raft.NetworkTransport

	// fsm is the store machine used with Raft
	fsm *udupFSM

	// rpcListener is used to listen for incoming connections
	rpcListener  net.Listener
	rpcServer    *rpc.Server
	rpcAdvertise net.Addr

	// peers is used to track the known Udup servers. This is
	// used for region forwarding and clustering.
	peers      map[string][]*serverParts
	localPeers map[raft.ServerAddress]*serverParts
	peerLock   sync.RWMutex

	// serf is the Serf cluster containing only Udup
	// servers. This is used for multi-region federation
	// and automatic clustering within regions.
	serf *serf.Serf

	// reconcileCh is used to pass events from the serf handler
	// into the leader manager. Mostly used to handle when servers
	// join/leave from the region.
	reconcileCh chan serf.Member

	// eventCh is used to receive events from the serf cluster
	eventCh chan serf.Event

	// evalBroker is used to manage the in-progress evaluations
	// that are waiting to be brokered to a sub-scheduler
	evalBroker *EvalBroker

	// BlockedEvals is used to manage evaluations that are blocked on node
	// capacity changes.
	blockedEvals *BlockedEvals

	// planQueue is used to manage the submitted allocation
	// plans that are waiting to be assessed by the leader
	planQueue *PlanQueue

	// heartbeatTimers track the expiration time of each heartbeat that has
	// a TTL. On expiration, the node status is updated to be 'down'.
	heartbeatTimers     map[string]*time.Timer
	heartbeatTimersLock sync.Mutex

	// Worker used for processing
	workers []*Worker

	left         bool
	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// Holds the RPC endpoints
type endpoints struct {
	Status *Status
	Node   *Node
	Job    *Job
	Eval   *Eval
	Plan   *Plan
	Alloc  *Alloc
}

// NewServer is used to construct a new Udup server from the
// configuration, potentially returning an error
func NewServer(config *uconf.ServerConfig, logger *log.Logger) (*Server, error) {
	// Create an eval broker
	evalBroker, err := NewEvalBroker(config.EvalNackTimeout, config.EvalDeliveryLimit)
	if err != nil {
		return nil, err
	}

	// Create a new blocked eval tracker.
	blockedEvals := NewBlockedEvals(evalBroker)

	// Create a plan queue
	planQueue, err := NewPlanQueue()
	if err != nil {
		return nil, err
	}

	// Create the server
	s := &Server{
		config:       config,
		connPool:     NewPool(config.LogOutput, serverRPCCache, serverMaxStreams),
		logger:       logger,
		rpcServer:    rpc.NewServer(),
		peers:        make(map[string][]*serverParts),
		localPeers:   make(map[raft.ServerAddress]*serverParts),
		reconcileCh:  make(chan serf.Member, 32),
		eventCh:      make(chan serf.Event, 256),
		evalBroker:   evalBroker,
		blockedEvals: blockedEvals,
		planQueue:    planQueue,
		shutdownCh:   make(chan struct{}),
	}

	// Initialize the RPC layer
	if err := s.setupRPC(); err != nil {
		s.Shutdown()
		s.logger.Printf("[ERR] server: failed to start RPC layer: %s", err)
		return nil, fmt.Errorf("Failed to start RPC layer: %v", err)
	}

	// Initialize the Raft server
	if err := s.setupRaft(); err != nil {
		s.Shutdown()
		s.logger.Printf("[ERR] server: failed to start Raft: %s", err)
		return nil, fmt.Errorf("Failed to start Raft: %v", err)
	}

	// Initialize the wan Serf
	s.serf, err = s.setupSerf(config.SerfConfig, s.eventCh, serfSnapshot)
	if err != nil {
		s.Shutdown()
		s.logger.Printf("[ERR] server: failed to start serf WAN: %s", err)
		return nil, fmt.Errorf("Failed to start serf: %v", err)
	}

	// Initialize the scheduling workers
	if err := s.setupWorkers(); err != nil {
		s.Shutdown()
		s.logger.Printf("[ERR] server: failed to start workers: %s", err)
		return nil, fmt.Errorf("Failed to start workers: %v", err)
	}

	// Monitor leadership changes
	go s.monitorLeadership()

	// Start ingesting events for Serf
	go s.serfEventHandler()

	// Start the RPC listeners
	go s.listen()

	// Emit metrics for the eval broker
	go evalBroker.EmitStats(time.Second, s.shutdownCh)

	// Emit metrics for the plan queue
	go planQueue.EmitStats(time.Second, s.shutdownCh)

	// Emit metrics for the blocked eval tracker.
	go blockedEvals.EmitStats(time.Second, s.shutdownCh)

	// Emit metrics
	go s.heartbeatStats()

	// Done
	return s, nil
}

// Shutdown is used to shutdown the server
func (s *Server) Shutdown() error {
	s.logger.Printf("[INFO] server: shutting down server")
	s.shutdownLock.Lock()
	defer s.shutdownLock.Unlock()

	if s.shutdown {
		return nil
	}

	s.shutdown = true
	close(s.shutdownCh)

	if s.serf != nil {
		s.serf.Shutdown()
	}

	if s.raft != nil {
		s.raftTransport.Close()
		s.raftLayer.Close()
		future := s.raft.Shutdown()
		if err := future.Error(); err != nil {
			s.logger.Printf("[WARN] server: Error shutting down raft: %s", err)
		}
		if s.raftStore != nil {
			s.raftStore.Close()
		}
	}

	// Shutdown the RPC listener
	if s.rpcListener != nil {
		s.rpcListener.Close()
	}

	// Close the connection pool
	s.connPool.Shutdown()

	// Close the fsm
	if s.fsm != nil {
		s.fsm.Close()
	}

	return nil
}

// IsShutdown checks if the server is shutdown
func (s *Server) IsShutdown() bool {
	select {
	case <-s.shutdownCh:
		return true
	default:
		return false
	}
}

// Leave is used to prepare for a graceful shutdown of the server
func (s *Server) Leave() error {
	s.logger.Printf("[INFO] server: server starting leave")
	s.left = true

	// Check the number of known peers
	numPeers, err := s.numPeers()
	if err != nil {
		s.logger.Printf("[ERR] server: failed to check raft peers: %v", err)
		return err
	}

	// TODO (alexdadgar) - This will need to be updated once we support node
	// IDs.
	addr := s.raftTransport.LocalAddr()

	// If we are the current leader, and we have any other peers (cluster has multiple
	// servers), we should do a RemovePeer to safely reduce the quorum size. If we are
	// not the leader, then we should issue our leave intention and wait to be removed
	// for some sane period of time.
	isLeader := s.IsLeader()
	if isLeader && numPeers > 1 {
		future := s.raft.RemovePeer(addr)
		if err := future.Error(); err != nil {
			s.logger.Printf("[ERR] server: failed to remove ourself as raft peer: %v", err)
		}
	}

	// Leave the gossip pool
	if s.serf != nil {
		if err := s.serf.Leave(); err != nil {
			s.logger.Printf("[ERR] server: failed to leave Serf cluster: %v", err)
		}
	}

	// If we were not leader, wait to be safely removed from the cluster.
	// We must wait to allow the raft replication to take place, otherwise
	// an immediate shutdown could cause a loss of quorum.
	if !isLeader {
		left := false
		limit := time.Now().Add(raftRemoveGracePeriod)
		for !left && time.Now().Before(limit) {
			// Sleep a while before we check.
			time.Sleep(50 * time.Millisecond)

			// Get the latest configuration.
			future := s.raft.GetConfiguration()
			if err := future.Error(); err != nil {
				s.logger.Printf("[ERR] server: failed to get raft configuration: %v", err)
				break
			}

			// See if we are no longer included.
			left = true
			for _, server := range future.Configuration().Servers {
				if server.Address == addr {
					left = false
					break
				}
			}
		}

		// TODO (alexdadgar) With the old Raft library we used to force the
		// peers set to empty when a graceful leave occurred. This would
		// keep voting spam down if the server was restarted, but it was
		// dangerous because the peers was inconsistent with the logs and
		// snapshots, so it wasn't really safe in all cases for the server
		// to become leader. This is now safe, but the log spam is noisy.
		// The next new version of the library will have a "you are not a
		// peer stop it" behavior that should address this. We will have
		// to evaluate during the RC period if this interim situation is
		// not too confusing for operators.

		// TODO (alexdadgar) When we take a later new version of the Raft
		// library it won't try to complete replication, so this peer
		// may not realize that it has been removed. Need to revisit this
		// and the warning here.
		if !left {
			s.logger.Printf("[WARN] server: failed to leave raft configuration gracefully, timeout")
		}
	}
	return nil
}

// setupRPC is used to setup the RPC listener
func (s *Server) setupRPC() error {
	// Create endpoints
	s.endpoints.Alloc = &Alloc{s}
	s.endpoints.Eval = &Eval{s}
	s.endpoints.Job = &Job{s}
	s.endpoints.Node = &Node{srv: s}
	s.endpoints.Plan = &Plan{s}
	s.endpoints.Status = &Status{s}

	// Register the handlers
	s.rpcServer.Register(s.endpoints.Alloc)
	s.rpcServer.Register(s.endpoints.Eval)
	s.rpcServer.Register(s.endpoints.Job)
	s.rpcServer.Register(s.endpoints.Node)
	s.rpcServer.Register(s.endpoints.Plan)
	s.rpcServer.Register(s.endpoints.Status)

	list, err := net.ListenTCP("tcp", s.config.RPCAddr)
	if err != nil {
		return err
	}
	s.rpcListener = list

	if s.config.RPCAdvertise != nil {
		s.rpcAdvertise = s.config.RPCAdvertise
	} else {
		s.rpcAdvertise = s.rpcListener.Addr()
	}

	// Verify that we have a usable advertise address
	addr, ok := s.rpcAdvertise.(*net.TCPAddr)
	if !ok {
		list.Close()
		return fmt.Errorf("RPC advertise address is not a TCP Address: %v", addr)
	}
	if addr.IP.IsUnspecified() {
		list.Close()
		return fmt.Errorf("RPC advertise address is not advertisable: %v", addr)
	}

	s.raftLayer = NewRaftLayer(s.rpcAdvertise)
	return nil
}

// setupRaft is used to setup and initialize Raft
func (s *Server) setupRaft() error {
	// If we have an unclean exit then attempt to close the Raft store.
	defer func() {
		if s.raft == nil && s.raftStore != nil {
			if err := s.raftStore.Close(); err != nil {
				s.logger.Printf("[ERR] server: failed to close Raft store: %v", err)
			}
		}
	}()

	// Create the FSM
	var err error
	s.fsm, err = NewFSM(s.evalBroker, s.blockedEvals, s.config.LogOutput)
	if err != nil {
		return err
	}

	// Create a transport layer
	trans := raft.NewNetworkTransport(s.raftLayer, 3, s.config.RaftTimeout,
		s.config.LogOutput)
	s.raftTransport = trans

	// Make sure we set the LogOutput.
	s.config.RaftConfig.LogOutput = s.config.LogOutput

	// Our version of Raft protocol requires the LocalID to match the network
	// address of the transport.
	s.config.RaftConfig.LocalID = raft.ServerID(trans.LocalAddr())

	// Build an all in-memory setup for dev mode, otherwise prepare a full
	// disk-based setup.
	var log raft.LogStore
	var stable raft.StableStore
	var snap raft.SnapshotStore
	// Create the base raft path
	path := filepath.Join(s.config.DataDir, raftState)
	if err := ensurePath(path, true); err != nil {
		return err
	}

	// Create the BoltDB backend
	store, err := raftboltdb.NewBoltStore(filepath.Join(path, "raft.db"))
	if err != nil {
		return err
	}
	s.raftStore = store
	stable = store

	// Wrap the store in a LogCache to improve performance
	cacheStore, err := raft.NewLogCache(raftLogCacheSize, store)
	if err != nil {
		store.Close()
		return err
	}
	log = cacheStore

	// Create the snapshot store
	snapshots, err := raft.NewFileSnapshotStore(path, snapshotsRetained, s.config.LogOutput)
	if err != nil {
		if s.raftStore != nil {
			s.raftStore.Close()
		}
		return err
	}
	snap = snapshots

	// For an existing cluster being upgraded to the new version of
	// Raft, we almost never want to run recovery based on the old
	// peers.json file. We create a peers.info file with a helpful
	// note about where peers.json went, and use that as a sentinel
	// to avoid ingesting the old one that first time (if we have to
	// create the peers.info file because it's not there, we also
	// blow away any existing peers.json file).
	peersFile := filepath.Join(path, "peers.json")
	peersInfoFile := filepath.Join(path, "peers.info")
	if _, err := os.Stat(peersInfoFile); os.IsNotExist(err) {
		if err := ioutil.WriteFile(peersInfoFile, []byte(peersInfoContent), 0755); err != nil {
			return fmt.Errorf("failed to write peers.info file: %v", err)
		}

		// Blow away the peers.json file if present, since the
		// peers.info sentinel wasn't there.
		if _, err := os.Stat(peersFile); err == nil {
			if err := os.Remove(peersFile); err != nil {
				return fmt.Errorf("failed to delete peers.json, please delete manually (see peers.info for details): %v", err)
			}
			s.logger.Printf("[INFO] server: deleted peers.json file (see peers.info for details)")
		}
	} else if _, err := os.Stat(peersFile); err == nil {
		s.logger.Printf("[INFO] server: found peers.json file, recovering Raft configuration...")
		configuration, err := raft.ReadPeersJSON(peersFile)
		if err != nil {
			return fmt.Errorf("recovery failed to parse peers.json: %v", err)
		}
		tmpFsm, err := NewFSM(s.evalBroker, s.blockedEvals, s.config.LogOutput)
		if err != nil {
			return fmt.Errorf("recovery failed to make temp FSM: %v", err)
		}
		if err := raft.RecoverCluster(s.config.RaftConfig, tmpFsm,
			log, stable, snap, trans, configuration); err != nil {
			return fmt.Errorf("recovery failed: %v", err)
		}
		if err := os.Remove(peersFile); err != nil {
			return fmt.Errorf("recovery failed to delete peers.json, please delete manually (see peers.info for details): %v", err)
		}
		s.logger.Printf("[INFO] server: deleted peers.json file after successful recovery")
	}

	// If we are in bootstrap or dev mode and the store is clean then we can
	// bootstrap now.
	if s.config.Bootstrap {
		hasState, err := raft.HasExistingState(log, stable, snap)
		if err != nil {
			return err
		}
		if !hasState {
			// TODO (alexdadgar) - This will need to be updated when
			// we add support for node IDs.
			configuration := raft.Configuration{
				Servers: []raft.Server{
					raft.Server{
						ID:      raft.ServerID(trans.LocalAddr()),
						Address: trans.LocalAddr(),
					},
				},
			}
			if err := raft.BootstrapCluster(s.config.RaftConfig,
				log, stable, snap, trans, configuration); err != nil {
				return err
			}
		}
	}

	// Setup the leader channel
	leaderCh := make(chan bool, 1)
	s.config.RaftConfig.NotifyCh = leaderCh
	s.leaderCh = leaderCh

	// Setup the Raft store
	s.raft, err = raft.NewRaft(s.config.RaftConfig, s.fsm, log, stable, snap, trans)
	if err != nil {
		return err
	}
	return nil
}

// setupSerf is used to setup and initialize a Serf
func (s *Server) setupSerf(conf *serf.Config, ch chan serf.Event, path string) (*serf.Serf, error) {
	conf.Init()
	conf.NodeName = fmt.Sprintf("%s.%s", s.config.NodeName, s.config.Region)
	conf.Tags["role"] = "server"
	conf.Tags["region"] = s.config.Region
	conf.Tags["dc"] = s.config.Datacenter
	conf.Tags["build"] = s.config.Build
	conf.Tags["port"] = fmt.Sprintf("%d", s.rpcAdvertise.(*net.TCPAddr).Port)
	if s.config.Bootstrap {
		conf.Tags["bootstrap"] = "1"
	}
	bootstrapExpect := atomic.LoadInt32(&s.config.BootstrapExpect)
	if bootstrapExpect != 0 {
		conf.Tags["expect"] = fmt.Sprintf("%d", bootstrapExpect)
	}
	conf.MemberlistConfig.LogOutput = s.config.LogOutput
	conf.LogOutput = s.config.LogOutput
	conf.EventCh = ch
	conf.SnapshotPath = filepath.Join(s.config.DataDir, path)
	if err := ensurePath(conf.SnapshotPath, false); err != nil {
		return nil, err
	}
	conf.RejoinAfterLeave = true

	// Until Udup supports this fully, we disable automatic resolution.
	// When enabled, the Serf gossip may just turn off if we are the minority
	// node which is rather unexpected.
	conf.EnableNameConflictResolution = false
	return serf.Create(conf)
}

// setupWorkers is used to start the scheduling workers
func (s *Server) setupWorkers() error {
	// Check if all the schedulers are disabled
	if len(s.config.EnabledSchedulers) == 0 || s.config.NumSchedulers == 0 {
		s.logger.Printf("[WARN] server: no enabled schedulers")
		return nil
	}

	// Start the workers
	for i := 0; i < s.config.NumSchedulers; i++ {
		if w, err := NewWorker(s); err != nil {
			return err
		} else {
			s.workers = append(s.workers, w)
		}
	}
	s.logger.Printf("[INFO] server: starting %d scheduling worker(s) for %v",
		s.config.NumSchedulers, s.config.EnabledSchedulers)
	return nil
}

// numPeers is used to check on the number of known peers, including the local
// node.
func (s *Server) numPeers() (int, error) {
	future := s.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return 0, err
	}
	configuration := future.Configuration()
	return len(configuration.Servers), nil
}

// IsLeader checks if this server is the cluster leader
func (s *Server) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

// Join is used to have Udup join the gossip ring
// The target address should be another node listening on the
// Serf address
func (s *Server) Join(addrs []string) (int, error) {
	return s.serf.Join(addrs, true)
}

// LocalMember is used to return the local node
func (c *Server) LocalMember() serf.Member {
	return c.serf.LocalMember()
}

// Members is used to return the members of the serf cluster
func (s *Server) Members() []serf.Member {
	return s.serf.Members()
}

// RemoveFailedNode is used to remove a failed node from the cluster
func (s *Server) RemoveFailedNode(node string) error {
	return s.serf.RemoveFailedNode(node)
}

// Encrypted determines if gossip is encrypted
func (s *Server) Encrypted() bool {
	return s.serf.EncryptionEnabled()
}

// State returns the underlying store store. This should *not*
// be used to modify store directly.
func (s *Server) State() *store.StateStore {
	return s.fsm.State()
}

// Regions returns the known regions in the cluster.
func (s *Server) Regions() []string {
	s.peerLock.RLock()
	defer s.peerLock.RUnlock()

	regions := make([]string, 0, len(s.peers))
	for region, _ := range s.peers {
		regions = append(regions, region)
	}
	sort.Strings(regions)
	return regions
}

// inmemCodec is used to do an RPC call without going over a network
type inmemCodec struct {
	method string
	args   interface{}
	reply  interface{}
	err    error
}

func (i *inmemCodec) ReadRequestHeader(req *rpc.Request) error {
	req.ServiceMethod = i.method
	return nil
}

func (i *inmemCodec) ReadRequestBody(args interface{}) error {
	sourceValue := reflect.Indirect(reflect.Indirect(reflect.ValueOf(i.args)))
	dst := reflect.Indirect(reflect.Indirect(reflect.ValueOf(args)))
	dst.Set(sourceValue)
	return nil
}

func (i *inmemCodec) WriteResponse(resp *rpc.Response, reply interface{}) error {
	if resp.Error != "" {
		i.err = errors.New(resp.Error)
		return nil
	}
	sourceValue := reflect.Indirect(reflect.Indirect(reflect.ValueOf(reply)))
	dst := reflect.Indirect(reflect.Indirect(reflect.ValueOf(i.reply)))
	dst.Set(sourceValue)
	return nil
}

func (i *inmemCodec) Close() error {
	return nil
}

// RPC is used to make a local RPC call
func (s *Server) RPC(method string, args interface{}, reply interface{}) error {
	codec := &inmemCodec{
		method: method,
		args:   args,
		reply:  reply,
	}
	if err := s.rpcServer.ServeRequest(codec); err != nil {
		return err
	}
	return codec.err
}

// Stats is used to return statistics for debugging and insight
// for various sub-systems
func (s *Server) Stats() map[string]map[string]string {
	toString := func(v uint64) string {
		return strconv.FormatUint(v, 10)
	}
	stats := map[string]map[string]string{
		"server": map[string]string{
			"server":        "true",
			"leader":        fmt.Sprintf("%v", s.IsLeader()),
			"leader_addr":   string(s.raft.Leader()),
			"bootstrap":     fmt.Sprintf("%v", s.config.Bootstrap),
			"known_regions": toString(uint64(len(s.peers))),
		},
		"raft":    s.raft.Stats(),
		"serf":    s.serf.Stats(),
		"runtime": internal.RuntimeStats(),
	}

	return stats
}

// Region retuns the region of the server
func (s *Server) Region() string {
	return s.config.Region
}

// Datacenter returns the data center of the server
func (s *Server) Datacenter() string {
	return s.config.Datacenter
}

// GetConfig returns the config of the server for testing purposes only
func (s *Server) GetConfig() *uconf.ServerConfig {
	return s.config
}

// peersInfoContent is used to help operators understand what happened to the
// peers.json file. This is written to a file called peers.info in the same
// location.
const peersInfoContent = `
As of Udup 0.1.0, the peers.json file is only used for recovery
after an outage. It should be formatted as a JSON array containing the address
and port of each Consul server in the cluster, like this:

["10.1.0.1:8191","10.1.0.2:8191","10.1.0.3:8191"]

Under normal operation, the peers.json file will not be present.

When Udup starts for the first time, it will create this peers.info file and
delete any existing peers.json file so that recovery doesn't occur on the first
startup.

Once this peers.info file is present, any peers.json file will be ingested at
startup, and will set the Raft peer configuration manually to recover from an
outage. It's crucial that all servers in the cluster are shut down before
creating the peers.json file, and that all servers receive the same
configuration. Once the peers.json file is successfully ingested and applied, it
will be deleted.

`
