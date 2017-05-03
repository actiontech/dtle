package client

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/consul/lib"
	"github.com/hashicorp/go-multierror"
	"github.com/mitchellh/hashstructure"
	gnatsd "github.com/nats-io/gnatsd/server"
	stand "github.com/nats-io/nats-streaming-server/server"
	"github.com/shirou/gopsutil/host"

	"udup/internal"
	"udup/internal/client/driver"
	"udup/internal/config"
	uconf "udup/internal/config"
	"udup/internal/models"
	"udup/internal/server"
)

const (
	// clientRPCCache controls how long we keep an idle connection
	// open to a server
	clientRPCCache = 5 * time.Minute

	// clientMaxStreams controsl how many idle streams we keep
	// open to a server
	clientMaxStreams = 2

	// registerRetryIntv is minimum interval on which we retry
	// registration. We pick a value between this and 2x this.
	registerRetryIntv = 15 * time.Second

	// getAllocRetryIntv is minimum interval on which we retry
	// to fetch allocations. We pick a value between this and 2x this.
	getAllocRetryIntv = 30 * time.Second

	// stateSnapshotIntv is how often the client snapshots state
	stateSnapshotIntv = 60 * time.Second

	// initialHeartbeatStagger is used to stagger the interval between
	// starting and the intial heartbeat. After the intial heartbeat,
	// we switch to using the TTL specified by the servers.
	initialHeartbeatStagger = 10 * time.Second

	// nodeUpdateRetryIntv is how often the client checks for updates to the
	// node attributes or meta map.
	nodeUpdateRetryIntv = 5 * time.Second

	// allocSyncIntv is the batching period of allocation updates before they
	// are synced with the server.
	allocSyncIntv = 200 * time.Millisecond

	// allocSyncRetryIntv is the interval on which we retry updating
	// the status of the allocation
	allocSyncRetryIntv = 5 * time.Second
)

// ClientStatsReporter exposes all the APIs related to resource usage of a Udup
// Client
type ClientStatsReporter interface {
	// GetAllocStats returns the AllocStatsReporter for the passed allocation.
	// If it does not exist an error is reported.
	GetAllocStats(allocID string) (AllocStatsReporter, error)
}

// Client is used to implement the client interaction with Udup. Clients
// are expected to register as a schedulable node to the servers, and to
// run allocations as determined by the servers.
type Client struct {
	config *uconf.ClientConfig
	start  time.Time

	// configCopy is a copy that should be passed to alloc-runners.
	configCopy *uconf.ClientConfig
	configLock sync.RWMutex

	logger *log.Logger

	connPool *server.ConnPool

	// servers is the (optionally prioritized) list of server servers
	servers *serverlist

	// heartbeat related times for tracking how often to heartbeat
	lastHeartbeat time.Time
	heartbeatTTL  time.Duration
	heartbeatLock sync.Mutex

	// triggerDiscoveryCh triggers Consul discovery; see triggerDiscovery
	triggerDiscoveryCh chan struct{}

	// discovered will be ticked whenever Consul discovery completes
	// succesfully
	serversDiscoveredCh chan struct{}

	// allocs is the current set of allocations
	allocs    map[string]*Allocator
	allocLock sync.RWMutex

	// blockedAllocations are allocations which are blocked because their
	// chained allocations haven't finished running
	blockedAllocations map[string]*models.Allocation
	blockedAllocsLock  sync.RWMutex

	// migratingAllocs is the set of allocs whose data migration is in flight
	migratingAllocs     map[string]*migrateAllocCtrl
	migratingAllocsLock sync.Mutex

	// allocUpdates stores allocations that need to be synced to the server.
	allocUpdates chan *models.Allocation

	workUpdates chan *models.TaskUpdate

	stand *stand.StanServer

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// migrateAllocCtrl indicates whether migration is complete
type migrateAllocCtrl struct {
	alloc  *models.Allocation
	ch     chan struct{}
	closed bool
	chLock sync.Mutex
}

func newMigrateAllocCtrl(alloc *models.Allocation) *migrateAllocCtrl {
	return &migrateAllocCtrl{
		ch:    make(chan struct{}),
		alloc: alloc,
	}
}

func (m *migrateAllocCtrl) closeCh() {
	m.chLock.Lock()
	defer m.chLock.Unlock()

	if m.closed {
		return
	}

	// If channel is not closed then close it
	m.closed = true
	close(m.ch)
}

var (
	// noServersErr is returned by the RPC method when the client has no
	// configured servers. This is used to trigger Consul discovery if
	// enabled.
	noServersErr = errors.New("no servers")
)

// NewClient is used to create a new client from the given configuration
func NewClient(cfg *uconf.ClientConfig, logger *log.Logger) (*Client, error) {
	// Create the client
	c := &Client{
		config:              cfg,
		start:               time.Now(),
		connPool:            server.NewPool(cfg.LogOutput, clientRPCCache, clientMaxStreams),
		logger:              logger,
		allocs:              make(map[string]*Allocator),
		blockedAllocations:  make(map[string]*models.Allocation),
		allocUpdates:        make(chan *models.Allocation, 64),
		workUpdates:          make(chan *models.TaskUpdate, 64),
		shutdownCh:          make(chan struct{}),
		migratingAllocs:     make(map[string]*migrateAllocCtrl),
		servers:             newServerList(),
		triggerDiscoveryCh:  make(chan struct{}),
		serversDiscoveredCh: make(chan struct{}),
	}

	// Initialize the client
	if err := c.init(); err != nil {
		return nil, fmt.Errorf("failed to initialize client: %v", err)
	}

	// Setup the node
	if err := c.setupNode(); err != nil {
		return nil, fmt.Errorf("node setup failed: %v", err)
	}

	if err := c.setupNatsServer(); err != nil {
		return nil, fmt.Errorf("nats server setup failed: %v", err)
	}

	// Scan for drivers
	if err := c.setupDrivers(); err != nil {
		return nil, fmt.Errorf("driver setup failed: %v", err)
	}

	// Store the config copy before restoring state but after it has been
	// initialized.
	c.configLock.Lock()
	c.configCopy = c.config.Copy()
	c.configLock.Unlock()

	// Set the preconfigured list of static servers
	c.configLock.RLock()
	if len(c.configCopy.Servers) > 0 {
		if err := c.SetServers(c.configCopy.Servers); err != nil {
			logger.Printf("[WARN] client: None of the configured servers are valid: %v", err)
		}
	}
	c.configLock.RUnlock()

	// Restore the state
	if err := c.restoreState(); err != nil {
		return nil, fmt.Errorf("failed to restore state: %v", err)
	}

	// Register and then start heartbeating to the servers.
	go c.registerAndHeartbeat()

	// Begin periodic snapshotting of state.
	go c.periodicSnapshot()

	// Begin syncing allocations to the server
	go c.allocSync()

	// Start the client!
	go c.run()

	c.logger.Printf("[INFO] client: Node ID %q", c.Node().ID)
	return c, nil
}

// init is used to initialize the client and perform any setup
// needed before we begin starting its various components.
func (c *Client) init() error {
	// Ensure the state dir exists if we have one
	if c.config.StateDir != "" {
		if err := os.MkdirAll(c.config.StateDir, 0700); err != nil {
			return fmt.Errorf("failed creating state dir: %s", err)
		}

	} else {
		// Othewise make a temp directory to use.
		p, err := ioutil.TempDir("", "UdupClient")
		if err != nil {
			return fmt.Errorf("failed creating temporary directory for the StateDir: %v", err)
		}

		p, err = filepath.EvalSymlinks(p)
		if err != nil {
			return fmt.Errorf("failed to find temporary directory for the StateDir: %v", err)
		}

		c.config.StateDir = p
	}
	c.logger.Printf("[INFO] client: using state directory %v", c.config.StateDir)

	// Ensure the alloc dir exists if we have one
	if c.config.AllocDir != "" {
		if err := os.MkdirAll(c.config.AllocDir, 0755); err != nil {
			return fmt.Errorf("failed creating alloc dir: %s", err)
		}
	} else {
		// Othewise make a temp directory to use.
		p, err := ioutil.TempDir("", "UdupClient")
		if err != nil {
			return fmt.Errorf("failed creating temporary directory for the AllocDir: %v", err)
		}

		p, err = filepath.EvalSymlinks(p)
		if err != nil {
			return fmt.Errorf("failed to find temporary directory for the AllocDir: %v", err)
		}

		// Change the permissions to have the execute bit
		if err := os.Chmod(p, 0755); err != nil {
			return fmt.Errorf("failed to change directory permissions for the AllocDir: %v", err)
		}

		c.config.AllocDir = p
	}

	c.logger.Printf("[INFO] client: using alloc directory %v", c.config.AllocDir)
	return nil
}

// Leave is used to prepare the client to leave the cluster
func (c *Client) Leave() error {
	// TODO
	return nil
}

// Datacenter returns the datacenter for the given client
func (c *Client) Datacenter() string {
	c.configLock.RLock()
	dc := c.configCopy.Node.Datacenter
	c.configLock.RUnlock()
	return dc
}

// Region returns the region for the given client
func (c *Client) Region() string {
	return c.config.Region
}

// Shutdown is used to tear down the client
func (c *Client) Shutdown() error {
	c.logger.Printf("[INFO] client: shutting down")
	c.shutdownLock.Lock()
	defer c.shutdownLock.Unlock()

	if c.shutdown {
		return nil
	}

	c.stand.Shutdown()
	c.shutdown = true
	close(c.shutdownCh)
	c.connPool.Shutdown()
	return c.saveState()
}

// RPC is used to forward an RPC call to a server server, or fail if no servers.
func (c *Client) RPC(method string, args interface{}, reply interface{}) error {
	// Invoke the RPCHandler if it exists
	if c.config.RPCHandler != nil {
		return c.config.RPCHandler.RPC(method, args, reply)
	}

	servers := c.servers.all()
	if len(servers) == 0 {
		return noServersErr
	}

	var mErr multierror.Error
	for _, s := range servers {
		// Make the RPC request
		if err := c.connPool.RPC(c.Region(), s.addr, method, args, reply); err != nil {
			errmsg := fmt.Errorf("RPC failed to server %s: %v", s.addr, err)
			mErr.Errors = append(mErr.Errors, errmsg)
			c.logger.Printf("[DEBUG] client: %v", errmsg)
			c.servers.failed(s)
			continue
		}
		c.servers.good(s)
		return nil
	}

	return mErr.ErrorOrNil()
}

// Stats is used to return statistics for debugging and insight
// for various sub-systems
func (c *Client) Stats() map[string]map[string]string {
	c.allocLock.RLock()
	numAllocs := len(c.allocs)
	c.allocLock.RUnlock()

	c.heartbeatLock.Lock()
	defer c.heartbeatLock.Unlock()
	stats := map[string]map[string]string{
		"client": map[string]string{
			"node_id":         c.Node().ID,
			"known_servers":   c.servers.all().String(),
			"num_allocations": strconv.Itoa(numAllocs),
			"last_heartbeat":  fmt.Sprintf("%v", time.Since(c.lastHeartbeat)),
			"heartbeat_ttl":   fmt.Sprintf("%v", c.heartbeatTTL),
		},
		"runtime": internal.RuntimeStats(),
	}
	return stats
}

// Node returns the locally registered node
func (c *Client) Node() *models.Node {
	c.configLock.RLock()
	defer c.configLock.RUnlock()
	return c.config.Node
}

// StatsReporter exposes the various APIs related resource usage of a Udup
// client
func (c *Client) StatsReporter() ClientStatsReporter {
	return c
}

func (c *Client) GetAllocStats(allocID string) (AllocStatsReporter, error) {
	c.allocLock.RLock()
	defer c.allocLock.RUnlock()
	ar, ok := c.allocs[allocID]
	if !ok {
		return nil, fmt.Errorf("unknown allocation ID %q", allocID)
	}
	return ar.StatsReporter(), nil
}

// GetClientAlloc returns the allocation from the client
func (c *Client) GetClientAlloc(allocID string) (*models.Allocation, error) {
	all := c.allAllocs()
	alloc, ok := all[allocID]
	if !ok {
		return nil, fmt.Errorf("unknown allocation ID %q", allocID)
	}
	return alloc, nil
}

// GetServers returns the list of server servers this client is aware of.
func (c *Client) GetServers() []string {
	endpoints := c.servers.all()
	res := make([]string, len(endpoints))
	for i := range endpoints {
		res[i] = endpoints[i].addr.String()
	}
	return res
}

// SetServers sets a new list of server servers to connect to. As long as one
// server is resolvable no error is returned.
func (c *Client) SetServers(servers []string) error {
	endpoints := make([]*endpoint, 0, len(servers))
	var merr multierror.Error
	for _, s := range servers {
		addr, err := resolveServer(s)
		if err != nil {
			c.logger.Printf("[DEBUG] client: ignoring server %s due to resolution error: %v", s, err)
			merr.Errors = append(merr.Errors, err)
			continue
		}

		// Valid endpoint, append it without a priority as this API
		// doesn't support different priorities for different servers
		endpoints = append(endpoints, &endpoint{name: s, addr: addr})
	}

	// Only return errors if no servers are valid
	if len(endpoints) == 0 {
		if len(merr.Errors) > 0 {
			return merr.ErrorOrNil()
		}
		return noServersErr
	}

	c.servers.set(endpoints)
	return nil
}

// restoreState is used to restore our state from the data dir
func (c *Client) restoreState() error {
	// Scan the directory
	list, err := ioutil.ReadDir(filepath.Join(c.config.StateDir, "alloc"))
	if err != nil && os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to list alloc state: %v", err)
	}

	// Load each alloc back
	var mErr multierror.Error
	for _, entry := range list {
		id := entry.Name()
		alloc := &models.Allocation{ID: id}
		c.configLock.RLock()
		ar := NewAllocator(c.logger, c.configCopy, c.updateAllocStatus, alloc,c.workUpdates)
		c.configLock.RUnlock()
		c.allocLock.Lock()
		c.allocs[id] = ar
		c.allocLock.Unlock()
		if err := ar.RestoreState(); err != nil {
			c.logger.Printf("[ERR] client: failed to restore state for alloc %s: %v", id, err)
			mErr.Errors = append(mErr.Errors, err)
		} else {
			go ar.Run()
		}
	}
	return mErr.ErrorOrNil()
}

// saveState is used to snapshot our state into the data dir
func (c *Client) saveState() error {
	var mErr multierror.Error
	for id, ar := range c.getAllocRunners() {
		if err := ar.SaveState(); err != nil {
			c.logger.Printf("[ERR] client: failed to save state for alloc %s: %v",
				id, err)
			mErr.Errors = append(mErr.Errors, err)
		}
	}
	return mErr.ErrorOrNil()
}

// getAllocRunners returns a snapshot of the current set of alloc runners.
func (c *Client) getAllocRunners() map[string]*Allocator {
	c.allocLock.RLock()
	defer c.allocLock.RUnlock()
	runners := make(map[string]*Allocator, len(c.allocs))
	for id, ar := range c.allocs {
		runners[id] = ar
	}
	return runners
}

// nodeID restores, or generates if necessary, a unique node ID.
// The node ID is, if available, a persistent unique ID.
func (c *Client) nodeID() (id string, err error) {
	var hostID string
	hostInfo, err := host.Info()
	if !c.config.NoHostUUID && err == nil && internal.IsUUID(hostInfo.HostID) {
		hostID = hostInfo.HostID
	} else {
		// Generate a random hostID if no constant ID is available on
		// this platform.
		hostID = models.GenerateUUID()
	}

	// Attempt to read existing ID
	idPath := filepath.Join(c.config.StateDir, "client-id")
	idBuf, err := ioutil.ReadFile(idPath)
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}

	// Use existing ID if any
	if len(idBuf) != 0 {
		id = strings.ToLower(string(idBuf))
	} else {
		id = hostID

		// Persist the ID
		if err := ioutil.WriteFile(idPath, []byte(id), 0700); err != nil {
			return "", err
		}
	}

	return id, nil
}

// setupNode is used to setup the initial node
func (c *Client) setupNode() error {
	node := c.config.Node
	if node == nil {
		node = &models.Node{}
		c.config.Node = node
	}
	// Generate an ID for the node
	id, err := c.nodeID()
	if err != nil {
		return fmt.Errorf("node ID setup failed: %v", err)
	}

	node.ID = id
	if node.Attributes == nil {
		node.Attributes = make(map[string]string)
	}
	if node.Datacenter == "" {
		node.Datacenter = "dc1"
	}
	if node.Name == "" {
		node.Name, _ = os.Hostname()
	}
	if node.Name == "" {
		node.Name = node.ID
	}
	node.Status = models.NodeStatusInit
	return nil
}

func (c *Client) setupNatsServer() error {
	host, port, err := net.SplitHostPort(c.config.NatsConfig.Addr)
	p, err := strconv.Atoi(port)
	if err != nil {
		return err
	}

	nOpts := gnatsd.Options{
		Host:       host,
		Port:       p,
		MaxPayload: (100 * 1024 * 1024),
		Trace:      true,
		Debug:      true,
	}
	c.logger.Printf("[DEBUG] client: starting nats streaming server [%s]", c.config.NatsConfig.Addr)
	sOpts := stand.GetDefaultOptions()
	sOpts.ID = config.DefaultClusterID
	if c.config.NatsConfig.StoreType == "file" {
		sOpts.StoreType = c.config.NatsConfig.StoreType
		sOpts.FilestoreDir = c.config.NatsConfig.FilestoreDir
	}
	s, err := stand.RunServerWithOpts(sOpts, &nOpts)
	if err != nil {
		return err
	}
	c.stand = s
	return nil
}

// setupDrivers is used to find the available drivers
func (c *Client) setupDrivers() error {
	var avail []string
	driverCtx := driver.NewDriverContext("", "", c.config, c.config.Node, c.logger)
	for name := range driver.BuiltinDrivers {
		_, err := driver.NewDriver(name, driverCtx)
		if err != nil {
			return err
		}

		avail = append(avail, name)
		c.config.Node.Attributes[fmt.Sprintf("driver.%s", name)] = "1"
	}

	c.logger.Printf("[DEBUG] client: available drivers %v", avail)

	return nil
}

// retryIntv calculates a retry interval value given the base
func (c *Client) retryIntv(base time.Duration) time.Duration {
	return base + lib.RandomStagger(base)
}

// registerAndHeartbeat is a long lived goroutine used to register the client
// and then start heartbeatng to the server.
func (c *Client) registerAndHeartbeat() {
	// Register the node
	c.retryRegisterNode()

	// Start watching changes for node changes
	go c.watchNodeUpdates()

	// Setup the heartbeat timer, for the initial registration
	// we want to do this quickly. We want to do it extra quickly
	// in development mode.
	var heartbeat <-chan time.Time
	heartbeat = time.After(lib.RandomStagger(initialHeartbeatStagger))

	for {
		select {
		case <-c.serversDiscoveredCh:
		case <-heartbeat:
		case <-c.shutdownCh:
			return
		}

		if err := c.updateNodeStatus(); err != nil {
			// The servers have changed such that this node has not been
			// registered before
			if strings.Contains(err.Error(), "node not found") {
				// Re-register the node
				c.logger.Printf("[INFO] client: re-registering node")
				c.retryRegisterNode()
				heartbeat = time.After(lib.RandomStagger(initialHeartbeatStagger))
			} else {
				intv := c.retryIntv(registerRetryIntv)
				c.logger.Printf("[ERR] client: heartbeating failed. Retrying in %v: %v", intv, err)
				heartbeat = time.After(intv)

				// if heartbeating fails, trigger Consul discovery
				c.triggerDiscovery()
			}
		} else {
			c.heartbeatLock.Lock()
			heartbeat = time.After(c.heartbeatTTL)
			c.heartbeatLock.Unlock()
		}
	}
}

// periodicSnapshot is a long lived goroutine used to periodically snapshot the
// state of the client
func (c *Client) periodicSnapshot() {
	// Create a snapshot timer
	snapshot := time.After(stateSnapshotIntv)

	for {
		select {
		case <-snapshot:
			snapshot = time.After(stateSnapshotIntv)
			if err := c.saveState(); err != nil {
				c.logger.Printf("[ERR] client: failed to save state: %v", err)
			}

		case <-c.shutdownCh:
			return
		}
	}
}

// run is a long lived goroutine used to run the client
func (c *Client) run() {
	time.Sleep(5 * time.Second)
	// Watch for changes in allocations
	allocUpdates := make(chan *allocUpdates, 8)
	jobUpdates := make(chan *jobUpdates, 8)
	go c.watchAllocations(allocUpdates,jobUpdates)

	for {
		select {
		case update := <-allocUpdates:
			c.runAllocs(update)

		case <-c.shutdownCh:
			return
		}
	}
}

// hasNodeChanged calculates a hash for the node attributes- and meta map.
// The new hash values are compared against the old (passed-in) hash values to
// determine if the node properties have changed. It returns the new hash values
// in case they are different from the old hash values.
func (c *Client) hasNodeChanged(oldAttrHash uint64, oldMetaHash uint64) (bool, uint64, uint64) {
	c.configLock.RLock()
	defer c.configLock.RUnlock()
	newAttrHash, err := hashstructure.Hash(c.config.Node.Attributes, nil)
	if err != nil {
		c.logger.Printf("[DEBUG] client: unable to calculate node attributes hash: %v", err)
	}

	if newAttrHash != oldAttrHash {
		return true, newAttrHash, 0
	}
	return false, oldAttrHash, 0
}

// retryRegisterNode is used to register the node or update the registration and
// retry in case of failure.
func (c *Client) retryRegisterNode() {
	for {
		err := c.registerNode()
		if err == nil {
			// Registered!
			return
		}

		if err == noServersErr {
			c.logger.Print("[DEBUG] client: registration waiting on servers")
			c.triggerDiscovery()
		} else {
			c.logger.Printf("[ERR] client: registration failure: %v", err)
		}
		select {
		case <-c.serversDiscoveredCh:
		case <-time.After(c.retryIntv(registerRetryIntv)):
		case <-c.shutdownCh:
			return
		}
	}
}

// registerNode is used to register the node or update the registration
func (c *Client) registerNode() error {
	node := c.Node()
	req := models.NodeRegisterRequest{
		Node:         node,
		WriteRequest: models.WriteRequest{Region: c.Region()},
	}
	var resp models.NodeUpdateResponse
	if err := c.RPC("Node.Register", &req, &resp); err != nil {
		return err
	}

	// Update the node status to ready after we register.
	c.configLock.Lock()
	node.Status = models.NodeStatusReady
	c.configLock.Unlock()

	c.logger.Printf("[INFO] client: node registration complete")
	if len(resp.EvalIDs) != 0 {
		c.logger.Printf("[DEBUG] client: %d evaluations triggered by node registration", len(resp.EvalIDs))
	}

	c.heartbeatLock.Lock()
	defer c.heartbeatLock.Unlock()
	c.lastHeartbeat = time.Now()
	c.heartbeatTTL = resp.HeartbeatTTL
	return nil
}

// updateNodeStatus is used to heartbeat and update the status of the node
func (c *Client) updateNodeStatus() error {
	c.heartbeatLock.Lock()
	defer c.heartbeatLock.Unlock()

	node := c.Node()
	req := models.NodeUpdateStatusRequest{
		NodeID:       node.ID,
		Status:       models.NodeStatusReady,
		WriteRequest: models.WriteRequest{Region: c.Region()},
	}
	var resp models.NodeUpdateResponse
	if err := c.RPC("Node.UpdateStatus", &req, &resp); err != nil {
		c.triggerDiscovery()
		return fmt.Errorf("failed to update status: %v", err)
	}
	if len(resp.EvalIDs) != 0 {
		c.logger.Printf("[DEBUG] client: %d evaluations triggered by node update", len(resp.EvalIDs))
	}
	if resp.Index != 0 {
		c.logger.Printf("[DEBUG] client: state updated to %s", req.Status)
	}

	// Update heartbeat time and ttl
	c.lastHeartbeat = time.Now()
	c.heartbeatTTL = resp.HeartbeatTTL

	// Convert []*NodeServerInfo to []*endpoints
	localdc := c.Datacenter()
	servers := make(endpoints, 0, len(resp.Servers))
	for _, s := range resp.Servers {
		addr, err := resolveServer(s.RPCAdvertiseAddr)
		if err != nil {
			continue
		}
		e := endpoint{name: s.RPCAdvertiseAddr, addr: addr}
		if s.Datacenter != localdc {
			// server is non-local; de-prioritize
			e.priority = 1
		}
		servers = append(servers, &e)
	}
	if len(servers) == 0 {
		return fmt.Errorf("server returned no valid servers")
	}
	c.servers.set(servers)

	// Begin polling Consul if there is no Udup leader.  We could be
	// heartbeating to a Udup server that is in the minority of a
	// partition of the Udup server quorum, but this Udup Agent still
	// has connectivity to the existing majority of Udup Servers, but
	// only if it queries Consul.
	if resp.LeaderRPCAddr == "" {
		c.triggerDiscovery()
	}

	return nil
}

// updateAllocStatus is used to update the status of an allocation
func (c *Client) updateAllocStatus(alloc *models.Allocation) {
	// If this alloc was blocking another alloc and transitioned to a
	// terminal state then start the blocked allocation
	c.blockedAllocsLock.Lock()
	if blockedAlloc, ok := c.blockedAllocations[alloc.ID]; ok && alloc.Terminated() {
		if err := c.addAlloc(blockedAlloc); err != nil {
			c.logger.Printf("[ERR] client: failed to add alloc which was previously blocked %q: %v",
				blockedAlloc.ID, err)
		}
		delete(c.blockedAllocations, blockedAlloc.PreviousAllocation)
	}
	c.blockedAllocsLock.Unlock()

	// Strip all the information that can be reconstructed at the server.  Only
	// send the fields that are updatable by the client.
	stripped := new(models.Allocation)
	stripped.ID = alloc.ID
	stripped.NodeID = c.Node().ID
	stripped.TaskStates = alloc.TaskStates
	stripped.ClientStatus = alloc.ClientStatus
	stripped.ClientDescription = alloc.ClientDescription

	select {
	case c.allocUpdates <- stripped:
	case <-c.shutdownCh:
	}
}

// allocSync is a long lived function that batches allocation updates to the
// server.
func (c *Client) allocSync() {
	staggered := false
	syncTicker := time.NewTicker(allocSyncIntv)
	aUpdates := make(map[string]*models.Allocation)
	jUpdates := make(map[string]*models.TaskUpdate)
	for {
		select {
		case <-c.shutdownCh:
			syncTicker.Stop()
			return
		case alloc := <-c.allocUpdates:
			// Batch the allocation updates until the timer triggers.
			aUpdates[alloc.ID] = alloc

		case update := <-c.workUpdates:
			jUpdates[update.JobID] = update

		case <-syncTicker.C:
			// Fast path if there are no updates
			if len(aUpdates) != 0 {
				sync := make([]*models.Allocation, 0, len(aUpdates))
				for _, alloc := range aUpdates {
					sync = append(sync, alloc)
				}

				// Send to server.
				args := models.AllocUpdateRequest{
					Alloc:        sync,
					WriteRequest: models.WriteRequest{Region: c.Region()},
				}

				var resp models.GenericResponse
				if err := c.RPC("Node.UpdateAlloc", &args, &resp); err != nil {
					c.logger.Printf("[ERR] client: failed to update allocations: %v", err)
					syncTicker.Stop()
					syncTicker = time.NewTicker(c.retryIntv(allocSyncRetryIntv))
					staggered = true
				} else {
					aUpdates = make(map[string]*models.Allocation)
					if staggered {
						syncTicker.Stop()
						syncTicker = time.NewTicker(allocSyncIntv)
						staggered = false
					}
				}
			}
			if len(jUpdates) != 0 {
				sync := make([]*models.TaskUpdate, 0, len(jUpdates))
				for _, ju := range jUpdates {
					sync = append(sync, ju)
				}

				// Send to server.
				args := models.JobUpdateRequest{
					JobUpdates:        sync,
					WriteRequest: models.WriteRequest{Region: c.Region()},
				}

				var resp models.GenericResponse
				if err := c.RPC("Node.UpdateJob", &args, &resp); err != nil {
					c.logger.Printf("[ERR] client: failed to update allocations: %v", err)
					syncTicker.Stop()
					syncTicker = time.NewTicker(c.retryIntv(allocSyncRetryIntv))
					staggered = true
				} else {
					jUpdates = make(map[string]*models.TaskUpdate)
					if staggered {
						syncTicker.Stop()
						syncTicker = time.NewTicker(allocSyncIntv)
						staggered = false
					}
				}
			}
		}
	}
}

type jobUpdates struct {
	pulled map[string]string
}

// allocUpdates holds the results of receiving updated allocations from the
// servers.
type allocUpdates struct {
	// pulled is the set of allocations that were downloaded from the servers.
	pulled map[string]*models.Allocation

	// filtered is the set of allocations that were not pulled because their
	// AllocModifyIndex didn't change.
	filtered map[string]struct{}
}

// watchAllocations is used to scan for updates to allocations
func (c *Client) watchAllocations(updates chan *allocUpdates,jUpdates chan *jobUpdates) {
	// The request and response for getting the map of allocations that should
	// be running on the Node to their AllocModifyIndex which is incremented
	// when the allocation is updated by the servers.
	n := c.Node()
	req := models.NodeSpecificRequest{
		NodeID: n.ID,
		QueryOptions: models.QueryOptions{
			Region:     c.Region(),
			AllowStale: true,
		},
	}
	var resp models.NodeClientAllocsResponse

	// The request and response for pulling down the set of allocations that are
	// new, or updated server side.
	allocsReq := models.AllocsGetRequest{
		QueryOptions: models.QueryOptions{
			Region:     c.Region(),
			AllowStale: true,
		},
	}
	var allocsResp models.AllocsGetResponse

	//OUTER:
	for {
		// Get the allocation modify index map, blocking for updates. We will
		// use this to determine exactly what allocations need to be downloaded
		// in full.
		resp = models.NodeClientAllocsResponse{}
		err := c.RPC("Node.GetClientAllocs", &req, &resp)
		if err != nil {
			// Shutdown often causes EOF errors, so check for shutdown first
			select {
			case <-c.shutdownCh:
				return
			default:
			}

			if err != noServersErr {
				c.logger.Printf("[ERR] client: failed to query for node allocations: %v", err)
			}
			retry := c.retryIntv(getAllocRetryIntv)
			select {
			case <-c.serversDiscoveredCh:
				continue
			case <-time.After(retry):
				continue
			case <-c.shutdownCh:
				return
			}
		}

		// Check for shutdown
		select {
		case <-c.shutdownCh:
			return
		default:
		}

		// Filter all allocations whose AllocModifyIndex was not incremented.
		// These are the allocations who have either not been updated, or whose
		// updates are a result of the client sending an update for the alloc.
		// This lets us reduce the network traffic to the server as we don't
		// need to pull all the allocations.
		var pull []string
		filtered := make(map[string]struct{})
		runners := c.getAllocRunners()
		var pullIndex uint64
		for allocID, modifyIndex := range resp.Allocs {
			// Pull the allocation if we don't have an alloc runner for the
			// allocation or if the alloc runner requires an updated allocation.
			runner, ok := runners[allocID]
			if !ok || runner.shouldUpdate(modifyIndex) {
				// Only pull allocs that are required. Filtered
				// allocs might be at a higher index, so ignore
				// it.
				if modifyIndex > pullIndex {
					pullIndex = modifyIndex
				}
				pull = append(pull, allocID)
			} else {
				filtered[allocID] = struct{}{}
			}
		}

		// Pull the allocations that passed filtering.
		allocsResp.Allocs = nil
		var pulledAllocs map[string]*models.Allocation
		if len(pull) != 0 {
			// Pull the allocations that need to be updated.
			allocsReq.AllocIDs = pull
			allocsReq.MinQueryIndex = pullIndex - 1
			allocsResp = models.AllocsGetResponse{}
			if err := c.RPC("Alloc.GetAllocs", &allocsReq, &allocsResp); err != nil {
				c.logger.Printf("[ERR] client: failed to query updated allocations: %v", err)
				retry := c.retryIntv(getAllocRetryIntv)
				select {
				case <-c.serversDiscoveredCh:
					continue
				case <-time.After(retry):
					continue
				case <-c.shutdownCh:
					return
				}
			}

			// Ensure that we received all the allocations we wanted
			pulledAllocs = make(map[string]*models.Allocation, len(allocsResp.Allocs))
			for _, alloc := range allocsResp.Allocs {
				if alloc.Task == models.TaskTypeSrc {
					args := models.JobSpecificRequest{
						JobID:     alloc.Job.ID,
						AllAllocs: true,
						QueryOptions: models.QueryOptions{
							Region:     c.Region(),
							AllowStale: true,
						},
					}
					var out models.JobAllocationsResponse
					if err := c.RPC("Job.Allocations", &args, &out); err != nil {
						c.logger.Printf("[ERR] client: failed to query job allocations: %v", err)
						retry := c.retryIntv(getAllocRetryIntv)
						select {
						case <-c.serversDiscoveredCh:
							continue
						case <-time.After(retry):
							continue
						case <-c.shutdownCh:
							return
						}
					}
					for _, ja := range out.Allocations {
						if ja.Task!=alloc.Task && ja.ClientStatus == models.TaskStateRunning{
							pulledAllocs[alloc.ID] = alloc
						}
					}
				}else{
					pulledAllocs[alloc.ID] = alloc
				}
			}

			/*for _, desiredID := range pull {
				if _, ok := pulledAllocs[desiredID]; !ok {
					// We didn't get everything we wanted. Do not update the
					// MinQueryIndex, sleep and then retry.
					wait := c.retryIntv(2 * time.Second)
					select {
					case <-time.After(wait):
						// Wait for the server we contact to receive the
						// allocations
						continue OUTER
					case <-c.shutdownCh:
						return
					}
				}
			}*/

			// Check for shutdown
			select {
			case <-c.shutdownCh:
				return
			default:
			}
		}

		c.logger.Printf("[DEBUG] client: updated allocations at index %d (total %d) (pulled %d) (filtered %d)",
			resp.Index, len(resp.Allocs), len(allocsResp.Allocs), len(filtered))

		// Update the query index.
		if resp.Index > req.MinQueryIndex {
			req.MinQueryIndex = resp.Index
		}

		// Push the updates.
		update := &allocUpdates{
			filtered: filtered,
			pulled:   pulledAllocs,
		}
		select {
		case updates <- update:
		case <-c.shutdownCh:
			return
		}
	}
}

// watchNodeUpdates periodically checks for changes to the node attributes or meta map
func (c *Client) watchNodeUpdates() {
	c.logger.Printf("[DEBUG] client: periodically checking for node changes at duration %v", nodeUpdateRetryIntv)

	// Initialize the hashes
	_, attrHash, metaHash := c.hasNodeChanged(0, 0)
	var changed bool
	for {
		select {
		case <-time.After(c.retryIntv(nodeUpdateRetryIntv)):
			changed, attrHash, metaHash = c.hasNodeChanged(attrHash, metaHash)
			if changed {
				c.logger.Printf("[DEBUG] client: state changed, updating node.")

				// Update the config copy.
				c.configLock.Lock()
				node := c.config.Node.Copy()
				c.configCopy.Node = node
				c.configLock.Unlock()

				c.retryRegisterNode()
			}
		case <-c.shutdownCh:
			return
		}
	}
}

// runAllocs is invoked when we get an updated set of allocations
func (c *Client) runAllocs(update *allocUpdates) {
	// Get the existing allocs
	c.allocLock.RLock()
	exist := make([]*models.Allocation, 0, len(c.allocs))
	for _, ar := range c.allocs {
		exist = append(exist, ar.alloc)
	}
	c.allocLock.RUnlock()

	// Diff the existing and updated allocations
	diff := diffAllocs(exist, update)
	c.logger.Printf("[DEBUG] client: %#v", diff)

	// Remove the old allocations
	for _, remove := range diff.removed {
		if err := c.removeAlloc(remove); err != nil {
			c.logger.Printf("[ERR] client: failed to remove alloc '%s': %v",
				remove.ID, err)
		}
	}

	// Update the existing allocations
	for _, update := range diff.updated {
		if update.updated.DesiredStatus == models.AllocDesiredStatusRun {
			c.blockedAllocsLock.Lock()
			ar, ok := c.getAllocRunners()[update.updated.PreviousAllocation]
			if ok && !ar.Alloc().Terminated() {
				// Check if the alloc is already present in the blocked allocations
				// map
				if _, ok := c.blockedAllocations[update.updated.PreviousAllocation]; !ok {
					c.logger.Printf("[DEBUG] client: updated alloc %q to blocked queue for previous allocation %q", update.updated.ID,
						update.updated.PreviousAllocation)
					c.blockedAllocations[update.updated.PreviousAllocation] = update.updated
				}
				c.blockedAllocsLock.Unlock()
				continue
			}
			c.blockedAllocsLock.Unlock()

			// This means the allocation has a previous allocation on another node
			// so we will block for the previous allocation to complete
			if update.updated.PreviousAllocation != "" && !ok {
				// Ensure that we are not blocking for the remote allocation if we
				// have already blocked
				c.migratingAllocsLock.Lock()
				if _, ok := c.migratingAllocs[update.updated.ID]; !ok {
					// Check that we don't have an alloc runner already. This
					// prevents a race between a finishing blockForRemoteAlloc and
					// another invocation of runAllocs
					if _, ok := c.getAllocRunners()[update.updated.PreviousAllocation]; !ok {
						c.migratingAllocs[update.updated.ID] = newMigrateAllocCtrl(update.updated)
						go c.blockForRemoteAlloc(update.updated)
					}
				}
				c.migratingAllocsLock.Unlock()
				continue
			}

			if err := c.resumeAlloc(update.updated); err != nil {
				c.logger.Printf("[ERR] client: failed to resume alloc '%s': %v",
					update.updated.ID, err)
			}
		} else {
			if err := c.updateAlloc(update.exist, update.updated); err != nil {
				c.logger.Printf("[ERR] client: failed to update alloc '%s': %v",
					update.exist.ID, err)
			}

			// See if the updated alloc is getting migrated
			c.migratingAllocsLock.Lock()
			ch, ok := c.migratingAllocs[update.updated.ID]
			c.migratingAllocsLock.Unlock()
			if ok {
				// Stopping the migration if the allocation doesn't need any
				// migration
				if !update.updated.ShouldMigrate() {
					ch.closeCh()
				}
			}
		}
	}

	// Start the new allocations
	for _, add := range diff.added {
		// If the allocation is chained and the previous allocation hasn't
		// terminated yet, then add the alloc to the blocked queue.
		c.blockedAllocsLock.Lock()
		ar, ok := c.getAllocRunners()[add.PreviousAllocation]
		if ok && !ar.Alloc().Terminated() {
			// Check if the alloc is already present in the blocked allocations
			// map
			if _, ok := c.blockedAllocations[add.PreviousAllocation]; !ok {
				c.logger.Printf("[DEBUG] client: added alloc %q to blocked queue for previous allocation %q", add.ID,
					add.PreviousAllocation)
				c.blockedAllocations[add.PreviousAllocation] = add
			}
			c.blockedAllocsLock.Unlock()
			continue
		}
		c.blockedAllocsLock.Unlock()

		// This means the allocation has a previous allocation on another node
		// so we will block for the previous allocation to complete
		if add.PreviousAllocation != "" && !ok {
			// Ensure that we are not blocking for the remote allocation if we
			// have already blocked
			c.migratingAllocsLock.Lock()
			if _, ok := c.migratingAllocs[add.ID]; !ok {
				// Check that we don't have an alloc runner already. This
				// prevents a race between a finishing blockForRemoteAlloc and
				// another invocation of runAllocs
				if _, ok := c.getAllocRunners()[add.PreviousAllocation]; !ok {
					c.migratingAllocs[add.ID] = newMigrateAllocCtrl(add)
					go c.blockForRemoteAlloc(add)
				}
			}
			c.migratingAllocsLock.Unlock()
			continue
		}

		if err := c.addAlloc(add); err != nil {
			c.logger.Printf("[ERR] client: failed to add alloc '%s': %v",
				add.ID, err)
		}
	}

	// Persist our state
	if err := c.saveState(); err != nil {
		c.logger.Printf("[ERR] client: failed to save state: %v", err)
	}
}

// blockForRemoteAlloc blocks until the previous allocation of an allocation has
// been terminated and migrates the snapshot data
func (c *Client) blockForRemoteAlloc(alloc *models.Allocation) {
	// Removing the allocation from the set of allocs which are currently
	// undergoing migration
	defer func() {
		c.migratingAllocsLock.Lock()
		delete(c.migratingAllocs, alloc.ID)
		c.migratingAllocsLock.Unlock()
	}()

	// If the allocation is not sticky then we won't wait for the previous
	// allocation to be terminal
	tg := alloc.Job.LookupTask(alloc.Task)
	if tg == nil {
		c.logger.Printf("[ERR] client: task %q not found in job %q", tg.Type, alloc.Job.ID)
		goto ADDALLOC
	}

ADDALLOC:
	// Add the allocation
	if err := c.addAlloc(alloc); err != nil {
		c.logger.Printf("[ERR] client: error adding alloc: %v", err)
	}
}

// waitForAllocTerminal waits for an allocation with the given alloc id to
// transition to terminal state and blocks the caller until then.
func (c *Client) waitForAllocTerminal(allocID string, stopCh *migrateAllocCtrl) (*models.Allocation, error) {
	req := models.AllocSpecificRequest{
		AllocID: allocID,
		QueryOptions: models.QueryOptions{
			Region:     c.Region(),
			AllowStale: true,
		},
	}

	for {
		resp := models.SingleAllocResponse{}
		err := c.RPC("Alloc.GetAlloc", &req, &resp)
		if err != nil {
			c.logger.Printf("[ERR] client: failed to query allocation %q: %v", allocID, err)
			retry := c.retryIntv(getAllocRetryIntv)
			select {
			case <-time.After(retry):
				continue
			case <-stopCh.ch:
				return nil, fmt.Errorf("giving up waiting on alloc %v since migration is not needed", allocID)
			case <-c.shutdownCh:
				return nil, fmt.Errorf("aborting because client is shutting down")
			}
		}
		if resp.Alloc == nil {
			return nil, nil
		}
		if resp.Alloc.Terminated() {
			return resp.Alloc, nil
		}

		// Update the query index.
		if resp.Index > req.MinQueryIndex {
			req.MinQueryIndex = resp.Index
		}

	}
}

// unarchiveAllocDir reads the stream of a compressed allocation directory and
// writes them to the disk.
func (c *Client) unarchiveAllocDir(resp io.ReadCloser, allocID string, pathToAllocDir string) error {
	tr := tar.NewReader(resp)
	defer resp.Close()

	buf := make([]byte, 1024)

	stopMigrating, ok := c.migratingAllocs[allocID]
	if !ok {
		os.RemoveAll(pathToAllocDir)
		return fmt.Errorf("Allocation %q is not marked for remote migration", allocID)
	}
	for {
		// See if the alloc still needs migration
		select {
		case <-stopMigrating.ch:
			os.RemoveAll(pathToAllocDir)
			c.logger.Printf("[INFO] client: stopping migration of allocdir for alloc: %v", allocID)
			return nil
		case <-c.shutdownCh:
			os.RemoveAll(pathToAllocDir)
			c.logger.Printf("[INFO] client: stopping migration of alloc %q since client is shutting down", allocID)
			return nil
		default:
		}

		// Get the next header
		hdr, err := tr.Next()

		// Snapshot has ended
		if err == io.EOF {
			return nil
		}
		// If there is an error then we avoid creating the alloc dir
		if err != nil {
			os.RemoveAll(pathToAllocDir)
			return fmt.Errorf("error creating alloc dir for alloc %q: %v", allocID, err)
		}

		// If the header is for a directory we create the directory
		if hdr.Typeflag == tar.TypeDir {
			os.MkdirAll(filepath.Join(pathToAllocDir, hdr.Name), os.FileMode(hdr.Mode))
			continue
		}
		// If the header is a file, we write to a file
		if hdr.Typeflag == tar.TypeReg {
			f, err := os.Create(filepath.Join(pathToAllocDir, hdr.Name))
			if err != nil {
				c.logger.Printf("[ERR] client: error creating file: %v", err)
				continue
			}

			// Setting the permissions of the file as the origin.
			if err := f.Chmod(os.FileMode(hdr.Mode)); err != nil {
				f.Close()
				c.logger.Printf("[ERR] client: error chmod-ing file %s: %v", f.Name(), err)
				return fmt.Errorf("error chmoding file %v", err)
			}
			if err := f.Chown(hdr.Uid, hdr.Gid); err != nil {
				f.Close()
				c.logger.Printf("[ERR] client: error chown-ing file %s: %v", f.Name(), err)
				return fmt.Errorf("error chowning file %v", err)
			}

			// We write in chunks of 32 bytes so that we can test if
			// the client is still alive
			for {
				if c.shutdown {
					f.Close()
					os.RemoveAll(pathToAllocDir)
					c.logger.Printf("[INFO] client: stopping migration of alloc %q because client is shutting down", allocID)
					return nil
				}

				n, err := tr.Read(buf)
				if err != nil {
					f.Close()
					if err != io.EOF {
						return fmt.Errorf("error reading snapshot: %v", err)
					}
					break
				}
				if _, err := f.Write(buf[:n]); err != nil {
					f.Close()
					os.RemoveAll(pathToAllocDir)
					return fmt.Errorf("error writing to file %q: %v", f.Name(), err)
				}
			}

		}
	}
}

// getNode gets the node from the server with the given Node ID
func (c *Client) getNode(nodeID string) (*models.Node, error) {
	req := models.NodeSpecificRequest{
		NodeID: nodeID,
		QueryOptions: models.QueryOptions{
			Region:     c.Region(),
			AllowStale: true,
		},
	}

	resp := models.SingleNodeResponse{}
	for {
		err := c.RPC("Node.GetNode", &req, &resp)
		if err != nil {
			c.logger.Printf("[ERR] client: failed to query node info %q: %v", nodeID, err)
			retry := c.retryIntv(getAllocRetryIntv)
			select {
			case <-time.After(retry):
				continue
			case <-c.shutdownCh:
				return nil, fmt.Errorf("aborting because client is shutting down")
			}
		}
		break
	}

	return resp.Node, nil
}

// removeAlloc is invoked when we should remove an allocation
func (c *Client) removeAlloc(alloc *models.Allocation) error {
	c.allocLock.Lock()
	_, ok := c.allocs[alloc.ID]
	if !ok {
		c.allocLock.Unlock()
		c.logger.Printf("[WARN] client: missing context for alloc '%s'", alloc.ID)
		return nil
	}

	delete(c.allocs, alloc.ID)
	c.allocLock.Unlock()

	return nil
}

// updateAlloc is invoked when we should update an allocation
func (c *Client) updateAlloc(exist, update *models.Allocation) error {
	c.allocLock.RLock()
	ar, ok := c.allocs[exist.ID]
	c.allocLock.RUnlock()
	if !ok {
		c.logger.Printf("[WARN] client: missing context for alloc '%s'", exist.ID)
		return nil
	}

	ar.Update(update)
	return nil
}

func (c *Client) resumeAlloc(alloc *models.Allocation) error {
	c.allocLock.Lock()
	ar, ok := c.allocs[alloc.ID]
	ar.alloc = alloc
	c.allocLock.Unlock()
	if !ok {
		c.logger.Printf("[WARN] client: missing context for alloc '%s'", alloc.ID)
		c.allocLock.Unlock()
		return nil
	}
	c.allocLock.Lock()
	defer c.allocLock.Unlock()
	go ar.Run()
	// Store the alloc runner.
	c.allocs[alloc.ID] = ar
	return nil
}

// addAlloc is invoked when we should add an allocation
func (c *Client) addAlloc(alloc *models.Allocation) error {
	// Check if we already have an alloc runner
	c.allocLock.Lock()
	if _, ok := c.allocs[alloc.ID]; ok {
		c.logger.Printf("[DEBUG]: client: dropping duplicate add allocation request: %q", alloc.ID)
		c.allocLock.Unlock()
		return nil
	}
	c.allocLock.Unlock()

	c.allocLock.Lock()
	defer c.allocLock.Unlock()

	c.configLock.RLock()
	ar := NewAllocator(c.logger, c.configCopy, c.updateAllocStatus, alloc,c.workUpdates)
	c.configLock.RUnlock()
	go ar.Run()

	// Store the alloc runner.
	c.allocs[alloc.ID] = ar
	return nil
}

// triggerDiscovery causes a Consul discovery to begin (if one hasn't alread)
func (c *Client) triggerDiscovery() {
	select {
	case c.triggerDiscoveryCh <- struct{}{}:
		// Discovery goroutine was released to execute
	default:
		// Discovery goroutine was already running
	}
}

// emitClientMetrics emits lower volume client metrics
func (c *Client) emitClientMetrics() {
	nodeID := c.Node().ID

	// Emit allocation metrics
	c.migratingAllocsLock.Lock()
	migrating := len(c.migratingAllocs)
	c.migratingAllocsLock.Unlock()

	c.blockedAllocsLock.Lock()
	blocked := len(c.blockedAllocations)
	c.blockedAllocsLock.Unlock()

	pending, running, terminal := 0, 0, 0
	for _, ar := range c.getAllocRunners() {
		switch ar.Alloc().ClientStatus {
		case models.AllocClientStatusPending:
			pending++
		case models.AllocClientStatusRunning:
			running++
		case models.AllocClientStatusComplete, models.AllocClientStatusFailed:
			terminal++
		}
	}

	metrics.SetGauge([]string{"client", "allocations", "migrating", nodeID}, float32(migrating))
	metrics.SetGauge([]string{"client", "allocations", "blocked", nodeID}, float32(blocked))
	metrics.SetGauge([]string{"client", "allocations", "pending", nodeID}, float32(pending))
	metrics.SetGauge([]string{"client", "allocations", "running", nodeID}, float32(running))
	metrics.SetGauge([]string{"client", "allocations", "terminal", nodeID}, float32(terminal))
}

// allAllocs returns all the allocations managed by the client
func (c *Client) allAllocs() map[string]*models.Allocation {
	allocs := make(map[string]*models.Allocation, 16)
	for _, ar := range c.getAllocRunners() {
		a := ar.Alloc()
		allocs[a.ID] = a
	}
	c.blockedAllocsLock.Lock()
	for _, alloc := range c.blockedAllocations {
		allocs[alloc.ID] = alloc
	}
	c.blockedAllocsLock.Unlock()

	c.migratingAllocsLock.Lock()
	for _, ctrl := range c.migratingAllocs {
		allocs[ctrl.alloc.ID] = ctrl.alloc
	}
	c.migratingAllocsLock.Unlock()
	return allocs
}

// resolveServer given a sever's address as a string, return it's resolved
// net.Addr or an error.
func resolveServer(s string) (net.Addr, error) {
	const defaultClientPort = "8191" // default client RPC port
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		if strings.Contains(err.Error(), "missing port") {
			host = s
			port = defaultClientPort
		} else {
			return nil, err
		}
	}
	return net.ResolveTCPAddr("tcp", net.JoinHostPort(host, port))
}

// serverlist is a prioritized randomized list of server servers. Users should
// call all() to retrieve the full list, followed by failed(e) on each endpoint
// that's failed and good(e) when a valid endpoint is found.
type serverlist struct {
	e  endpoints
	mu sync.RWMutex
}

func newServerList() *serverlist {
	return &serverlist{}
}

// set the server list to a new list. The new list will be shuffled and sorted
// by priority.
func (s *serverlist) set(in endpoints) {
	s.mu.Lock()
	s.e = in
	s.mu.Unlock()
}

// all returns a copy of the full server list, shuffled and then sorted by
// priority
func (s *serverlist) all() endpoints {
	s.mu.RLock()
	out := make(endpoints, len(s.e))
	copy(out, s.e)
	s.mu.RUnlock()

	// Randomize the order
	for i, j := range rand.Perm(len(out)) {
		out[i], out[j] = out[j], out[i]
	}

	// Sort by priority
	sort.Sort(out)
	return out
}

// failed endpoint will be deprioritized if its still in the list.
func (s *serverlist) failed(e *endpoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cur := range s.e {
		if cur.equal(e) {
			cur.priority++
			return
		}
	}
}

// good endpoint will get promoted to the highest priority if it's still in the
// list.
func (s *serverlist) good(e *endpoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cur := range s.e {
		if cur.equal(e) {
			cur.priority = 0
			return
		}
	}
}

func (e endpoints) Len() int {
	return len(e)
}

func (e endpoints) Less(i int, j int) bool {
	// Sort only by priority as endpoints should be shuffled and ordered
	// only by priority
	return e[i].priority < e[j].priority
}

func (e endpoints) Swap(i int, j int) {
	e[i], e[j] = e[j], e[i]
}

type endpoints []*endpoint

func (e endpoints) String() string {
	names := make([]string, 0, len(e))
	for _, endpoint := range e {
		names = append(names, endpoint.name)
	}
	return strings.Join(names, ",")
}

type endpoint struct {
	name string
	addr net.Addr

	// 0 being the highest priority
	priority int
}

// equal returns true if the name and addr match between two endpoints.
// Priority is ignored because the same endpoint may be added by discovery and
// heartbeating with different priorities.
func (e *endpoint) equal(o *endpoint) bool {
	return e.name == o.name && e.addr == o.addr
}
