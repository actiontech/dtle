package server

import (
	"fmt"
	"sync"
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-memdb"
	"github.com/hashicorp/go-multierror"

	"udup/internal/models"
	"udup/internal/server/store"
)

const (
	// batchUpdateInterval is how long we wait to batch updates
	batchUpdateInterval = 50 * time.Millisecond

	// maxParallelRequestsPerDerive  is the maximum number of parallel Vault
	// create token requests that may be outstanding per derive request
	maxParallelRequestsPerDerive = 16
)

// Node endpoint is used for client interactions
type Node struct {
	srv *Server

	// updates holds pending client status updates for allocations
	updates []*models.Allocation

	// updateFuture is used to wait for the pending batch update
	// to complete. This may be nil if no batch is pending.
	updateFuture *batchFuture

	// updateTimer is the timer that will trigger the next batch
	// update, and may be nil if there is no batch pending.
	updateTimer *time.Timer

	// updatesLock synchronizes access to the updates list,
	// the future and the timer.
	updatesLock sync.Mutex
}

// Register is used to upsert a client that is available for scheduling
func (n *Node) Register(args *models.NodeRegisterRequest, reply *models.NodeUpdateResponse) error {
	if done, err := n.srv.forward("Node.Register", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "client", "register"}, time.Now())

	// Validate the arguments
	if args.Node == nil {
		return fmt.Errorf("missing node for client registration")
	}
	if args.Node.ID == "" {
		return fmt.Errorf("missing node ID for client registration")
	}
	if args.Node.Datacenter == "" {
		return fmt.Errorf("missing datacenter for client registration")
	}
	if args.Node.Name == "" {
		return fmt.Errorf("missing node name for client registration")
	}

	// Default the status if none is given
	if args.Node.Status == "" {
		args.Node.Status = models.NodeStatusInit
	}
	if !models.ValidNodeStatus(args.Node.Status) {
		return fmt.Errorf("invalid status for node")
	}

	// Set the timestamp when the node is registered
	args.Node.StatusUpdatedAt = time.Now().Unix()

	// Look for the node so we can detect a store transistion
	snap, err := n.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}

	ws := memdb.NewWatchSet()
	originalNode, err := snap.NodeByID(ws, args.Node.ID)
	if err != nil {
		return err
	}

	// Commit this update via Raft
	_, index, err := n.srv.raftApply(models.NodeRegisterRequestType, args)
	if err != nil {
		n.srv.logger.Errorf("server.client: Register failed: %v", err)
		return err
	}
	reply.NodeModifyIndex = index

	// Check if we should trigger evaluations
	originalStatus := models.NodeStatusInit
	if originalNode != nil {
		originalStatus = originalNode.Status
	}
	transitionToReady := transitionedToReady(args.Node.Status, originalStatus)
	if transitionToReady {
		evalIDs, evalIndex, err := n.createNodeEvals(args.Node.ID, index)
		if err != nil {
			n.srv.logger.Errorf("server.client: eval creation failed: %v", err)
			return err
		}
		reply.EvalIDs = evalIDs
		reply.EvalCreateIndex = evalIndex
	}

	// Check if we need to setup a heartbeat
	if !args.Node.TerminalStatus() {
		ttl, err := n.srv.resetHeartbeatTimer(args.Node.ID)
		if err != nil {
			n.srv.logger.Errorf("server.client: heartbeat reset failed: %v", err)
			return err
		}
		reply.HeartbeatTTL = ttl
	}

	// Set the reply index
	reply.Index = index
	snap, err = n.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}

	n.srv.peerLock.RLock()
	defer n.srv.peerLock.RUnlock()
	if err := n.constructNodeServerInfoResponse(snap, reply); err != nil {
		n.srv.logger.Errorf("server.client: failed to populate NodeUpdateResponse: %v", err)
		return err
	}

	return nil
}

// updateNodeUpdateResponse assumes the n.srv.peerLock is held for reading.
func (n *Node) constructNodeServerInfoResponse(snap *store.StateSnapshot, reply *models.NodeUpdateResponse) error {
	reply.LeaderRPCAddr = string(n.srv.raft.Leader())

	// Reply with config information required for future RPC requests
	reply.Servers = make([]*models.NodeServerInfo, 0, len(n.srv.localPeers))
	for k, v := range n.srv.localPeers {
		reply.Servers = append(reply.Servers,
			&models.NodeServerInfo{
				RPCAdvertiseAddr: string(k),
				Datacenter:       v.Datacenter,
			})
	}

	// TODO(sean@): Use an indexed node count instead
	//
	// Snapshot is used only to iterate over all nodes to create a node
	// count to send back to Udup Clients in their heartbeat so Clients
	// can estimate the size of the cluster.
	ws := memdb.NewWatchSet()
	iter, err := snap.Nodes(ws)
	if err == nil {
		for {
			raw := iter.Next()
			if raw == nil {
				break
			}
			reply.NumNodes++
		}
	}

	return nil
}

// Deregister is used to remove a client from the cluster. If a client should
// just be made unavailable for scheduling, a status update is preferred.
func (n *Node) Deregister(args *models.NodeDeregisterRequest, reply *models.NodeUpdateResponse) error {
	if done, err := n.srv.forward("Node.Deregister", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "client", "deregister"}, time.Now())

	// Verify the arguments
	if args.NodeID == "" {
		return fmt.Errorf("missing node ID for client deregistration")
	}

	// Commit this update via Raft
	_, index, err := n.srv.raftApply(models.NodeDeregisterRequestType, args)
	if err != nil {
		n.srv.logger.Errorf("server.client: Deregister failed: %v", err)
		return err
	}

	// Clear the heartbeat timer if any
	n.srv.clearHeartbeatTimer(args.NodeID)

	// Create the evaluations for this node
	evalIDs, evalIndex, err := n.createNodeEvals(args.NodeID, index)
	if err != nil {
		n.srv.logger.Errorf("server.client: eval creation failed: %v", err)
		return err
	}

	// Setup the reply
	reply.EvalIDs = evalIDs
	reply.EvalCreateIndex = evalIndex
	reply.NodeModifyIndex = index
	reply.Index = index
	return nil
}

// UpdateStatus is used to update the status of a client node
func (n *Node) UpdateStatus(args *models.NodeUpdateStatusRequest, reply *models.NodeUpdateResponse) error {
	if done, err := n.srv.forward("Node.UpdateStatus", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "client", "update_status"}, time.Now())

	// Verify the arguments
	if args.NodeID == "" {
		return fmt.Errorf("missing node ID for client status update")
	}
	if !models.ValidNodeStatus(args.Status) {
		return fmt.Errorf("invalid status for node")
	}

	// Look for the node
	snap, err := n.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}

	ws := memdb.NewWatchSet()
	node, err := snap.NodeByID(ws, args.NodeID)
	if err != nil {
		return err
	}
	if node == nil {
		return fmt.Errorf("node not found")
	}

	// Update the timestamp of when the node status was updated
	node.StatusUpdatedAt = time.Now().Unix()

	// Commit this update via Raft
	var index uint64
	if node.Status != args.Status {
		_, index, err = n.srv.raftApply(models.NodeUpdateStatusRequestType, args)
		if err != nil {
			n.srv.logger.Errorf("server.client: status update failed: %v", err)
			return err
		}
		reply.NodeModifyIndex = index
	}

	// Check if we should trigger evaluations
	transitionToReady := transitionedToReady(args.Status, node.Status)
	if transitionToReady {
		evalIDs, evalIndex, err := n.createNodeEvals(args.NodeID, index)
		if err != nil {
			n.srv.logger.Errorf("server.client: eval creation failed: %v", err)
			return err
		}
		reply.EvalIDs = evalIDs
		reply.EvalCreateIndex = evalIndex
	}

	// Check if we need to setup a heartbeat
	switch args.Status {
	case models.NodeStatusDown:

	default:
		ttl, err := n.srv.resetHeartbeatTimer(args.NodeID)
		if err != nil {
			n.srv.logger.Errorf("server.client: heartbeat reset failed: %v", err)
			return err
		}
		reply.HeartbeatTTL = ttl
	}

	// Set the reply index and leader
	reply.Index = index
	n.srv.peerLock.RLock()
	defer n.srv.peerLock.RUnlock()
	if err := n.constructNodeServerInfoResponse(snap, reply); err != nil {
		n.srv.logger.Errorf("server.client: failed to populate NodeUpdateResponse: %v", err)
		return err
	}

	return nil
}

// transitionedToReady is a helper that takes a nodes new and old status and
// returns whether it has transistioned to ready.
func transitionedToReady(newStatus, oldStatus string) bool {
	initToReady := oldStatus == models.NodeStatusInit && newStatus == models.NodeStatusReady
	terminalToReady := oldStatus == models.NodeStatusDown && newStatus == models.NodeStatusReady
	return initToReady || terminalToReady
}

// Evaluate is used to force a re-evaluation of the node
func (n *Node) Evaluate(args *models.NodeEvaluateRequest, reply *models.NodeUpdateResponse) error {
	if done, err := n.srv.forward("Node.Evaluate", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "client", "evaluate"}, time.Now())

	// Verify the arguments
	if args.NodeID == "" {
		return fmt.Errorf("missing node ID for evaluation")
	}

	// Look for the node
	snap, err := n.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}
	ws := memdb.NewWatchSet()
	node, err := snap.NodeByID(ws, args.NodeID)
	if err != nil {
		return err
	}
	if node == nil {
		return fmt.Errorf("node not found")
	}

	// Create the evaluation
	evalIDs, evalIndex, err := n.createNodeEvals(args.NodeID, node.ModifyIndex)
	if err != nil {
		n.srv.logger.Errorf("server.client: eval creation failed: %v", err)
		return err
	}
	reply.EvalIDs = evalIDs
	reply.EvalCreateIndex = evalIndex

	// Set the reply index
	reply.Index = evalIndex

	n.srv.peerLock.RLock()
	defer n.srv.peerLock.RUnlock()
	if err := n.constructNodeServerInfoResponse(snap, reply); err != nil {
		n.srv.logger.Errorf("server.client: failed to populate NodeUpdateResponse: %v", err)
		return err
	}
	return nil
}

// GetNode is used to request information about a specific node
func (n *Node) GetNode(args *models.NodeSpecificRequest,
	reply *models.SingleNodeResponse) error {
	if done, err := n.srv.forward("Node.GetNode", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "client", "get_node"}, time.Now())

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Verify the arguments
			if args.NodeID == "" {
				return fmt.Errorf("missing node ID")
			}

			// Look for the node
			out, err := state.NodeByID(ws, args.NodeID)
			if err != nil {
				return err
			}

			// Setup the output
			if out != nil {
				reply.Node = out.Copy()
				reply.Index = out.ModifyIndex
			} else {
				// Use the last index that affected the nodes table
				index, err := state.Index("nodes")
				if err != nil {
					return err
				}
				reply.Node = nil
				reply.Index = index
			}

			// Set the query response
			n.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return n.srv.blockingRPC(&opts)
}

// GetAllocs is used to request allocations for a specific node
func (n *Node) GetAllocs(args *models.NodeSpecificRequest,
	reply *models.NodeAllocsResponse) error {
	if done, err := n.srv.forward("Node.GetAllocs", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "client", "get_allocs"}, time.Now())

	// Verify the arguments
	if args.NodeID == "" {
		return fmt.Errorf("missing node ID")
	}

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Look for the node
			allocs, err := state.AllocsByNode(ws, args.NodeID)
			if err != nil {
				return err
			}

			// Setup the output
			if len(allocs) != 0 {
				reply.Allocs = allocs
				for _, alloc := range allocs {
					reply.Index = maxUint64(reply.Index, alloc.ModifyIndex)
				}
			} else {
				reply.Allocs = nil

				// Use the last index that affected the nodes table
				index, err := state.Index("allocs")
				if err != nil {
					return err
				}

				// Must provide non-zero index to prevent blocking
				// Index 1 is impossible anyways (due to Raft internals)
				if index == 0 {
					reply.Index = 1
				} else {
					reply.Index = index
				}
			}
			return nil
		}}
	return n.srv.blockingRPC(&opts)
}

// GetClientAllocs is used to request a lightweight list of alloc modify indexes
// per allocation.
func (n *Node) GetClientAllocs(args *models.NodeSpecificRequest,
	reply *models.NodeClientAllocsResponse) error {
	if done, err := n.srv.forward("Node.GetClientAllocs", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "client", "get_client_allocs"}, time.Now())

	// Verify the arguments
	if args.NodeID == "" {
		return fmt.Errorf("missing node ID")
	}

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Look for the node
			node, err := state.NodeByID(ws, args.NodeID)
			if err != nil {
				return err
			}

			var allocs []*models.Allocation
			if node != nil {
				var err error
				allocs, err = state.AllocsByNode(ws, args.NodeID)
				if err != nil {
					return err
				}
			}

			reply.Allocs = make(map[string]uint64)
			// Setup the output
			if len(allocs) != 0 {
				for _, alloc := range allocs {
					reply.Allocs[alloc.ID] = alloc.AllocModifyIndex
					reply.Index = maxUint64(reply.Index, alloc.ModifyIndex)
				}
			} else {
				// Use the last index that affected the nodes table
				index, err := state.Index("allocs")
				if err != nil {
					return err
				}

				// Must provide non-zero index to prevent blocking
				// Index 1 is impossible anyways (due to Raft internals)
				if index == 0 {
					reply.Index = 1
				} else {
					reply.Index = index
				}
			}
			return nil
		}}
	return n.srv.blockingRPC(&opts)
}

func (n *Node) UpdateJob(args *models.JobUpdateRequest, reply *models.GenericResponse) error {
	if done, err := n.srv.forward("Node.UpdateJob", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "client", "update_job"}, time.Now())

	// Ensure at least a single job
	if len(args.JobUpdates) == 0 {
		return fmt.Errorf("must update at least one job")
	}

	_, index, err := n.srv.raftApply(models.JobClientUpdateRequestType, args)
	if err != nil {
		n.srv.logger.Errorf("server.job: client update failed: %v", err)
		return err
	}

	// Setup the response
	reply.Index = index
	return nil
}

// UpdateAlloc is used to update the client status of an allocation
func (n *Node) UpdateAlloc(args *models.AllocUpdateRequest, reply *models.GenericResponse) error {
	if done, err := n.srv.forward("Node.UpdateAlloc", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "client", "update_alloc"}, time.Now())

	// Ensure at least a single alloc
	if len(args.Alloc) == 0 {
		return fmt.Errorf("must update at least one allocation")
	}

	// Add this to the batch
	n.updatesLock.Lock()
	n.updates = append(n.updates, args.Alloc...)

	// Start a new batch if none
	future := n.updateFuture
	if future == nil {
		future = NewBatchFuture()
		n.updateFuture = future
		n.updateTimer = time.AfterFunc(batchUpdateInterval, func() {
			// Get the pending updates
			n.updatesLock.Lock()
			updates := n.updates
			future := n.updateFuture
			n.updates = nil
			n.updateFuture = nil
			n.updateTimer = nil
			n.updatesLock.Unlock()

			// Perform the batch update
			n.batchUpdate(future, updates)
		})
	}
	n.updatesLock.Unlock()

	// Wait for the future
	if err := future.Wait(); err != nil {
		return err
	}

	// Setup the response
	reply.Index = future.Index()
	return nil
}

// batchUpdate is used to update all the allocations
func (n *Node) batchUpdate(future *batchFuture, updates []*models.Allocation) {
	// Prepare the batch update
	batch := &models.AllocUpdateRequest{
		Alloc:        updates,
		WriteRequest: models.WriteRequest{Region: n.srv.config.Region},
	}

	// Commit this update via Raft
	var mErr multierror.Error
	_, index, err := n.srv.raftApply(models.AllocClientUpdateRequestType, batch)
	if err != nil {
		n.srv.logger.Errorf("server.client: alloc update failed: %v", err)
		mErr.Errors = append(mErr.Errors, err)
	}

	// Respond to the future
	future.Respond(index, mErr.ErrorOrNil())
}

// List is used to list the available nodes
func (n *Node) List(args *models.NodeListRequest,
	reply *models.NodeListResponse) error {
	if done, err := n.srv.forward("Node.List", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "client", "list"}, time.Now())

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Capture all the nodes
			var err error
			var iter memdb.ResultIterator
			if prefix := args.QueryOptions.Prefix; prefix != "" {
				iter, err = state.NodesByIDPrefix(ws, prefix)
			} else {
				iter, err = state.Nodes(ws)
			}
			if err != nil {
				return err
			}

			var nodes []*models.NodeListStub
			for {
				raw := iter.Next()
				if raw == nil {
					break
				}
				node := raw.(*models.Node)
				nodes = append(nodes, node.Stub())
			}
			reply.Nodes = nodes

			// Use the last index that affected the jobs table
			index, err := state.Index("nodes")
			if err != nil {
				return err
			}
			reply.Index = index

			// Set the query response
			n.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return n.srv.blockingRPC(&opts)
}

// createNodeEvals is used to create evaluations for each alloc on a node.
// Each Eval is scoped to a job, so we need to potentially trigger many evals.
func (n *Node) createNodeEvals(nodeID string, nodeIndex uint64) ([]string, uint64, error) {
	// Snapshot the store
	snap, err := n.srv.fsm.State().Snapshot()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to snapshot store: %v", err)
	}

	// Find all the allocations for this node
	ws := memdb.NewWatchSet()
	allocs, err := snap.AllocsByNode(ws, nodeID)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to find allocs for '%s': %v", nodeID, err)
	}

	sysJobsIter, err := snap.JobsByScheduler(ws, "synchronous")
	if err != nil {
		return nil, 0, fmt.Errorf("failed to find synchronous jobs for '%s': %v", nodeID, err)
	}

	var sysJobs []*models.Job
	for job := sysJobsIter.Next(); job != nil; job = sysJobsIter.Next() {
		sysJobs = append(sysJobs, job.(*models.Job))
	}

	// Fast-path if nothing to do
	if len(allocs) == 0 && len(sysJobs) == 0 {
		return nil, 0, nil
	}

	// Create an eval for each JobID affected
	var evals []*models.Evaluation
	var evalIDs []string
	jobIDs := make(map[string]struct{})

	for _, alloc := range allocs {
		// Deduplicate on JobID
		if _, ok := jobIDs[alloc.JobID]; ok {
			continue
		}
		jobIDs[alloc.JobID] = struct{}{}

		// Create a new eval
		eval := &models.Evaluation{
			ID:              models.GenerateUUID(),
			Type:            alloc.Job.Type,
			TriggeredBy:     models.EvalTriggerNodeUpdate,
			JobID:           alloc.JobID,
			NodeID:          nodeID,
			NodeModifyIndex: nodeIndex,
			Status:          models.EvalStatusPending,
		}
		evals = append(evals, eval)
		evalIDs = append(evalIDs, eval.ID)
	}

	// Create an evaluation for each system job.
	for _, job := range sysJobs {
		// Still dedup on JobID as the node may already have the system job.
		if _, ok := jobIDs[job.ID]; ok {
			continue
		}
		jobIDs[job.ID] = struct{}{}

		// Create a new eval
		eval := &models.Evaluation{
			ID:              models.GenerateUUID(),
			Type:            job.Type,
			TriggeredBy:     models.EvalTriggerNodeUpdate,
			JobID:           job.ID,
			NodeID:          nodeID,
			NodeModifyIndex: nodeIndex,
			Status:          models.EvalStatusPending,
		}
		evals = append(evals, eval)
		evalIDs = append(evalIDs, eval.ID)
	}

	// Create the Raft transaction
	update := &models.EvalUpdateRequest{
		Evals:        evals,
		WriteRequest: models.WriteRequest{Region: n.srv.config.Region},
	}

	// Commit this evaluation via Raft
	// XXX: There is a risk of partial failure where the node update succeeds
	// but that the EvalUpdate does not.
	_, evalIndex, err := n.srv.raftApply(models.EvalUpdateRequestType, update)
	if err != nil {
		return nil, 0, err
	}
	return evalIDs, evalIndex, nil
}

// batchFuture is used to wait on a batch update to complete
type batchFuture struct {
	doneCh chan struct{}
	err    error
	index  uint64
}

// NewBatchFuture creates a new batch future
func NewBatchFuture() *batchFuture {
	return &batchFuture{
		doneCh: make(chan struct{}),
	}
}

// Wait is used to block for the future to complete and returns the error
func (b *batchFuture) Wait() error {
	<-b.doneCh
	return b.err
}

// Index is used to return the index of the batch, only after Wait()
func (b *batchFuture) Index() uint64 {
	return b.index
}

// Respond is used to unblock the future
func (b *batchFuture) Respond(index uint64, err error) {
	b.index = index
	b.err = err
	close(b.doneCh)
}
