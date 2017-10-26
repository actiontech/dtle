package server

import (
	"fmt"
	"runtime"
	"time"

	"github.com/armon/go-metrics"
	memdb "github.com/hashicorp/go-memdb"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/raft"

	"udup/internal/models"
	"udup/internal/server/store"
)

func (s *Server) planApply() {
	// waitCh is used to track an outstanding application while snap
	// holds an optimistic store which includes that plan application.
	var waitCh chan struct{}
	var snap *store.StateSnapshot

	// Setup a worker pool with half the cores, with at least 1
	poolSize := runtime.NumCPU() / 2
	if poolSize == 0 {
		poolSize = 1
	}
	pool := NewEvaluatePool(poolSize, workerPoolBufferSize)
	defer pool.Shutdown()

	for {
		// Pull the next pending plan, exit if we are no longer leader
		pending, err := s.planQueue.Dequeue(0)
		if err != nil {
			return
		}

		// Check if out last plan has completed
		select {
		case <-waitCh:
			waitCh = nil
			snap = nil
		default:
		}

		// Snapshot the store so that we have a consistent view of the world
		// if no snapshot is available
		if waitCh == nil || snap == nil {
			snap, err = s.fsm.State().Snapshot()
			if err != nil {
				s.logger.Errorf("manager: failed to snapshot store: %v", err)
				pending.respond(nil, err)
				continue
			}
		}

		// Evaluate the plan
		result, err := evaluatePlan(pool, snap, pending.plan)
		if err != nil {
			s.logger.Errorf("manager: failed to evaluate plan: %v", err)
			pending.respond(nil, err)
			continue
		}

		// Fast-path the response if there is nothing to do
		if result.IsNoOp() {
			pending.respond(result, nil)
			continue
		}

		// Ensure any parallel apply is complete before starting the next one.
		// This also limits how out of date our snapshot can be.
		if waitCh != nil {
			<-waitCh
			snap, err = s.fsm.State().Snapshot()
			if err != nil {
				s.logger.Errorf("manager: failed to snapshot store: %v", err)
				pending.respond(nil, err)
				continue
			}
		}

		// Dispatch the Raft transaction for the plan
		future, err := s.applyPlan(pending.plan.Job, result, snap)
		if err != nil {
			s.logger.Errorf("manager: failed to submit plan: %v", err)
			pending.respond(nil, err)
			continue
		}

		// Respond to the plan in async
		waitCh = make(chan struct{})
		go s.asyncPlanWait(waitCh, future, result, pending)
	}
}

// applyPlan is used to apply the plan result and to return the alloc index
func (s *Server) applyPlan(job *models.Job, result *models.PlanResult, snap *store.StateSnapshot) (raft.ApplyFuture, error) {
	// Determine the miniumum number of updates, could be more if there
	// are multiple updates per node
	minUpdates := len(result.NodeUpdate)
	minUpdates += len(result.NodeAllocation)

	// Setup the update request
	req := models.AllocUpdateRequest{
		Job:   job,
		Alloc: make([]*models.Allocation, 0, minUpdates),
	}
	for _, updateList := range result.NodeUpdate {
		req.Alloc = append(req.Alloc, updateList...)
	}
	for _, allocList := range result.NodeAllocation {
		req.Alloc = append(req.Alloc, allocList...)
	}

	// Set the time the alloc was applied for the first time. This can be used
	// to approximate the scheduling time.
	now := time.Now().UTC().UnixNano()
	for _, alloc := range req.Alloc {
		if alloc.CreateTime == 0 {
			alloc.CreateTime = now
		}
	}

	// Dispatch the Raft transaction
	future, err := s.raftApplyFuture(models.AllocUpdateRequestType, &req)
	if err != nil {
		return nil, err
	}

	// Optimistically apply to our store view
	if snap != nil {
		nextIdx := s.raft.AppliedIndex() + 1
		if err := snap.UpsertAllocs(nextIdx, req.Alloc); err != nil {
			return future, err
		}
	}
	return future, nil
}

// asyncPlanWait is used to apply and respond to a plan async
func (s *Server) asyncPlanWait(waitCh chan struct{}, future raft.ApplyFuture,
	result *models.PlanResult, pending *pendingPlan) {
	defer metrics.MeasureSince([]string{"server", "plan", "apply"}, time.Now())
	defer close(waitCh)

	// Wait for the plan to apply
	if err := future.Error(); err != nil {
		s.logger.Errorf("manager: failed to apply plan: %v", err)
		pending.respond(nil, err)
		return
	}

	// Respond to the plan
	result.AllocIndex = future.Index()

	// If this is a partial plan application, we need to ensure the scheduler
	// at least has visibility into any placements it made to avoid double placement.
	// The RefreshIndex computed by evaluatePlan may be stale due to evaluation
	// against an optimistic copy of the store.
	if result.RefreshIndex != 0 {
		result.RefreshIndex = maxUint64(result.RefreshIndex, result.AllocIndex)
	}
	pending.respond(result, nil)
}

// evaluatePlan is used to determine what portions of a plan
// can be applied if any. Returns if there should be a plan application
// which may be partial or if there was an error
func evaluatePlan(pool *EvaluatePool, snap *store.StateSnapshot, plan *models.Plan) (*models.PlanResult, error) {
	defer metrics.MeasureSince([]string{"server", "plan", "evaluate"}, time.Now())

	// Create a result holder for the plan
	result := &models.PlanResult{
		NodeUpdate:     make(map[string][]*models.Allocation),
		NodeAllocation: make(map[string][]*models.Allocation),
	}

	// Collect all the nodeIDs
	nodeIDs := make(map[string]struct{})
	nodeIDList := make([]string, 0, len(plan.NodeUpdate)+len(plan.NodeAllocation))
	for nodeID := range plan.NodeUpdate {
		if _, ok := nodeIDs[nodeID]; !ok {
			nodeIDs[nodeID] = struct{}{}
			nodeIDList = append(nodeIDList, nodeID)
		}
	}
	for nodeID := range plan.NodeAllocation {
		if _, ok := nodeIDs[nodeID]; !ok {
			nodeIDs[nodeID] = struct{}{}
			nodeIDList = append(nodeIDList, nodeID)
		}
	}

	// Setup a multierror to handle potentially getting many
	// errors since we are processing in parallel.
	var mErr multierror.Error
	partialCommit := false

	// handleResult is used to process the result of evaluateNodePlan
	handleResult := func(nodeID string, fit bool, err error) (cancel bool) {
		// Evaluate the plan for this node
		if err != nil {
			mErr.Errors = append(mErr.Errors, err)
			return true
		}
		if !fit {
			// Set that this is a partial commit
			partialCommit = true

			// Skip this node, since it cannot be used.
			return
		}

		// Add this to the plan result
		if nodeUpdate := plan.NodeUpdate[nodeID]; len(nodeUpdate) > 0 {
			result.NodeUpdate[nodeID] = nodeUpdate
		}
		if nodeAlloc := plan.NodeAllocation[nodeID]; len(nodeAlloc) > 0 {
			result.NodeAllocation[nodeID] = nodeAlloc
		}
		return
	}

	// Get the pool channels
	req := pool.RequestCh()
	resp := pool.ResultCh()
	outstanding := 0
	didCancel := false

	// Evalute each node in the plan, handling results as they are ready to
	// avoid blocking.
	for len(nodeIDList) > 0 {
		nodeID := nodeIDList[0]
		select {
		case req <- evaluateRequest{snap, plan, nodeID}:
			outstanding++
			nodeIDList = nodeIDList[1:]
		case r := <-resp:
			outstanding--

			// Handle a result that allows us to cancel evaluation,
			// which may save time processing additional entries.
			if cancel := handleResult(r.nodeID, r.fit, r.err); cancel {
				didCancel = true
				break
			}
		}
	}

	// Drain the remaining results
	for outstanding > 0 {
		r := <-resp
		if !didCancel {
			if cancel := handleResult(r.nodeID, r.fit, r.err); cancel {
				didCancel = true
			}
		}
		outstanding--
	}

	// If the plan resulted in a partial commit, we need to determine
	// a minimum refresh index to force the scheduler to work on a more
	// up-to-date store to avoid the failures.
	if partialCommit {
		allocIndex, err := snap.Index("allocs")
		if err != nil {
			mErr.Errors = append(mErr.Errors, err)
		}
		nodeIndex, err := snap.Index("nodes")
		if err != nil {
			mErr.Errors = append(mErr.Errors, err)
		}
		result.RefreshIndex = maxUint64(nodeIndex, allocIndex)

		if result.RefreshIndex == 0 {
			err := fmt.Errorf("partialCommit with RefreshIndex of 0 (%d node, %d alloc)", nodeIndex, allocIndex)
			mErr.Errors = append(mErr.Errors, err)
		}
	}
	return result, mErr.ErrorOrNil()
}

// evaluateNodePlan is used to evalute the plan for a single node,
// returning if the plan is valid or if an error is encountered
func evaluateNodePlan(snap *store.StateSnapshot, plan *models.Plan, nodeID string) (bool, error) {
	// If this is an evict-only plan, it always 'fits' since we are removing things.
	if len(plan.NodeAllocation[nodeID]) == 0 {
		return true, nil
	}

	// Get the node itself
	ws := memdb.NewWatchSet()
	node, err := snap.NodeByID(ws, nodeID)
	if err != nil {
		return false, fmt.Errorf("failed to get node '%s': %v", nodeID, err)
	}

	// If the node does not exist or is not ready for schduling it is not fit
	// XXX: There is a potential race between when we do this check and when
	// the Raft commit happens.
	if node == nil || node.Status != models.NodeStatusReady {
		return false, nil
	}

	// Get the existing allocations that are non-terminal
	existingAlloc, err := snap.AllocsByNodeTerminal(ws, nodeID, false)
	if err != nil {
		return false, fmt.Errorf("failed to get existing allocations for '%s': %v", nodeID, err)
	}

	// Determine the proposed allocation by first removing allocations
	// that are planned evictions and adding the new allocations.
	proposed := existingAlloc
	var remove []*models.Allocation
	if update := plan.NodeUpdate[nodeID]; len(update) > 0 {
		remove = append(remove, update...)
	}
	if updated := plan.NodeAllocation[nodeID]; len(updated) > 0 {
		for _, alloc := range updated {
			remove = append(remove, alloc)
		}
	}
	proposed = models.RemoveAllocs(existingAlloc, remove)
	proposed = append(proposed, plan.NodeAllocation[nodeID]...)

	return true, err
}
