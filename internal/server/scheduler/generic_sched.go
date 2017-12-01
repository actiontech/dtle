package scheduler

import (
	"fmt"
	//"math/rand"

	memdb "github.com/hashicorp/go-memdb"
	"github.com/hashicorp/go-multierror"

	log "udup/internal/logger"
	"udup/internal/models"
)

const (
	// maxSyncScheduleAttempts is used to limit the number of times
	// we will attempt to schedule if we continue to hit conflicts for services.
	maxScheduleAttempts = 5

	// allocNotNeeded is the status used when a job no longer requires an allocation
	allocNotNeeded = "alloc not needed due to job update"

	// allocLost is the status used when an allocation is lost
	allocLost = "alloc is lost since its node is down"

	// allocInPlace is the status used when speculating on an in-place update
	allocInPlace = "alloc updating in-place"

	// blockedEvalMaxPlanDesc is the description used for blocked evals that are
	// a result of hitting the max number of plan attempts
	blockedEvalMaxPlanDesc = "created due to placement conflicts"

	// blockedEvalFailedPlacements is the description used for blocked evals
	// that are a result of failing to place all allocations.
	blockedEvalFailedPlacements = "created to place remaining allocations"
)

// SetStatusError is used to set the status of the evaluation to the given error
type SetStatusError struct {
	Err        error
	EvalStatus string
}

func (s *SetStatusError) Error() string {
	return s.Err.Error()
}

// GenericScheduler is used for 'synchronous' 'migration' and 'subscribe' type jobs. This scheduler is
// designed for long-lived services, and as such spends more time attemping
// to make a high quality placement. This is the primary scheduler for
// most workloads.
type GenericScheduler struct {
	logger  *log.Logger
	state   State
	planner Planner

	eval       *models.Evaluation
	job        *models.Job
	plan       *models.Plan
	planResult *models.PlanResult
	ctx        *EvalContext

	nextEval *models.Evaluation

	blocked        *models.Evaluation
	failedTGAllocs map[string]*models.AllocMetric
	queuedAllocs   map[string]int
}

// NewGenericScheduler is a factory function to instantiate a new synchronous scheduler
func NewGenericScheduler(logger *log.Logger, state State, planner Planner) Scheduler {
	s := &GenericScheduler{
		logger:  logger,
		state:   state,
		planner: planner,
	}
	return s
}

// Process is used to handle a single evaluation
func (s *GenericScheduler) Process(eval *models.Evaluation) error {
	// Store the evaluation
	s.eval = eval

	// Verify the evaluation trigger reason is understood
	switch eval.TriggeredBy {
	case models.EvalTriggerJobRegister, models.EvalTriggerNodeUpdate,
		models.EvalTriggerJobDeregister, models.EvalTriggerRollingUpdate,
		models.EvalTriggerJobPause, models.EvalTriggerJobResume,
		models.EvalTriggerMaxPlans:
	default:
		desc := fmt.Sprintf("scheduler cannot handle '%s' evaluation reason",
			eval.TriggeredBy)
		return setStatus(s.logger, s.planner, s.eval, s.nextEval, s.blocked,
			s.failedTGAllocs, models.EvalStatusFailed, desc, s.queuedAllocs)
	}

	// Retry up to the maxScheduleAttempts and reset if progress is made.
	progress := func() bool { return progressMade(s.planResult) }
	if err := retryMax(maxScheduleAttempts, s.process, progress); err != nil {
		if statusErr, ok := err.(*SetStatusError); ok {
			// Scheduling was tried but made no forward progress so create a
			// blocked eval to retry once resources become available.
			var mErr multierror.Error
			if err := s.createBlockedEval(true); err != nil {
				mErr.Errors = append(mErr.Errors, err)
			}
			if err := setStatus(s.logger, s.planner, s.eval, s.nextEval, s.blocked,
				s.failedTGAllocs, statusErr.EvalStatus, err.Error(),
				s.queuedAllocs); err != nil {
				mErr.Errors = append(mErr.Errors, err)
			}
			return mErr.ErrorOrNil()
		}
		return err
	}

	// If the current evaluation is a blocked evaluation and we didn't place
	// everything, do not update the status to complete.
	if s.eval.Status == models.EvalStatusBlocked && len(s.failedTGAllocs) != 0 {
		e := s.ctx.Eligibility()
		newEval := s.eval.Copy()
		newEval.EscapedComputedClass = e.HasEscaped()
		newEval.ClassEligibility = e.GetClasses()
		return s.planner.ReblockEval(newEval)
	}

	// Update the status to complete
	return setStatus(s.logger, s.planner, s.eval, s.nextEval, s.blocked,
		s.failedTGAllocs, models.EvalStatusComplete, "", s.queuedAllocs)
}

// createBlockedEval creates a blocked eval and submits it to the planner. If
// failure is set to true, the eval's trigger reason reflects that.
func (s *GenericScheduler) createBlockedEval(planFailure bool) error {
	e := s.ctx.Eligibility()
	escaped := e.HasEscaped()

	// Only store the eligible classes if the eval hasn't escaped.
	var classEligibility map[string]bool
	if !escaped {
		classEligibility = e.GetClasses()
	}

	s.blocked = s.eval.CreateBlockedEval(classEligibility, escaped)
	if planFailure {
		s.blocked.TriggeredBy = models.EvalTriggerMaxPlans
		s.blocked.StatusDescription = blockedEvalMaxPlanDesc
	} else {
		s.blocked.StatusDescription = blockedEvalFailedPlacements
	}

	return s.planner.CreateEval(s.blocked)
}

// process is wrapped in retryMax to iteratively run the handler until we have no
// further work or we've made the maximum number of attempts.
func (s *GenericScheduler) process() (bool, error) {
	// Lookup the Job by ID
	var err error
	ws := memdb.NewWatchSet()
	s.job, err = s.state.JobByID(ws, s.eval.JobID)
	if err != nil {
		return false, fmt.Errorf("failed to get job '%s': %v",
			s.eval.JobID, err)
	}

	numTaskGroups := 0
	if s.job != nil {
		numTaskGroups = len(s.job.Tasks)
		if s.job.Status == models.JobStatusDead || s.job.Status == models.JobStatusComplete {
			return true, nil
		}
	}

	s.queuedAllocs = make(map[string]int, numTaskGroups)

	// Create a plan
	s.plan = s.eval.MakePlan(s.job)

	// Reset the failed allocations
	s.failedTGAllocs = nil

	// Create an evaluation context
	s.ctx = NewEvalContext(s.state, s.plan, s.logger)

	// Compute the target job allocations
	if err := s.computeJobAllocs(); err != nil {
		s.logger.Errorf("sched: %#v: %v", s.eval, err)
		return false, err
	}

	// If there are failed allocations, we need to create a blocked evaluation
	// to place the failed allocations when resources become available. If the
	// current evaluation is already a blocked eval, we reuse it.
	if s.eval.Status != models.EvalStatusBlocked && len(s.failedTGAllocs) != 0 && s.blocked == nil {
		if err := s.createBlockedEval(false); err != nil {
			s.logger.Errorf("sched: %#v failed to make blocked eval: %v", s.eval, err)
			return false, err
		}
		s.logger.Debugf("sched: %#v: failed to place all allocations, blocked eval '%s' created", s.eval, s.blocked.ID)
	}

	// If the plan is a no-op, we can bail. If AnnotatePlan is set submit the plan
	// anyways to get the annotations.
	if s.plan.IsNoOp() && !s.eval.AnnotatePlan {
		return true, nil
	}

	// Submit the plan and store the results.
	result, newState, err := s.planner.SubmitPlan(s.plan)
	s.planResult = result
	if err != nil {
		return false, err
	}

	// Decrement the number of allocations pending per task based on the
	// number of allocations successfully placed
	adjustQueuedAllocations(s.logger, result, s.queuedAllocs)

	// If we got a store refresh, try again since we have stale data
	if newState != nil {
		s.logger.Debugf("sched: %#v: refresh forced", s.eval)
		s.state = newState
		return false, nil
	}

	// Try again if the plan was not fully committed, potential conflict
	fullCommit, expected, actual := result.FullCommit(s.plan)
	if !fullCommit {
		s.logger.Debugf("sched: %#v: attempted %d placements, %d placed",
			s.eval, expected, actual)
		if newState == nil {
			return false, fmt.Errorf("missing state refresh after partial commit")
		}
		return false, nil
	}

	// Success!
	return true, nil
}

// filterCompleteAllocs filters allocations that are terminal and should be
// re-placed.
func (s *GenericScheduler) filterCompleteAllocs(allocs []*models.Allocation) ([]*models.Allocation, map[string]*models.Allocation) {
	filter := func(a *models.Allocation) bool {
		// Filter terminal, non batch allocations
		return a.TerminalStatus()
	}

	terminalAllocsByName := make(map[string]*models.Allocation)
	n := len(allocs)
	for i := 0; i < n; i++ {
		if filter(allocs[i]) {

			// Add the allocation to the terminal allocs map if it's not already
			// added or has a higher create index than the one which is
			// currently present.
			alloc, ok := terminalAllocsByName[allocs[i].Name]
			if !ok || alloc.CreateIndex < allocs[i].CreateIndex {
				terminalAllocsByName[allocs[i].Name] = allocs[i]
			}

			// Remove the allocation
			allocs[i], allocs[n-1] = allocs[n-1], nil
			i--
			n--
		}
	}

	// If the job is batch, we want to filter allocations that have been
	// replaced by a newer version for the same task.
	filtered := allocs[:n]

	return filtered, terminalAllocsByName
}

// computeJobAllocs is used to reconcile differences between the job,
// existing allocations and node status to update the allocations.
func (s *GenericScheduler) computeJobAllocs() error {
	// Materialize all the tasks, job could be missing if deregistered
	var tasks map[string]*models.Task
	if s.job != nil {
		tasks = materializeTasks(s.job)
	}

	// Lookup the allocations by JobID
	ws := memdb.NewWatchSet()
	allocs, err := s.state.AllocsByJob(ws, s.eval.JobID, true)
	if err != nil {
		return fmt.Errorf("failed to get allocs for job '%s': %v",
			s.eval.JobID, err)
	}

	// Determine the tainted nodes containing job allocs
	tainted, err := taintedNodes(s.state, allocs)
	if err != nil {
		return fmt.Errorf("failed to get tainted nodes for job '%s': %v",
			s.eval.JobID, err)
	}

	// Update the allocations which are in pending/running store on tainted
	// nodes to lost
	updateNonTerminalAllocsToLost(s.plan, tainted, allocs)

	// Filter out the allocations in a terminal store
	allocs, terminalAllocs := s.filterCompleteAllocs(allocs)

	// Diff the required and existing allocations
	diff := diffAllocs(s.job, tainted, tasks, allocs, terminalAllocs)
	s.logger.Debugf("sched: %#v: %#v", s.eval, diff)

	// Add all the allocs to stop
	for _, e := range diff.stop {
		s.plan.AppendUpdate(e.Alloc, models.AllocDesiredStatusStop, allocNotNeeded, "")
	}

	for _, e := range diff.pause {
		s.plan.AppendUpdate(e.Alloc, models.AllocDesiredStatusPause, "", models.AllocClientStatusPending)
	}

	for _, e := range diff.resume {
		s.plan.AppendUpdate(e.Alloc, models.AllocDesiredStatusRun, "", "")
	}

	// Attempt to do the upgrades in place
	destructiveUpdates, inplaceUpdates := inplaceUpdate(s.ctx, s.eval, s.job, diff.update)
	diff.update = destructiveUpdates

	if s.eval.AnnotatePlan {
		s.plan.Annotations = &models.PlanAnnotations{
			DesiredTGUpdates: desiredUpdates(diff, inplaceUpdates, destructiveUpdates),
		}
	}

	// Nothing remaining to do if placement is not required
	if len(diff.place) == 0 {
		if s.job != nil {
			for _, tg := range s.job.Tasks {
				s.queuedAllocs[tg.Type] = 0
			}
		}
		return nil
	}

	// Record the number of allocations that needs to be placed per task
	for _, allocTuple := range diff.place {
		s.queuedAllocs[allocTuple.Task.Type] += 1
	}

	// Compute the placements
	return s.computePlacements(diff.place)
}

// computePlacements computes placements for allocations
func (s *GenericScheduler) computePlacements(place []allocTuple) error {
	// Get the base nodes
	nodes, byDC, err := readyNodesInDCs(s.state, s.job.Datacenters)
	if err != nil {
		return err
	}

	if len(nodes) == 0 {
		return fmt.Errorf("no ready nodes")
	}

	s.ctx.Metrics().EvaluateNode()

	for _, missing := range place {
		// Check if this task has already failed
		if metric, ok := s.failedTGAllocs[missing.Task.Type]; ok {
			metric.CoalescedFailures += 1
			continue
		}

		// Find the preferred node
		preferredNode, err := s.findPreferredNode(&missing)
		if err != nil {
			return err
		}

		var nodeId string
		if preferredNode != nil {
			nodeId = preferredNode.ID
		} /*else {
			nodeId = nodes[rand.Intn(len(nodes))].ID
		}*/

		// Store the available nodes by datacenter
		s.ctx.Metrics().NodesAvailable = byDC

		if nodeId != "" {
			// Create an allocation for this
			alloc := &models.Allocation{
				ID:            models.GenerateUUID(),
				EvalID:        s.eval.ID,
				Name:          missing.Name,
				JobID:         s.job.ID,
				Task:          missing.Task.Type,
				Metrics:       s.ctx.Metrics(),
				NodeID:        preferredNode.ID,
				DesiredStatus: models.AllocDesiredStatusRun,
				ClientStatus:  models.AllocClientStatusPending,
			}

			// If the new allocation is replacing an older allocation then we
			// set the record the older allocation id so that they are chained
			if missing.Alloc != nil {
				alloc.PreviousAllocation = missing.Alloc.ID
			}

			if missing.Task.Type == models.TaskTypeDest {
				for i, task := range s.job.Tasks {
					task.Config["NatsAddr"] = preferredNode.NatsAddr
					s.job.Tasks[i] = task
				}
			}
			s.plan.AppendAlloc(alloc)
		} else {
			// Lazy initialize the failed map
			if s.failedTGAllocs == nil {
				s.failedTGAllocs = make(map[string]*models.AllocMetric)
			}

			s.failedTGAllocs[missing.Task.Type] = s.ctx.Metrics()
		}
	}

	return nil
}

// findPreferredNode finds the preferred node for an allocation
func (s *GenericScheduler) findPreferredNode(allocTuple *allocTuple) (node *models.Node, err error) {
	if allocTuple.Alloc != nil {
		task := allocTuple.Alloc.Job.LookupTask(allocTuple.Alloc.Task)
		if task == nil {
			err = fmt.Errorf("can't find task of existing allocation %q", allocTuple.Alloc.ID)
			return
		}
		var preferredNode *models.Node
		ws := memdb.NewWatchSet()
		preferredNode, err = s.state.NodeByID(ws, allocTuple.Alloc.NodeID)
		if preferredNode.Ready() {
			node = preferredNode
		}
	}

	if allocTuple.Task.NodeID != "" {
		var preferredNode *models.Node
		ws := memdb.NewWatchSet()
		preferredNode, err = s.state.NodeByID(ws, allocTuple.Task.NodeID)
		if err != nil || preferredNode == nil {
			return nil, fmt.Errorf("sched: Can't find preferred node %s", allocTuple.Task.NodeID)
		}
		if preferredNode.Ready() {
			node = preferredNode
			return
		}
	}

	if allocTuple.Task.NodeName != "" {
		findNode := false
		nodes, _, err := readyNodesInDCs(s.state, s.job.Datacenters)
		if err != nil {
			return nil, err
		}

		for _, preferredNode := range nodes {
			if preferredNode.Name == allocTuple.Task.NodeName && preferredNode.Ready() {
				node = preferredNode
				findNode = true
			}
		}
		if !findNode {
			return nil, fmt.Errorf("sched: Can't find preferred node %s", allocTuple.Task.NodeName)
		}
	}
	return
}
