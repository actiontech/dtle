/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package scheduler

import (
	"fmt"
	"math/rand"
	"reflect"

	memdb "github.com/hashicorp/go-memdb"

	"github.com/actiontech/dts/internal/models"
	"github.com/sirupsen/logrus"
)

// allocTuple is a tuple of the allocation name and potential alloc ID
type allocTuple struct {
	Name  string
	Task  *models.Task
	Alloc *models.Allocation
}

// materializeTasks is used to materialize all the tasks
// a job requires. This is used to do the count expansion.
func materializeTasks(job *models.Job) map[string]*models.Task {
	out := make(map[string]*models.Task)
	if job == nil {
		return out
	}

	for _, t := range job.Tasks {
		name := fmt.Sprintf("%s.%s", job.Name, t.Type)
		out[name] = t
	}
	return out
}

// diffResult is used to return the sets that result from the diff
type diffResult struct {
	place, update, migrate, resume, pause, stop, ignore, lost []allocTuple
}

func (d *diffResult) GoString() string {
	return fmt.Sprintf("allocs: (place %d) (update %d) (migrate %d) (resume %d) (pause %d) (stop %d) (ignore %d) (lost %d)",
		len(d.place), len(d.update), len(d.migrate), len(d.resume), len(d.pause), len(d.stop), len(d.ignore), len(d.lost))
}

func (d *diffResult) Append(other *diffResult) {
	d.place = append(d.place, other.place...)
	d.update = append(d.update, other.update...)
	d.migrate = append(d.migrate, other.migrate...)
	d.resume = append(d.resume, other.resume...)
	d.pause = append(d.pause, other.pause...)
	d.stop = append(d.stop, other.stop...)
	d.ignore = append(d.ignore, other.ignore...)
	d.lost = append(d.lost, other.lost...)
}

func diffAllocs(job *models.Job, taintedNodes map[string]*models.Node,
	required map[string]*models.Task, allocs []*models.Allocation,
	terminalAllocs map[string]*models.Allocation) *diffResult {
	result := &diffResult{}

	// Scan the existing updates
	existing := make(map[string]struct{})
	for _, exist := range allocs {
		//exist.Job.Status = job.Status
		// Index the existing node
		name := exist.Name
		existing[name] = struct{}{}

		// Check for the definition in the required set
		t, ok := required[name]

		// If not required, we stop the alloc
		if !ok {
			result.stop = append(result.stop, allocTuple{
				Name:  name,
				Task:  t,
				Alloc: exist,
			})
			continue
		}

		// If we are on a tainted node, we must migrate if we are a service or
		// if the batch allocation did not finish
		if node, ok := taintedNodes[exist.NodeID]; ok {
			// If the job is batch and finished successfully, the fact that the
			// node is tainted does not mean it should be migrated or marked as
			// lost as the work was already successfully finished. However for
			// service/system jobs, tasks should never complete. The check of
			// batch type, defends against client bugs.
			if /*exist.Job.Type == models.JobType &&*/ exist.RanSuccessfully() {
				goto IGNORE
			}

			if node == nil || node.TerminalStatus() {
				result.lost = append(result.lost, allocTuple{
					Name:  name,
					Task:  t,
					Alloc: exist,
				})
			} else {
				// This is the drain case
				result.migrate = append(result.migrate, allocTuple{
					Name:  name,
					Task:  t,
					Alloc: exist,
				})
			}
			continue
		}

		// If the definition is updated we need to update
		if job.JobModifyIndex != exist.Job.JobModifyIndex {
			if job.Status == models.JobStatusPause {
				result.pause = append(result.pause, allocTuple{
					Name:  name,
					Task:  t,
					Alloc: exist,
				})
				continue
			} else if job.Status == models.JobStatusRunning {
				result.resume = append(result.resume, allocTuple{
					Name:  name,
					Task:  t,
					Alloc: exist,
				})
				continue
			} else {
				result.update = append(result.update, allocTuple{
					Name:  name,
					Task:  t,
					Alloc: exist,
				})
				continue
			}
		}

		// Everything is up-to-date
	IGNORE:
		result.ignore = append(result.ignore, allocTuple{
			Name:  name,
			Task:  t,
			Alloc: exist,
		})
	}

	// Scan the required groups
	for name, t := range required {
		// Check for an existing allocation
		_, ok := existing[name]

		// Require a placement if no existing allocation. If there
		// is an existing allocation, we would have checked for a potential
		// update or ignore above.
		if !ok {
			result.place = append(result.place, allocTuple{
				Name:  name,
				Task:  t,
				Alloc: terminalAllocs[name],
			})
		}
	}
	return result
}

// readyNodesInDCs returns all the ready nodes in the given datacenters and a
// mapping of each data center to the count of ready nodes.
func readyNodesInDCs(state State, dcs []string) ([]*models.Node, map[string]int, error) {
	// Index the DCs
	dcMap := make(map[string]int, len(dcs))
	for _, dc := range dcs {
		dcMap[dc] = 0
	}

	// Scan the nodes
	ws := memdb.NewWatchSet()
	var out []*models.Node
	iter, err := state.Nodes(ws)
	if err != nil {
		return nil, nil, err
	}
	for {
		raw := iter.Next()
		if raw == nil {
			break
		}

		// Filter on datacenter and status
		node := raw.(*models.Node)
		if node.Status != models.NodeStatusReady {
			continue
		}

		if _, ok := dcMap[node.Datacenter]; !ok {
			continue
		}
		out = append(out, node)
		dcMap[node.Datacenter] += 1
	}
	return out, dcMap, nil
}

// retryMax is used to retry a callback until it returns success or
// a maximum number of attempts is reached. An optional reset function may be
// passed which is called after each failed iteration. If the reset function is
// set and returns true, the number of attempts is reset back to max.
func retryMax(max int, cb func() (bool, error), reset func() bool) error {
	attempts := 0
	for attempts < max {
		done, err := cb()
		if err != nil {
			return err
		}
		if done {
			return nil
		}

		// Check if we should reset the number attempts
		if reset != nil && reset() {
			attempts = 0
		} else {
			attempts += 1
		}
	}
	return &SetStatusError{
		Err:        fmt.Errorf("maximum attempts reached (%d)", max),
		EvalStatus: models.EvalStatusFailed,
	}
}

// progressMade checks to see if the plan result made allocations or updates.
// If the result is nil, false is returned.
func progressMade(result *models.PlanResult) bool {
	return result != nil && (len(result.NodeUpdate) != 0 ||
		len(result.NodeAllocation) != 0)
}

// taintedNodes is used to scan the allocations and then check if the
// underlying nodes are tainted, and should force a migration of the allocation.
// All the nodes returned in the map are tainted.
func taintedNodes(state State, allocs []*models.Allocation) (map[string]*models.Node, error) {
	out := make(map[string]*models.Node)
	for _, alloc := range allocs {
		if _, ok := out[alloc.NodeID]; ok {
			continue
		}

		ws := memdb.NewWatchSet()
		node, err := state.NodeByID(ws, alloc.NodeID)
		if err != nil {
			return nil, err
		}

		// If the node does not exist, we should migrate
		if node == nil {
			out[alloc.NodeID] = nil
			continue
		}
	}
	return out, nil
}

// shuffleNodes randomizes the slice order with the Fisher-Yates algorithm
func shuffleNodes(nodes []*models.Node) {
	n := len(nodes)
	for i := n - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
}

// tasksUpdated does a diff between tasks to see if the
// tasks, their drivers, environment variables or config have updated. The
// inputs are the task name to diff and two jobs to diff.
func tasksUpdated(jobA, jobB *models.Job, task string) bool {
	a := jobA.LookupTask(task)
	b := jobB.LookupTask(task)

	// Check task
	if a.Driver != b.Driver {
		return true
	}

	if !reflect.DeepEqual(a.Config, b.Config) {
		return true
	}

	return false
}

// setStatus is used to update the status of the evaluation
func setStatus(logger *logrus.Logger, planner Planner,
	eval, nextEval, spawnedBlocked *models.Evaluation,
	tgMetrics map[string]*models.AllocMetric, status, desc string,
	queuedAllocs map[string]int) error {

	logger.Debugf("sched: %#v: setting status to %s", eval, status)
	newEval := eval.Copy()
	newEval.Status = status
	newEval.StatusDescription = desc
	newEval.FailedTGAllocs = tgMetrics
	if nextEval != nil {
		newEval.NextEval = nextEval.ID
	}
	if spawnedBlocked != nil {
		newEval.BlockedEval = spawnedBlocked.ID
	}
	if queuedAllocs != nil {
		newEval.QueuedAllocations = queuedAllocs
	}

	return planner.UpdateEval(newEval)
}

// inplaceUpdate attempts to update allocations in-place where possible. It
// returns the allocs that couldn't be done inplace and then those that could.
func inplaceUpdate(ctx Context, eval *models.Evaluation, job *models.Job,
	updates []allocTuple) (destructive, inplace []allocTuple) {

	// doInplace manipulates the updates map to make the current allocation
	// an inplace update.
	doInplace := func(cur, last, inplaceCount *int) {
		updates[*cur], updates[*last-1] = updates[*last-1], updates[*cur]
		*cur--
		*last--
		*inplaceCount++
	}

	ws := memdb.NewWatchSet()
	n := len(updates)
	inplaceCount := 0
	for i := 0; i < n; i++ {
		// Get the update
		update := updates[i]

		// Check if the task drivers or config has changed, requires
		// a rolling upgrade since that cannot be done in-place.
		existing := update.Alloc.Job
		if tasksUpdated(job, existing, update.Task.Type) {
			continue
		}

		// Terminal batch allocations are not filtered when they are completed
		// successfully. We should avoid adding the allocation to the plan in
		// the case that it is an in-place update to avoid both additional data
		// in the plan and work for the clients.
		if update.Alloc.ClientTerminalStatus() {
			doInplace(&i, &n, &inplaceCount)
			continue
		}

		// Get the existing node
		node, err := ctx.State().NodeByID(ws, update.Alloc.NodeID)
		if err != nil {
			ctx.Logger().Errorf("sched: %#v failed to get node '%s': %v",
				eval, update.Alloc.NodeID, err)
			continue
		}
		if node == nil {
			continue
		}

		// Stage an eviction of the current allocation. This is done so that
		// the current allocation is discounted when checking for feasability.
		// Otherwise we would be trying to fit the tasks current resources and
		// updated resources. After select is called we can remove the evict.
		ctx.Plan().AppendUpdate(update.Alloc, models.AllocDesiredStatusStop,
			allocInPlace, "")

		// Pop the allocation
		ctx.Plan().PopUpdate(update.Alloc)

		// Create a shallow copy
		newAlloc := new(models.Allocation)
		*newAlloc = *update.Alloc

		// Update the allocation
		newAlloc.EvalID = eval.ID
		newAlloc.Job = nil // Use the Job in the Plan
		newAlloc.Metrics = ctx.Metrics()
		ctx.Plan().AppendAlloc(newAlloc)

		// Remove this allocation from the slice
		doInplace(&i, &n, &inplaceCount)
	}

	if len(updates) > 0 {
		ctx.Logger().Debugf("sched: %#v: %d in-place updates of %d", eval, inplaceCount, len(updates))
	}
	return updates[:n], updates[n:]
}

// taskConstrainTuple is used to store the total constraints of a task.
type taskConstrainTuple struct {
	// Holds the combined constraints of the task and all it's sub-tasks.
	constraints []*models.Constraint

	// The set of required drivers within the task.
	drivers map[string]struct{}
}

func taskConstraints(t *models.Task) taskConstrainTuple {
	c := taskConstrainTuple{
		constraints: make([]*models.Constraint, 0, len(t.Constraints)),
		drivers:     make(map[string]struct{}),
	}

	c.constraints = append(c.constraints, t.Constraints...)
	c.drivers[t.Driver] = struct{}{}
	c.constraints = append(c.constraints, t.Constraints...)

	return c
}

// desiredUpdates takes the diffResult as well as the set of inplace and
// destructive updates and returns a map of tasks to their set of desired
// updates.
func desiredUpdates(diff *diffResult, inplaceUpdates,
	destructiveUpdates []allocTuple) map[string]*models.DesiredUpdates {
	desiredTgs := make(map[string]*models.DesiredUpdates)

	for _, tuple := range diff.place {
		name := tuple.Task.Type
		des, ok := desiredTgs[name]
		if !ok {
			des = &models.DesiredUpdates{}
			desiredTgs[name] = des
		}

		des.Place++
	}

	for _, tuple := range diff.stop {
		name := tuple.Alloc.Task
		des, ok := desiredTgs[name]
		if !ok {
			des = &models.DesiredUpdates{}
			desiredTgs[name] = des
		}

		des.Stop++
	}

	for _, tuple := range diff.ignore {
		name := tuple.Task.Type
		des, ok := desiredTgs[name]
		if !ok {
			des = &models.DesiredUpdates{}
			desiredTgs[name] = des
		}

		des.Ignore++
	}

	for _, tuple := range diff.migrate {
		name := tuple.Task.Type
		des, ok := desiredTgs[name]
		if !ok {
			des = &models.DesiredUpdates{}
			desiredTgs[name] = des
		}

		des.Migrate++
	}

	for _, tuple := range inplaceUpdates {
		name := tuple.Task.Type
		des, ok := desiredTgs[name]
		if !ok {
			des = &models.DesiredUpdates{}
			desiredTgs[name] = des
		}

		des.InPlaceUpdate++
	}

	for _, tuple := range destructiveUpdates {
		name := tuple.Task.Type
		des, ok := desiredTgs[name]
		if !ok {
			des = &models.DesiredUpdates{}
			desiredTgs[name] = des
		}

		des.DestructiveUpdate++
	}

	return desiredTgs
}

// adjustQueuedAllocations decrements the number of allocations pending per task
// group based on the number of allocations successfully placed
func adjustQueuedAllocations(logger *logrus.Logger, result *models.PlanResult, queuedAllocs map[string]int) {
	if result != nil {
		for _, allocations := range result.NodeAllocation {
			for _, allocation := range allocations {
				// Ensure that the allocation is newly created. We check that
				// the CreateIndex is equal to the ModifyIndex in order to check
				// that the allocation was just created. We do not check that
				// the CreateIndex is equal to the results AllocIndex because
				// the allocations we get back have gone through the planner's
				// optimistic snapshot and thus their indexes may not be
				// correct, but they will be consistent.
				if allocation.CreateIndex != allocation.ModifyIndex {
					continue
				}

				if _, ok := queuedAllocs[allocation.Task]; ok {
					queuedAllocs[allocation.Task] -= 1
				} else {
					logger.Errorf("sched: allocation %q placed but not in list of unplaced allocations", allocation.Task)
				}
			}
		}
	}
}

// updateNonTerminalAllocsToLost updates the allocations which are in pending/running store on tainted node
// to lost
func updateNonTerminalAllocsToLost(plan *models.Plan, tainted map[string]*models.Node, allocs []*models.Allocation) {
	for _, alloc := range allocs {
		if _, ok := tainted[alloc.NodeID]; ok &&
			alloc.DesiredStatus == models.AllocDesiredStatusStop &&
			(alloc.ClientStatus == models.AllocClientStatusRunning ||
				alloc.ClientStatus == models.AllocClientStatusPending) {
			plan.AppendUpdate(alloc, models.AllocDesiredStatusStop, allocLost, models.AllocClientStatusLost)
		}
	}
}
