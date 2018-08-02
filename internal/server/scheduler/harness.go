/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package scheduler

import (
	"fmt"
	"os"
	"sync"
	"testing"

	memdb "github.com/hashicorp/go-memdb"

	log "udup/internal/logger"
	"udup/internal/models"
	"udup/internal/server/store"
)

type Harness struct {
	State *store.StateStore

	Planner  Planner
	planLock sync.Mutex

	Plans        []*models.Plan
	Evals        []*models.Evaluation
	CreateEvals  []*models.Evaluation
	ReblockEvals []*models.Evaluation

	nextIndex     uint64
	nextIndexLock sync.Mutex
}

// SubmitPlan is used to handle plan submission
func (h *Harness) SubmitPlan(plan *models.Plan) (*models.PlanResult, State, error) {
	// Ensure sequential plan application
	h.planLock.Lock()
	defer h.planLock.Unlock()

	// Store the plan
	h.Plans = append(h.Plans, plan)

	// Check for custom planner
	if h.Planner != nil {
		return h.Planner.SubmitPlan(plan)
	}

	// Get the index
	index := h.NextIndex()

	// Prepare the result
	result := new(models.PlanResult)
	result.NodeUpdate = plan.NodeUpdate
	result.NodeAllocation = plan.NodeAllocation
	result.AllocIndex = index

	// Flatten evicts and allocs
	var allocs []*models.Allocation
	for _, updateList := range plan.NodeUpdate {
		allocs = append(allocs, updateList...)
	}
	for _, allocList := range plan.NodeAllocation {
		allocs = append(allocs, allocList...)
	}

	// Attach the plan to all the allocations. It is pulled out in the
	// payload to avoid the redundancy of encoding, but should be denormalized
	// prior to being inserted into MemDB.
	if j := plan.Job; j != nil {
		for _, alloc := range allocs {
			if alloc.Job == nil {
				alloc.Job = j
			}
		}
	}

	// Apply the full plan
	err := h.State.UpsertAllocs(index, allocs)
	return result, nil, err
}

func (h *Harness) UpdateEval(eval *models.Evaluation) error {
	// Ensure sequential plan application
	h.planLock.Lock()
	defer h.planLock.Unlock()

	// Store the eval
	h.Evals = append(h.Evals, eval)

	// Check for custom planner
	if h.Planner != nil {
		return h.Planner.UpdateEval(eval)
	}
	return nil
}

func (h *Harness) CreateEval(eval *models.Evaluation) error {
	// Ensure sequential plan application
	h.planLock.Lock()
	defer h.planLock.Unlock()

	// Store the eval
	h.CreateEvals = append(h.CreateEvals, eval)

	// Check for custom planner
	if h.Planner != nil {
		return h.Planner.CreateEval(eval)
	}
	return nil
}

func (h *Harness) ReblockEval(eval *models.Evaluation) error {
	// Ensure sequential plan application
	h.planLock.Lock()
	defer h.planLock.Unlock()

	// Check that the evaluation was already blocked.
	ws := memdb.NewWatchSet()
	old, err := h.State.EvalByID(ws, eval.ID)
	if err != nil {
		return err
	}

	if old == nil {
		return fmt.Errorf("evaluation does not exist to be reblocked")
	}
	if old.Status != models.EvalStatusBlocked {
		return fmt.Errorf("evaluation %q is not already in a blocked store", old.ID)
	}

	h.ReblockEvals = append(h.ReblockEvals, eval)
	return nil
}

// NextIndex returns the next index
func (h *Harness) NextIndex() uint64 {
	h.nextIndexLock.Lock()
	defer h.nextIndexLock.Unlock()
	idx := h.nextIndex
	h.nextIndex += 1
	return idx
}

// Snapshot is used to snapshot the current store
func (h *Harness) Snapshot() State {
	snap, _ := h.State.Snapshot()
	return snap
}

// Scheduler is used to return a new scheduler from
// a snapshot of current store using the harness for planning.
func (h *Harness) Scheduler(factory Factory) Scheduler {
	logger := log.New(os.Stderr, log.InfoLevel)
	return factory(logger, h.Snapshot(), h)
}

// Process is used to process an evaluation given a factory
// function to create the scheduler
func (h *Harness) Process(factory Factory, eval *models.Evaluation) error {
	sched := h.Scheduler(factory)
	return sched.Process(eval)
}

func (h *Harness) AssertEvalStatus(t *testing.T, state string) {
	if len(h.Evals) != 1 {
		t.Fatalf("bad: %#v", h.Evals)
	}
	update := h.Evals[0]

	if update.Status != state {
		t.Fatalf("bad: %#v", update)
	}
}
