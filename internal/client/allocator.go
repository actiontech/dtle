/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package client

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"

	"udup/internal/config"
	log "udup/internal/logger"
	"udup/internal/models"
)

// AllocStateUpdater is used to update the status of an allocation
type AllocStateUpdater func(alloc *models.Allocation)

type AllocStatsReporter interface {
	LatestAllocStats(taskFilter string) (*models.AllocStatistics, error)
}

// Allocator is used to wrap an allocation and provide the execution context.
type Allocator struct {
	config  *config.ClientConfig
	updater AllocStateUpdater
	logger  *log.Logger

	alloc                  *models.Allocation
	allocClientStatus      string // Explicit status of allocation. Set when there are failures
	allocClientDescription string
	allocLock              sync.Mutex

	dirtyCh chan struct{}

	tasks      map[string]*Worker
	taskStates map[string]*models.TaskState
	restored   map[string]struct{}
	taskLock   sync.RWMutex

	taskStatusLock sync.RWMutex

	updateCh    chan *models.Allocation
	workUpdates chan *models.TaskUpdate

	destroy     bool
	destroyCh   chan struct{}
	destroyLock sync.Mutex
	waitCh      chan struct{}

	// serialize saveAllocatorState calls
	persistLock sync.Mutex
}

// allocatorState is used to snapshot the store of the alloc runner
type allocatorState struct {
	Version                string
	Alloc                  *models.Allocation
	AllocClientStatus      string
	AllocClientDescription string
}

// NewAllocator is used to create a new allocation context
func NewAllocator(logger *log.Logger, config *config.ClientConfig, updater AllocStateUpdater,
	alloc *models.Allocation, workUpdates chan *models.TaskUpdate) *Allocator {
	ar := &Allocator{
		config:      config,
		updater:     updater,
		logger:      logger,
		alloc:       alloc,
		dirtyCh:     make(chan struct{}, 1),
		tasks:       make(map[string]*Worker),
		taskStates:  copyTaskStates(alloc.TaskStates),
		restored:    make(map[string]struct{}),
		updateCh:    make(chan *models.Allocation, 64),
		workUpdates: workUpdates,
		destroyCh:   make(chan struct{}),
		waitCh:      make(chan struct{}),
	}
	return ar
}

// stateFilePath returns the path to our store file
func (r *Allocator) stateFilePath() string {
	r.allocLock.Lock()
	defer r.allocLock.Unlock()
	path := filepath.Join(r.config.StateDir, "alloc", r.alloc.ID, "state.json")
	return path
}

// SaveState is used to snapshot the store of the alloc runner
// if the fullSync is marked as false only the store of the Alloc Runner
// is snapshotted. If fullSync is marked as true, we snapshot
// all the Task Runners associated with the Alloc
func (r *Allocator) SaveState() error {
	/*if err := r.saveAllocatorState(); err != nil {
		return err
	}*/

	// Save store for each task
	runners := r.getWorkers()
	var mErr multierror.Error
	for _, tr := range runners {
		if err := r.saveWorkerState(tr); err != nil {
			mErr.Errors = append(mErr.Errors, err)
		}
	}
	return mErr.ErrorOrNil()
}

func (r *Allocator) saveAllocatorState() error {
	r.persistLock.Lock()
	defer r.persistLock.Unlock()

	// Create the snapshot.
	alloc := r.Alloc()

	r.allocLock.Lock()
	allocClientStatus := r.allocClientStatus
	allocClientDescription := r.allocClientDescription
	r.allocLock.Unlock()

	snap := allocatorState{
		Version:                r.config.Version,
		Alloc:                  alloc,
		AllocClientStatus:      allocClientStatus,
		AllocClientDescription: allocClientDescription,
	}
	return persistState(r.stateFilePath(), &snap)
}

func (r *Allocator) saveWorkerState(tr *Worker) error {
	if err := tr.SaveState(); err != nil {
		return fmt.Errorf("failed to save state for alloc %s task '%s': %v",
			r.alloc.ID, tr.task.Type, err)
	}
	return nil
}

// DestroyState is used to cleanup after ourselves
func (r *Allocator) DestroyState() error {
	return os.RemoveAll(filepath.Dir(r.stateFilePath()))
}

// copyTaskStates returns a copy of the passed task states.
func copyTaskStates(states map[string]*models.TaskState) map[string]*models.TaskState {
	copy := make(map[string]*models.TaskState, len(states))
	for task, state := range states {
		copy[task] = state.Copy()
	}
	return copy
}

// Alloc returns the associated allocation
func (r *Allocator) Alloc() *models.Allocation {
	r.allocLock.Lock()
	alloc := r.alloc.Copy()

	// The status has explicitly been set.
	if r.allocClientStatus != "" || r.allocClientDescription != "" {
		alloc.ClientStatus = r.allocClientStatus
		alloc.ClientDescription = r.allocClientDescription

		// Copy over the task states so we don't lose them
		r.taskStatusLock.RLock()
		alloc.TaskStates = copyTaskStates(r.taskStates)
		r.taskStatusLock.RUnlock()

		r.allocLock.Unlock()
		return alloc
	}
	r.allocLock.Unlock()

	// Scan the task states to determine the status of the alloc
	var pending, running, dead, failed bool
	r.taskStatusLock.RLock()
	alloc.TaskStates = copyTaskStates(r.taskStates)
	for _, state := range r.taskStates {
		switch state.State {
		case models.TaskStateRunning:
			running = true
		case models.TaskStatePending:
			pending = true
		case models.TaskStateDead:
			if state.Failed {
				failed = true
			} else {
				dead = true
			}
		}
	}
	r.taskStatusLock.RUnlock()

	// Determine the alloc status
	if failed {
		alloc.ClientStatus = models.AllocClientStatusFailed
	} else if running {
		alloc.ClientStatus = models.AllocClientStatusRunning
	} else if pending {
		alloc.ClientStatus = models.AllocClientStatusPending
	} else if dead {
		alloc.ClientStatus = models.AllocClientStatusComplete
	}

	return alloc
}

// dirtySyncState is used to watch for store being marked dirty to sync
func (r *Allocator) dirtySyncState() {
	for {
		select {
		case <-r.dirtyCh:
			r.syncStatus()
		case <-r.destroyCh:
			return
		}
	}
}

// syncStatus is used to run and sync the status when it changes
func (r *Allocator) syncStatus() error {
	// Get a copy of our alloc, update status server side and sync to disk
	r.logger.Debugf("syncStatus: Alloc")
	alloc := r.Alloc()
	r.logger.Debugf("syncStatus: updater")
	r.updater(alloc)
	//return r.saveAllocatorState()
	return nil
}

// setStatus is used to update the allocation status
func (r *Allocator) setStatus(status, desc string) {
	r.allocLock.Lock()
	r.allocClientStatus = status
	r.allocClientDescription = desc
	r.allocLock.Unlock()
	select {
	case r.dirtyCh <- struct{}{}:
		r.logger.Debugf("setStatus")
	default:
	}
}

// setTaskState is used to set the status of a task. If store is empty then the
// event is appended but not synced with the server. The event may be omitted
func (r *Allocator) setTaskState(taskName, state string, event *models.TaskEvent) {
	r.taskStatusLock.Lock()
	defer r.taskStatusLock.Unlock()
	taskState, ok := r.taskStates[taskName]
	if !ok {
		taskState = &models.TaskState{}
		r.taskStates[taskName] = taskState
	}

	// Set the tasks store.
	if event != nil {
		if event.FailsTask {
			taskState.Failed = true
		}
		r.appendTaskEvent(taskState, event)

		if event.Type == models.TaskKilled {
			state = models.TaskStateStop
		}
	}

	if state == "" {
		return
	}

	switch state {
	case models.TaskStateRunning:
		// Capture the start time if it is just starting
		if taskState.State != models.TaskStateRunning {
			taskState.StartedAt = time.Now()
		}
	case models.TaskStateDead:
		// Capture the finished time. If it has never started there is no finish
		// time
		if !taskState.StartedAt.IsZero() {
			taskState.FinishedAt = time.Now()
		}

		// Find all tasks that are not the one that is dead and check if the one
		// that is dead is a leader
		var otherWorkers []*Worker
		var otherTaskNames []string
		leader := false
		for task, tr := range r.tasks {
			if task != taskName {
				otherWorkers = append(otherWorkers, tr)
				otherTaskNames = append(otherTaskNames, task)
			} else if tr.task.Leader {
				leader = true
			}
		}

		// If the task failed, we should kill all the other tasks in the task.
		if taskState.Failed {
			for _, tr := range otherWorkers {
				tr.Destroy(models.NewTaskEvent(models.TaskSiblingFailed).SetFailedSibling(taskName))
			}
			if len(otherWorkers) > 0 {
				r.logger.Debugf("agent: Task %q failed, destroying other tasks in task: %v", taskName, otherTaskNames)
			}
		} else if leader {
			// If the task was a leader task we should kill all the other tasks.
			for _, tr := range otherWorkers {
				tr.Destroy(models.NewTaskEvent(models.TaskLeaderDead))
			}
			if len(otherWorkers) > 0 {
				r.logger.Debugf("agent: Leader task %q is dead, destroying other tasks in task: %v", taskName, otherTaskNames)
			}
		}
	case models.TaskStateStop:
		// Capture the finished time. If it has never started there is no finish
		// time
		if !taskState.StartedAt.IsZero() {
			taskState.FinishedAt = time.Now()
		}

		// Find all tasks that are not the one that is dead and check if the one
		// that is dead is a leader
		var otherWorkers []*Worker
		var otherTaskNames []string
		leader := false
		for task, tr := range r.tasks {
			if task != taskName {
				otherWorkers = append(otherWorkers, tr)
				otherTaskNames = append(otherTaskNames, task)
			} else if tr.task.Leader {
				leader = true
			}
		}

		// If the task failed, we should kill all the other tasks in the task.
		if taskState.Failed {
			for _, tr := range otherWorkers {
				tr.Destroy(models.NewTaskEvent(models.TaskSiblingFailed).SetFailedSibling(taskName))
			}
			if len(otherWorkers) > 0 {
				r.logger.Debugf("agent: Task %q failed, destroying other tasks in task: %v", taskName, otherTaskNames)
			}
		} else if leader {
			// If the task was a leader task we should kill all the other tasks.
			for _, tr := range otherWorkers {
				tr.Destroy(models.NewTaskEvent(models.TaskLeaderDead))
			}
			if len(otherWorkers) > 0 {
				r.logger.Debugf("agent: Leader task %q is dead, destroying other tasks in task: %v", taskName, otherTaskNames)
			}
		}
	}

	// Store the new store
	taskState.State = state

	select {
	case r.dirtyCh <- struct{}{}:
		r.logger.Debugf("setTaskState: dirtyCh<-")
	default:
	}
}

// appendTaskEvent updates the task status by appending the new event.
func (r *Allocator) appendTaskEvent(state *models.TaskState, event *models.TaskEvent) {
	capacity := 10
	if state.Events == nil {
		state.Events = make([]*models.TaskEvent, 0, capacity)
	}

	// If we hit capacity, then shift it.
	if len(state.Events) == capacity {
		old := state.Events
		state.Events = make([]*models.TaskEvent, 0, capacity)
		state.Events = append(state.Events, old[1:]...)
	}

	state.Events = append(state.Events, event)
}

// Run is a long running goroutine used to manage an allocation
func (r *Allocator) Run() {
	defer close(r.waitCh)
	go r.dirtySyncState()

	// Find the task to run in the allocation
	alloc := r.alloc
	t := alloc.Job.LookupTask(alloc.Task)
	if t == nil {
		r.logger.Errorf("agent: Alloc '%s' for missing task '%s'", alloc.ID, alloc.Task)
		r.setStatus(models.AllocClientStatusFailed, fmt.Sprintf("missing task '%s'", alloc.Task))
		return
	}

	// Check if the allocation is in a terminal status. In this case, we don't
	// start any of the task runners and directly wait for the destroy signal to
	// clean up the allocation.
	if alloc.TerminalStatus() {
		r.logger.Debugf("agent: Alloc %q in terminal status, waiting for destroy", r.alloc.ID)
		r.handleDestroy()
		r.logger.Debugf("agent: Terminating runner for alloc '%s'", r.alloc.ID)
		return
	}

	// Start the task runners
	r.logger.Debugf("agent: Starting task runners for alloc '%s'", r.alloc.ID)
	r.taskLock.Lock()
	if _, ok := r.restored[t.Type]; ok {
		return
	}

	tr := NewWorker(r.logger, r.config, r.setTaskState, r.Alloc(), t.Copy(), r.workUpdates)
	r.tasks[t.Type] = tr
	tr.MarkReceived()

	go tr.Run()
	r.taskLock.Unlock()

	// taskDestroyEvent contains an event that caused the destroyment of a task
	// in the allocation.
	var taskDestroyEvent *models.TaskEvent

OUTER:
	// Wait for updates
	for {
		select {
		case update := <-r.updateCh:
			// Store the updated allocation.
			r.allocLock.Lock()
			r.alloc = update
			r.allocLock.Unlock()

			// Check if we're in a terminal status
			if update.ClientTerminalStatus() {
				taskDestroyEvent = models.NewTaskEvent(models.TaskKilled)
				break OUTER
			}

		case <-r.destroyCh:
			taskDestroyEvent = models.NewTaskEvent(models.TaskKilled)
			break OUTER
		}
	}
	// Kill the task runners
	r.destroyWorkers(taskDestroyEvent)

	// Block until we should destroy the store of the alloc
	r.handleDestroy()
	r.logger.Debugf("agent: Terminating runner for alloc '%s'", r.alloc.ID)
}

// destroyWorkers destroys the task runners, waits for them to terminate and
// then saves store.
func (r *Allocator) destroyWorkers(destroyEvent *models.TaskEvent) {
	// Destroy each sub-task
	runners := r.getWorkers()
	for _, tr := range runners {
		tr.Destroy(destroyEvent)
	}

	// Wait for termination of the task runners
	for _, tr := range runners {
		<-tr.WaitCh()
	}
}

// handleDestroy blocks till the Allocator should be destroyed and does the
// necessary cleanup.
func (r *Allocator) handleDestroy() {
	// Final state sync. We do this to ensure that the server has the correct
	// state as we wait for a destroy.
	//r.syncStatus()
	for {
		select {
		case <-r.destroyCh:
			if err := r.DestroyState(); err != nil {
				r.logger.Errorf("agent: Failed to destroy state for alloc '%s': %v",
					r.alloc.ID, err)
			}
			return
		case <-r.updateCh:
			r.logger.Errorf("agent: Dropping update to terminal alloc '%s'", r.alloc.ID)
		}
	}
}

// Update is used to update the allocation of the context
func (r *Allocator) Update(update *models.Allocation) {
	select {
	case r.updateCh <- update:
	default:
		r.logger.Errorf("agent: Dropping update to alloc '%s'", update.ID)
	}
}

// StatsReporter returns an interface to query resource usage statistics of an
// allocation
func (r *Allocator) StatsReporter() AllocStatsReporter {
	return r
}

// getWorkers is a helper that returns a copy of the task runners list using
// the taskLock.
func (r *Allocator) getWorkers() []*Worker {
	// Get the task runners
	r.taskLock.RLock()
	defer r.taskLock.RUnlock()
	runners := make([]*Worker, 0, len(r.tasks))
	for _, tr := range r.tasks {
		runners = append(runners, tr)
	}
	return runners
}

// LatestAllocStats returns the latest allocation stats. If the optional taskFilter is set
// the allocation stats will only include the given task.
func (r *Allocator) LatestAllocStats(taskFilter string) (*models.AllocStatistics, error) {
	astat := &models.AllocStatistics{
		Tasks: make(map[string]*models.TaskStatistics),
	}

	var flat []*models.TaskStatistics
	if taskFilter != "" {
		r.taskLock.RLock()
		tr, ok := r.tasks[taskFilter]
		r.taskLock.RUnlock()
		if !ok {
			return nil, fmt.Errorf("allocation %q has no task %q", r.alloc.ID, taskFilter)
		}
		l := tr.LatestTaskStats()
		if l != nil {
			astat.Tasks[taskFilter] = l
			flat = []*models.TaskStatistics{l}
		}
	} else {
		// Get the task runners
		runners := r.getWorkers()
		for _, tr := range runners {
			l := tr.LatestTaskStats()
			if l != nil {
				astat.Tasks[tr.task.Type] = l
				flat = append(flat, l)
			}
		}
	}

	return astat, nil
}

// shouldUpdate takes the AllocModifyIndex of an allocation sent from the server and
// checks if the current running allocation is behind and should be updated.
func (r *Allocator) shouldUpdate(serverIndex uint64) bool {
	r.allocLock.Lock()
	defer r.allocLock.Unlock()
	return r.alloc.AllocModifyIndex < serverIndex
}

// Destroy is used to indicate that the allocation context should be destroyed
func (r *Allocator) Destroy() {
	r.destroyLock.Lock()
	defer r.destroyLock.Unlock()

	if r.destroy {
		return
	}
	r.destroy = true
	close(r.destroyCh)
}

// WaitCh returns a channel to wait for termination
func (r *Allocator) WaitCh() <-chan struct{} {
	return r.waitCh
}
