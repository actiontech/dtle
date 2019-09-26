/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package server

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/armon/go-metrics"
	"github.com/sirupsen/logrus"

	"github.com/actiontech/dtle/internal/models"
	"github.com/actiontech/dtle/internal/server/scheduler"
)

const (
	// backoffBaselineFast is the baseline time for exponential backoff
	backoffBaselineFast = 20 * time.Millisecond

	// backoffBaselineSlow is the baseline time for exponential backoff
	// but that is much slower than backoffBaselineFast
	backoffBaselineSlow = 500 * time.Millisecond

	// backoffLimitFast is the limit of the exponential backoff
	backoffLimitFast = time.Second

	// backoffLimitSlow is the limit of the exponential backoff for
	// the slower backoff
	backoffLimitSlow = 10 * time.Second

	// backoffSchedulerVersionMismatch is the backoff between retries when the
	// scheduler version mismatches that of the leader.
	backoffSchedulerVersionMismatch = 30 * time.Second

	// dequeueTimeout is used to timeout an evaluation dequeue so that
	// we can check if there is a shutdown event
	dequeueTimeout = 500 * time.Millisecond

	// raftSyncLimit is the limit of time we will wait for Raft replication
	// to catch up to the evaluation. This is used to fast Nack and
	// allow another scheduler to pick it up.
	raftSyncLimit = 5 * time.Second

	// dequeueErrGrace is the grace period where we don't log about
	// dequeue errors after start. This is to improve the user experience
	// in dev mode where the leader isn't elected for a few seconds.
	dequeueErrGrace = 10 * time.Second
)

// Worker is a single threaded scheduling worker. There may be multiple
// running per server (leader or follower). They are responsible for dequeuing
// pending evaluations, invoking schedulers, plan submission and the
// lifecycle around making task allocations. They bridge the business logic
// of the scheduler with the plumbing required to make it all work.
type Worker struct {
	srv    *Server
	logger *logrus.Logger
	start  time.Time

	paused    bool
	pauseLock sync.Mutex
	pauseCond *sync.Cond

	failures uint

	evalToken string

	// snapshotIndex is the index of the snapshot in which the scheduler was
	// first envoked. It is used to mark the SnapshotIndex of evaluations
	// Created, Updated or Reblocked.
	snapshotIndex uint64
}

// NewWorker starts a new worker associated with the given server
func NewWorker(srv *Server) (*Worker, error) {
	w := &Worker{
		srv:    srv,
		logger: srv.logger,
		start:  time.Now(),
	}
	w.pauseCond = sync.NewCond(&w.pauseLock)
	go w.run()
	return w, nil
}

// SetPause is used to pause or unpause a worker
func (w *Worker) SetPause(p bool) {
	w.pauseLock.Lock()
	w.paused = p
	w.pauseLock.Unlock()
	if !p {
		w.pauseCond.Broadcast()
	}
}

// checkPaused is used to park the worker when paused
func (w *Worker) checkPaused() {
	w.pauseLock.Lock()
	for w.paused {
		w.pauseCond.Wait()
	}
	w.pauseLock.Unlock()
}

// run is the long-lived goroutine which is used to run the worker
func (w *Worker) run() {
	for {
		// Dequeue a pending evaluation
		eval, token, shutdown := w.dequeueEvaluation(dequeueTimeout)
		if shutdown {
			return
		}

		// Check for a shutdown
		if w.srv.IsShutdown() {
			w.sendAck(eval.ID, token, false)
			return
		}

		// Wait for the raft log to catchup to the evaluation
		if err := w.waitForIndex(eval.ModifyIndex, raftSyncLimit); err != nil {
			w.sendAck(eval.ID, token, false)
			continue
		}

		// Invoke the scheduler to determine placements
		if err := w.invokeScheduler(eval, token); err != nil {
			w.sendAck(eval.ID, token, false)
			continue
		}

		// Complete the evaluation
		w.sendAck(eval.ID, token, true)
	}
}

// dequeueEvaluation is used to fetch the next ready evaluation.
// This blocks until an evaluation is available or a timeout is reached.
func (w *Worker) dequeueEvaluation(timeout time.Duration) (*models.Evaluation, string, bool) {
	// Setup the request
	req := models.EvalDequeueRequest{
		Schedulers:       w.srv.config.EnabledSchedulers,
		Timeout:          timeout,
		SchedulerVersion: scheduler.SchedulerVersion,
		WriteRequest: models.WriteRequest{
			Region: w.srv.config.Region,
		},
	}
	var resp models.EvalDequeueResponse

REQ:
	// Check if we are paused
	w.checkPaused()

	// Make a blocking RPC
	start := time.Now()
	err := w.srv.RPC("Eval.Dequeue", &req, &resp)
	metrics.MeasureSince([]string{"server", "worker", "dequeue_eval"}, start)
	if err != nil {
		if time.Since(w.start) > dequeueErrGrace && !w.srv.IsShutdown() {
			w.logger.Errorf("worker: Failed to dequeue evaluation: %v", err)
		}

		// Adjust the backoff based on the error. If it is a scheduler version
		// mismatch we increase the baseline.
		base, limit := backoffBaselineFast, backoffLimitSlow
		if strings.Contains(err.Error(), "calling scheduler version") {
			base = backoffSchedulerVersionMismatch
			limit = backoffSchedulerVersionMismatch
		}

		if w.backoffErr(base, limit) {
			return nil, "", true
		}
		goto REQ
	}
	w.backoffReset()

	// Check if we got a response
	if resp.Eval != nil {
		w.logger.Debugf("worker: Dequeued evaluation %s", resp.Eval.ID)
		return resp.Eval, resp.Token, false
	}

	// Check for potential shutdown
	if w.srv.IsShutdown() {
		return nil, "", true
	}
	goto REQ
}

// sendAck makes a best effort to ack or nack the evaluation.
// Any errors are logged but swallowed.
func (w *Worker) sendAck(evalID, token string, ack bool) {
	defer metrics.MeasureSince([]string{"server", "worker", "send_ack"}, time.Now())
	// Setup the request
	req := models.EvalAckRequest{
		EvalID: evalID,
		Token:  token,
		WriteRequest: models.WriteRequest{
			Region: w.srv.config.Region,
		},
	}
	var resp models.GenericResponse

	// Determine if this is an Ack or Nack
	verb := "ack"
	endpoint := "Eval.Ack"
	if !ack {
		verb = "nack"
		endpoint = "Eval.Nack"
	}

	// Make the RPC call
	err := w.srv.RPC(endpoint, &req, &resp)
	if err != nil {
		w.logger.Errorf("worker: Failed to %s evaluation '%s': %v",
			verb, evalID, err)
	} else {
		w.logger.Debugf("worker: %s for evaluation %s", verb, evalID)
	}
}

// waitForIndex ensures that the local state is at least as fresh
// as the given index. This is used before starting an evaluation,
// but also potentially mid-stream. If a Plan fails because of stale
// state (attempt to allocate to a failed/dead node), we may need
// to sync our state again and do the planning with more recent data.
func (w *Worker) waitForIndex(index uint64, timeout time.Duration) error {
	// XXX: Potential optimization is to set up a watch on the state stores
	// index table and only unblock via a trigger rather than timing out and
	// checking.

	start := time.Now()
	defer metrics.MeasureSince([]string{"server", "worker", "wait_for_index"}, start)
CHECK:
	// Get the states current index
	snapshotIndex, err := w.srv.fsm.State().LatestIndex()
	if err != nil {
		return fmt.Errorf("failed to determine state store's index: %v", err)
	}

	// We only need the FSM state to be as recent as the given index
	if index <= snapshotIndex {
		w.backoffReset()
		return nil
	}

	// Check if we've reached our limit
	if time.Now().Sub(start) > timeout {
		return fmt.Errorf("sync wait timeout reached")
	}

	// Exponential back off if we haven't yet reached it
	if w.backoffErr(backoffBaselineFast, backoffLimitFast) {
		return fmt.Errorf("shutdown while waiting for state sync")
	}
	goto CHECK
}

// invokeScheduler is used to invoke the business logic of the scheduler
func (w *Worker) invokeScheduler(eval *models.Evaluation, token string) error {
	defer metrics.MeasureSince([]string{"server", "worker", "invoke_scheduler", eval.Type}, time.Now())
	// Store the evaluation token
	w.evalToken = token

	// Snapshot the current state
	snap, err := w.srv.fsm.State().Snapshot()
	if err != nil {
		return fmt.Errorf("failed to snapshot state: %v", err)
	}

	// Store the snapshot's index
	w.snapshotIndex, err = snap.LatestIndex()
	if err != nil {
		return fmt.Errorf("failed to determine snapshot's index: %v", err)
	}

	// Create the scheduler, or use the special system scheduler
	var sched scheduler.Scheduler
	sched, err = scheduler.NewScheduler(eval.Type, w.logger, snap, w)
	if err != nil {
		return fmt.Errorf("failed to instantiate scheduler: %v", err)
	}

	// Process the evaluation
	err = sched.Process(eval)
	if err != nil {
		return fmt.Errorf("failed to process evaluation: %v", err)
	}
	return nil
}

// SubmitPlan is used to submit a plan for consideration. This allows
// the worker to act as the planner for the scheduler.
func (w *Worker) SubmitPlan(plan *models.Plan) (*models.PlanResult, scheduler.State, error) {
	// Check for a shutdown before plan submission
	if w.srv.IsShutdown() {
		return nil, nil, fmt.Errorf("shutdown while planning")
	}
	defer metrics.MeasureSince([]string{"server", "worker", "submit_plan"}, time.Now())

	// Add the evaluation token to the plan
	plan.EvalToken = w.evalToken

	// Setup the request
	req := models.PlanRequest{
		Plan: plan,
		WriteRequest: models.WriteRequest{
			Region: w.srv.config.Region,
		},
	}
	var resp models.PlanResponse

SUBMIT:
	// Make the RPC call
	if err := w.srv.RPC("Plan.Submit", &req, &resp); err != nil {
		w.logger.Errorf("worker: Failed to submit plan for evaluation %s: %v",
			plan.EvalID, err)
		if w.shouldResubmit(err) && !w.backoffErr(backoffBaselineSlow, backoffLimitSlow) {
			goto SUBMIT
		}
		return nil, nil, err
	} else {
		w.logger.Debugf("worker: Submitted plan for evaluation %s", plan.EvalID)
		w.backoffReset()
	}

	// Look for a result
	result := resp.Result
	if result == nil {
		return nil, nil, fmt.Errorf("missing result")
	}

	// Check if a state update is required. This could be required if we
	// planning based on stale data, which is causing issues. For example, a
	// node failure since the time we've started planning or conflicting task
	// allocations.
	var state scheduler.State
	if result.RefreshIndex != 0 {
		// Wait for the raft log to catchup to the evaluation
		w.logger.Debugf("worker: Refreshing state to index %d for %q", result.RefreshIndex, plan.EvalID)
		if err := w.waitForIndex(result.RefreshIndex, raftSyncLimit); err != nil {
			return nil, nil, err
		}

		// Snapshot the current state
		snap, err := w.srv.fsm.State().Snapshot()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to snapshot state: %v", err)
		}
		state = snap
	}

	// Return the result and potential state update
	return result, state, nil
}

// UpdateEval is used to submit an updated evaluation. This allows
// the worker to act as the planner for the scheduler.
func (w *Worker) UpdateEval(eval *models.Evaluation) error {
	// Check for a shutdown before plan submission
	if w.srv.IsShutdown() {
		return fmt.Errorf("shutdown while planning")
	}
	defer metrics.MeasureSince([]string{"server", "worker", "update_eval"}, time.Now())

	// Store the snapshot index in the eval
	eval.SnapshotIndex = w.snapshotIndex

	// Setup the request
	req := models.EvalUpdateRequest{
		Evals:     []*models.Evaluation{eval},
		EvalToken: w.evalToken,
		WriteRequest: models.WriteRequest{
			Region: w.srv.config.Region,
		},
	}
	var resp models.GenericResponse

SUBMIT:
	// Make the RPC call
	if err := w.srv.RPC("Eval.Update", &req, &resp); err != nil {
		w.logger.Errorf("worker: Failed to update evaluation %#v: %v",
			eval, err)
		if w.shouldResubmit(err) && !w.backoffErr(backoffBaselineSlow, backoffLimitSlow) {
			goto SUBMIT
		}
		return err
	} else {
		w.logger.Debugf("worker: Updated evaluation %#v", eval)
		w.backoffReset()
	}
	return nil
}

// CreateEval is used to create a new evaluation. This allows
// the worker to act as the planner for the scheduler.
func (w *Worker) CreateEval(eval *models.Evaluation) error {
	// Check for a shutdown before plan submission
	if w.srv.IsShutdown() {
		return fmt.Errorf("shutdown while planning")
	}
	defer metrics.MeasureSince([]string{"server", "worker", "create_eval"}, time.Now())

	// Store the snapshot index in the eval
	eval.SnapshotIndex = w.snapshotIndex

	// Setup the request
	req := models.EvalUpdateRequest{
		Evals:     []*models.Evaluation{eval},
		EvalToken: w.evalToken,
		WriteRequest: models.WriteRequest{
			Region: w.srv.config.Region,
		},
	}
	var resp models.GenericResponse

SUBMIT:
	// Make the RPC call
	if err := w.srv.RPC("Eval.Create", &req, &resp); err != nil {
		w.logger.Errorf("worker: Failed to create evaluation %#v: %v",
			eval, err)
		if w.shouldResubmit(err) && !w.backoffErr(backoffBaselineSlow, backoffLimitSlow) {
			goto SUBMIT
		}
		return err
	} else {
		w.logger.Debugf("worker: Created evaluation %#v", eval)
		w.backoffReset()
	}
	return nil
}

// ReblockEval is used to reinsert a blocked evaluation into the blocked eval
// tracker. This allows the worker to act as the planner for the scheduler.
func (w *Worker) ReblockEval(eval *models.Evaluation) error {
	// Check for a shutdown before plan submission
	if w.srv.IsShutdown() {
		return fmt.Errorf("shutdown while planning")
	}
	defer metrics.MeasureSince([]string{"server", "worker", "reblock_eval"}, time.Now())

	// Store the snapshot index in the eval
	eval.SnapshotIndex = w.snapshotIndex

	// Setup the request
	req := models.EvalUpdateRequest{
		Evals:     []*models.Evaluation{eval},
		EvalToken: w.evalToken,
		WriteRequest: models.WriteRequest{
			Region: w.srv.config.Region,
		},
	}
	var resp models.GenericResponse

SUBMIT:
	// Make the RPC call
	if err := w.srv.RPC("Eval.Reblock", &req, &resp); err != nil {
		w.logger.Errorf("worker: Failed to reblock evaluation %#v: %v",
			eval, err)
		if w.shouldResubmit(err) && !w.backoffErr(backoffBaselineSlow, backoffLimitSlow) {
			goto SUBMIT
		}
		return err
	} else {
		w.logger.Debugf("worker: Reblocked evaluation %#v", eval)
		w.backoffReset()
	}
	return nil
}

// shouldResubmit checks if a given error should be swallowed and the plan
// resubmitted after a backoff. Usually these are transient errors that
// the cluster should heal from quickly.
func (w *Worker) shouldResubmit(err error) bool {
	s := err.Error()
	switch {
	case strings.Contains(s, "No cluster leader"):
		return true
	case strings.Contains(s, "plan queue is disabled"):
		return true
	default:
		return false
	}
}

// backoffErr is used to do an exponential back off on error. This is
// maintained statefully for the worker. Returns if attempts should be
// abandoneded due to shutdown.
func (w *Worker) backoffErr(base, limit time.Duration) bool {
	backoff := (1 << (2 * w.failures)) * base
	if backoff > limit {
		backoff = limit
	} else {
		w.failures++
	}
	select {
	case <-time.After(backoff):
		return false
	case <-w.srv.shutdownCh:
		return true
	}
}

// backoffReset is used to reset the failure count for
// exponential backoff
func (w *Worker) backoffReset() {
	w.failures = 0
}
