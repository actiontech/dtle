/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package client

import (
	"math/rand"
	"sync"
	"time"

	"github.com/actiontech/dtle/internal/models"
)

const (
	// jitter is the percent of jitter added to restart delays.
	jitter = 0.25

	ReasonUnrecoverableErrror = "Error was unrecoverable"
	ReasonWithinPolicy        = "Restart within policy"
	ReasonDelay               = "Exceeded allowed attempts, applying a delay"
)

func newRestartTracker() *RestartTracker {
	onSuccess := true
	return &RestartTracker{
		startTime: time.Now(),
		onSuccess: onSuccess,
		rand:      rand.New(rand.NewSource(time.Now().Unix())),
	}
}

type RestartTracker struct {
	waitRes          *models.WaitResult
	startErr         error
	restartTriggered bool      // Whether the task has been signalled to be restarted
	count            int       // Current number of attempts.
	onSuccess        bool      // Whether to restart on successful exit code.
	startTime        time.Time // When the interval began
	reason           string    // The reason for the last store
	rand             *rand.Rand
	lock             sync.Mutex
}

// SetStartError is used to mark the most recent start error. If starting was
// successful the error should be nil.
func (r *RestartTracker) SetStartError(err error) *RestartTracker {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.startErr = err
	return r
}

// SetWaitResult is used to mark the most recent wait result.
func (r *RestartTracker) SetWaitResult(res *models.WaitResult) *RestartTracker {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.waitRes = res
	return r
}

// SetRestartTriggered is used to mark that the task has been signalled to be
// restarted
func (r *RestartTracker) SetRestartTriggered() *RestartTracker {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.restartTriggered = true
	return r
}

// GetReason returns a human-readable description for the last store returned by
// GetState.
func (r *RestartTracker) GetReason() string {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.reason
}

// GetState returns the tasks next store given the set exit code and start
// error. One of the following states are returned:
// * TaskRestarting - Task should be restarted
// * TaskNotRestarting - Task should not be restarted and has exceeded its
//   restart policy.
// * TaskTerminated - Task has terminated successfully and does not need a
//   restart.
//
// If TaskRestarting is returned, the duration is how long to wait until
// starting the task again.
func (r *RestartTracker) GetState() (string, time.Duration) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Clear out the existing store
	defer func() {
		r.startErr = nil
		r.waitRes = nil
		r.restartTriggered = false
	}()

	// Hot path if a restart was triggered
	if r.restartTriggered {
		r.reason = ""
		return models.TaskRestarting, 0
	}

	if r.waitRes != nil && !r.waitRes.ShouldRestart() {
		return models.TaskNotRestarting, 0
	}

	if r.waitRes != nil && r.waitRes.Successful() {
		return models.TaskTerminated, 0
	}

	r.count++

	// Check if we have entered a new interval.
	end := r.startTime.Add(1 * time.Minute)
	now := time.Now()
	if now.After(end) {
		r.count = 0
		r.startTime = now
	}

	if r.startErr != nil {
		return r.handleStartError()
	} else if r.waitRes != nil {
		return r.handleWaitResult()
	}

	return "", 0
}

// handleStartError returns the new store and potential wait duration for
// restarting the task after it was not successfully started. On start errors,
// the restart policy is always treated as fail mode to ensure we don't
// infinitely try to start a task.
func (r *RestartTracker) handleStartError() (string, time.Duration) {
	// If the error is not recoverable, do not restart.
	if !models.IsRecoverable(r.startErr) {
		r.reason = ReasonUnrecoverableErrror
		return models.TaskNotRestarting, 0
	}

	if r.count > 5 {
		r.reason = ReasonDelay
		return models.TaskRestarting, r.getDelay()
	}

	r.reason = ReasonWithinPolicy
	return models.TaskRestarting, r.jitter()
}

// handleWaitResult returns the new store and potential wait duration for
// restarting the task after it has exited.
func (r *RestartTracker) handleWaitResult() (string, time.Duration) {
	// If the task started successfully and restart on success isn't specified,
	// don't restart but don't mark as failed.
	if r.waitRes == nil && !r.onSuccess {
		r.reason = "Restart unnecessary as task terminated successfully"
		return models.TaskTerminated, 0
	}

	if r.count > 5 {
		r.reason = ReasonDelay
		return models.TaskRestarting, r.getDelay()
	}

	r.reason = ReasonWithinPolicy
	return models.TaskRestarting, r.jitter()
}

// getDelay returns the delay time to enter the next interval.
func (r *RestartTracker) getDelay() time.Duration {
	end := r.startTime.Add(1 * time.Minute)
	now := time.Now()
	return end.Sub(now)
}

// jitter returns the delay time plus a jitter.
func (r *RestartTracker) jitter() time.Duration {
	// Get the delay and ensure it is valid.
	d := 15 * time.Second.Nanoseconds()
	if d == 0 {
		d = 1
	}

	j := float64(r.rand.Int63n(d)) * jitter
	return time.Duration(d + int64(j))
}
