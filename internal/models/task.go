/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package models

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/mitchellh/copystructure"
	"sync"
)

const (
	TaskTypeSrc  = "Src"
	TaskTypeDest = "Dest"

	TaskDriverMySQL  = "MySQL"
	TaskDriverOracle = "Oracle"
)

// Task is a single process typically that is executed as part of a task.
type Task struct {
	// Type of the task
	Type string

	NodeID string

	NodeName string

	// Driver is used to control which driver is used
	Driver string

	// Config is provided to the driver to initialize
	Config     map[string]interface{}
	ConfigLock *sync.RWMutex

	// Leader marks the task as the leader within the group. When the leader
	// task exits, other tasks will be gracefully terminated.
	Leader bool

	// Constraints can be specified at a task group level and apply to
	// all the tasks contained.
	Constraints []*Constraint
}

func NewTask() *Task {
	return &Task{
		ConfigLock: &sync.RWMutex{},
	}
}
func (t *Task) Copy() *Task {
	if t == nil {
		return nil
	}

	if t.ConfigLock == nil { // migrating old data
		t.ConfigLock = &sync.RWMutex{}
	}

	nt := new(Task)
	*nt = *t

	nt.ConfigLock.RLock()
	defer nt.ConfigLock.RUnlock()
	if i, err := copystructure.Copy(nt.Config); err != nil {
		nt.Config = i.(map[string]interface{})
		nt.ConfigLock = &sync.RWMutex{} // a lock per map, and a new map created.
	}

	return nt
}

// Canonicalize canonicalizes fields in the task.
func (t *Task) Canonicalize(job *Job) {
	if len(t.Config) == 0 {
		t.Config = nil
	}
}

func (t *Task) GoString() string {
	return fmt.Sprintf("*%#v", *t)
}

// Validate is used to sanity check a task
func (t *Task) Validate() error {
	var mErr multierror.Error
	if t.Type == "" {
		mErr.Errors = append(mErr.Errors, errors.New("Missing task type"))
	}
	if strings.ContainsAny(t.Type, `/\`) {
		// We enforce this so that when creating the directory on disk it will
		// not have any slashes.
		mErr.Errors = append(mErr.Errors, errors.New("Task name cannot include slashes"))
	}
	if t.Driver == "" {
		mErr.Errors = append(mErr.Errors, errors.New("Missing task driver"))
	}

	return mErr.ErrorOrNil()
}

// Set of possible states for a task.
const (
	TaskStatePending  = "pending" // The task is waiting to be run.
	TaskStateRunning  = "running" // The task is currently running.
	TaskStateDead     = "dead"    // Terminal state of task.
	TaskStateStop     = "stop"
	TaskStateQueued   = "queued"
	TaskStateComplete = "complete"
	TaskStateFailed   = "failed"
	TaskStateStarting = "starting"
	TaskStateLost     = "lost"
)

// TaskState tracks the current store of a task and events that caused store
// transitions.
type TaskState struct {
	// The current state of the task.
	State string

	// Failed marks a task as having failed
	Failed bool

	// StartedAt is the time the task is started. It is updated each time the
	// task starts
	StartedAt time.Time

	// FinishedAt is the time at which the task transistioned to dead and will
	// not be started again.
	FinishedAt time.Time

	// Series of task events that transition the state of the task.
	Events []*TaskEvent
}

func (ts *TaskState) Copy() *TaskState {
	if ts == nil {
		return nil
	}
	copy := new(TaskState)
	copy.State = ts.State
	copy.Failed = ts.Failed
	copy.StartedAt = ts.StartedAt
	copy.FinishedAt = ts.FinishedAt

	if ts.Events != nil {
		copy.Events = make([]*TaskEvent, len(ts.Events))
		for i, e := range ts.Events {
			copy.Events[i] = e.Copy()
		}
	}
	return copy
}

// Successful returns whether a task finished successfully.
func (ts *TaskState) Successful() bool {
	l := len(ts.Events)
	if ts.State != TaskStateDead || l == 0 {
		return false
	}

	e := ts.Events[l-1]
	if e.Type != TaskTerminated {
		return false
	}

	return e.ExitCode == 0
}

const (
	// TaskSetupFailure indicates that the task could not be started due to a
	// a setup failure.
	TaskSetupFailure = "Setup Failure"

	// TaskDriveFailure indicates that the task could not be started due to a
	// failure in the driver.
	TaskDriverFailure = "Driver Failure"

	// TaskReceived signals that the task has been pulled by the client at the
	// given timestamp.
	TaskReceived = "Received"

	// TaskStarted signals that the task was started and its timestamp can be
	// used to determine the running length of the task.
	TaskStarted = "Started"

	// TaskTerminated indicates that the task was started and exited.
	TaskTerminated = "Terminated"

	// TaskKilling indicates a kill signal has been sent to the task.
	TaskKilling = "Killing"

	// TaskKilled indicates a user has killed the task.
	TaskKilled = "Killed"

	// TaskRestarting indicates that task terminated and is being restarted.
	TaskRestarting = "Restarting"

	// TaskNotRestarting indicates that the task has failed and is not being
	// restarted because it has exceeded its restart policy.
	TaskNotRestarting = "Not Restarting"

	// TaskRestartSignal indicates that the task has been signalled to be
	// restarted
	TaskRestartSignal = "Restart Signaled"

	// TaskSiblingFailed indicates that a sibling task in the task has
	// failed.
	TaskSiblingFailed = "Sibling Task Failed"

	// TaskLeaderDead indicates that the leader task within the has finished.
	TaskLeaderDead = "Leader Task Dead"
)

// TaskEvent is an event that effects the state of a task and contains meta-data
// appropriate to the events type.
type TaskEvent struct {
	Type string
	Time time.Time // Unix timestamp

	// FailsTask marks whether this event fails the task
	FailsTask bool

	// Restart fields.
	RestartReason string

	// Setup Failure fields.
	SetupError string

	// Driver Failure fields.
	DriverError string // A driver error occurred while starting the task.

	// Task Terminated Fields.
	ExitCode int    // The exit code of the task.
	Message  string // A possible message explaining the termination of the task.

	// Killing fields
	KillTimeout time.Duration

	// Task Killed Fields.
	KillError string // Error killing the task.

	// KillReason is the reason the task was killed
	KillReason string

	// TaskRestarting fields.
	StartDelay int64 // The sleep period before restarting the task in unix nanoseconds.

	// The maximum allowed task disk size.
	DiskLimit int64

	// Name of the sibling task that caused termination of the task that
	// the TaskEvent refers to.
	FailedSibling string

	// TaskSignalReason indicates the reason the task is being signalled.
	TaskSignalReason string

	// TaskSignal is the signal that was sent to the task
	TaskSignal string

	// DriverMessage indicates a driver action being taken.
	DriverMessage string
}

func (te *TaskEvent) GoString() string {
	return fmt.Sprintf("%v at %v", te.Type, te.Time)
}

// SetMessage sets the message of TaskEvent
func (te *TaskEvent) SetMessage(msg string) *TaskEvent {
	te.Message = msg
	return te
}

func (te *TaskEvent) Copy() *TaskEvent {
	if te == nil {
		return nil
	}
	copy := new(TaskEvent)
	*copy = *te
	return copy
}

func NewTaskEvent(event string) *TaskEvent {
	return &TaskEvent{
		Type: event,
		Time: time.Now(),
	}
}

// SetSetupError is used to store an error that occured while setting up the
// task
func (e *TaskEvent) SetSetupError(err error) *TaskEvent {
	if err != nil {
		e.SetupError = err.Error()
	}
	return e
}

func (e *TaskEvent) SetFailsTask() *TaskEvent {
	e.FailsTask = true
	return e
}

func (e *TaskEvent) SetDriverError(err error) *TaskEvent {
	if err != nil {
		e.DriverError = err.Error()
	}
	return e
}

func (e *TaskEvent) SetExitCode(c int) *TaskEvent {
	e.ExitCode = c
	return e
}

func (e *TaskEvent) SetExitMessage(err error) *TaskEvent {
	if err != nil {
		e.Message = err.Error()
	}
	return e
}

func (e *TaskEvent) SetKillError(err error) *TaskEvent {
	if err != nil {
		e.KillError = err.Error()
	}
	return e
}

func (e *TaskEvent) SetKillReason(r string) *TaskEvent {
	e.KillReason = r
	return e
}

func (e *TaskEvent) SetRestartDelay(delay time.Duration) *TaskEvent {
	e.StartDelay = int64(delay)
	return e
}

func (e *TaskEvent) SetRestartReason(reason string) *TaskEvent {
	e.RestartReason = reason
	return e
}

func (e *TaskEvent) SetTaskSignalReason(r string) *TaskEvent {
	e.TaskSignalReason = r
	return e
}

func (e *TaskEvent) SetTaskSignal(s os.Signal) *TaskEvent {
	e.TaskSignal = s.String()
	return e
}

func (e *TaskEvent) SetKillTimeout(timeout time.Duration) *TaskEvent {
	e.KillTimeout = timeout
	return e
}

func (e *TaskEvent) SetDiskLimit(limit int64) *TaskEvent {
	e.DiskLimit = limit
	return e
}

func (e *TaskEvent) SetFailedSibling(sibling string) *TaskEvent {
	e.FailedSibling = sibling
	return e
}

func (e *TaskEvent) SetDriverMessage(m string) *TaskEvent {
	e.DriverMessage = m
	return e
}

type TaskUpdate struct {
	JobID    string
	Gtid     string
	NatsAddr string
}

const (
	// DefaultKillTimeout is the default timeout between signaling a task it
	// will be killed and killing it.
	DefaultKillTimeout = 5 * time.Second
)

// WaitResult stores the result of a Wait operation.
type WaitResult struct {
	ExitCode int
	Err      error
}

func NewWaitResult(code int, err error) *WaitResult {
	return &WaitResult{
		ExitCode: code,
		Err:      err,
	}
}

func (r *WaitResult) Successful() bool {
	return r.ExitCode == 0 && r.Err == nil
}

func (r *WaitResult) ShouldRestart() bool {
	return r.ExitCode == 1 && r.Err != nil
}

func (r *WaitResult) String() string {
	return fmt.Sprintf("Wait returned exit code %v, and error %v",
		r.ExitCode, r.Err)
}
