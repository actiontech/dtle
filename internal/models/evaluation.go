package models

import (
	"fmt"
	"time"
)

const (
	EvalStatusBlocked   = "blocked"
	EvalStatusPending   = "pending"
	EvalStatusComplete  = "complete"
	EvalStatusFailed    = "failed"
	EvalStatusCancelled = "canceled"
)

const (
	EvalTriggerJobRegister   = "job-register"
	EvalTriggerJobDeregister = "job-deregister"
	EvalTriggerJobPause      = "job-pause"
	EvalTriggerJobResume     = "job-resume"
	EvalTriggerNodeUpdate    = "node-update"
	EvalTriggerScheduled     = "scheduled"
	EvalTriggerRollingUpdate = "rolling-update"
	EvalTriggerMaxPlans      = "max-plan-attempts"
)

// Evaluation is used anytime we need to apply business logic as a result
// of a change to our desired store (job specification) or the emergent store
// (registered nodes). When the inputs change, we need to "evaluate" them,
// potentially taking action (allocation of work) or doing nothing if the state
// of the world does not require it.
type Evaluation struct {
	// ID is a randonly generated UUID used for this evaluation. This
	// is assigned upon the creation of the evaluation.
	ID string

	// Type is used to control which schedulers are available to handle
	// this evaluation.
	Type string

	// TriggeredBy is used to give some insight into why this Eval
	// was created. (Job change, node failure, alloc failure, etc).
	TriggeredBy string

	// JobID is the job this evaluation is scoped to. Evaluations cannot
	// be run in parallel for a given JobID, so we serialize on this.
	JobID string

	// JobModifyIndex is the modify index of the job at the time
	// the evaluation was created
	JobModifyIndex uint64

	// NodeID is the node that was affected triggering the evaluation.
	NodeID string

	// NodeModifyIndex is the modify index of the node at the time
	// the evaluation was created
	NodeModifyIndex uint64

	// Status of the evaluation
	Status string

	// StatusDescription is meant to provide more human useful information
	StatusDescription string

	// Wait is a minimum wait time for running the eval. This is used to
	// support a rolling upgrade.
	Wait time.Duration

	// NextEval is the evaluation ID for the eval created to do a followup.
	// This is used to support rolling upgrades, where we need a chain of evaluations.
	NextEval string

	// PreviousEval is the evaluation ID for the eval creating this one to do a followup.
	// This is used to support rolling upgrades, where we need a chain of evaluations.
	PreviousEval string

	// BlockedEval is the evaluation ID for a created blocked eval. A
	// blocked eval will be created if all allocations could not be placed due
	// to constraints or lacking resources.
	BlockedEval string

	// FailedTGAllocs are tasks which have allocations that could not be
	// made, but the metrics are persisted so that the user can use the feedback
	// to determine the cause.
	FailedTGAllocs map[string]*AllocMetric

	// ClassEligibility tracks computed node classes that have been explicitly
	// marked as eligible or ineligible.
	ClassEligibility map[string]bool

	// EscapedComputedClass marks whether the job has constraints that are not
	// captured by computed node classes.
	EscapedComputedClass bool

	// AnnotatePlan triggers the scheduler to provide additional annotations
	// during the evaluation. This should not be set during normal operations.
	AnnotatePlan bool

	// QueuedAllocations is the number of unplaced allocations at the time the
	// evaluation was processed. The map is keyed by Task names.
	QueuedAllocations map[string]int

	// SnapshotIndex is the Raft index of the snapshot used to process the
	// evaluation. As such it will only be set once it has gone through the
	// scheduler.
	SnapshotIndex uint64

	// Raft Indexes
	CreateIndex uint64
	ModifyIndex uint64
}

// TerminalStatus returns if the current status is terminal and
// will no longer transition.
func (e *Evaluation) TerminalStatus() bool {
	switch e.Status {
	case EvalStatusComplete, EvalStatusFailed, EvalStatusCancelled:
		return true
	default:
		return false
	}
}

func (e *Evaluation) GoString() string {
	return fmt.Sprintf("<Eval '%s' JobID: '%s'>", e.ID, e.JobID)
}

func (e *Evaluation) Copy() *Evaluation {
	if e == nil {
		return nil
	}
	ne := new(Evaluation)
	*ne = *e

	// Copy ClassEligibility
	if e.ClassEligibility != nil {
		classes := make(map[string]bool, len(e.ClassEligibility))
		for class, elig := range e.ClassEligibility {
			classes[class] = elig
		}
		ne.ClassEligibility = classes
	}

	// Copy FailedTGAllocs
	if e.FailedTGAllocs != nil {
		failedTGs := make(map[string]*AllocMetric, len(e.FailedTGAllocs))
		for tg, metric := range e.FailedTGAllocs {
			failedTGs[tg] = metric.Copy()
		}
		ne.FailedTGAllocs = failedTGs
	}

	// Copy queued allocations
	if e.QueuedAllocations != nil {
		queuedAllocations := make(map[string]int, len(e.QueuedAllocations))
		for tg, num := range e.QueuedAllocations {
			queuedAllocations[tg] = num
		}
		ne.QueuedAllocations = queuedAllocations
	}

	return ne
}

// ShouldEnqueue checks if a given evaluation should be enqueued into the
// eval_broker
func (e *Evaluation) ShouldEnqueue() bool {
	switch e.Status {
	case EvalStatusPending:
		return true
	case EvalStatusComplete, EvalStatusFailed, EvalStatusBlocked, EvalStatusCancelled:
		return false
	default:
		panic(fmt.Sprintf("unhandled evaluation (%s) status %s", e.ID, e.Status))
	}
}

// ShouldBlock checks if a given evaluation should be entered into the blocked
// eval tracker.
func (e *Evaluation) ShouldBlock() bool {
	switch e.Status {
	case EvalStatusBlocked:
		return true
	case EvalStatusComplete, EvalStatusFailed, EvalStatusPending, EvalStatusCancelled:
		return false
	default:
		panic(fmt.Sprintf("unhandled evaluation (%s) status %s", e.ID, e.Status))
	}
}

// MakePlan is used to make a plan from the given evaluation
// for a given Job
func (e *Evaluation) MakePlan(j *Job) *Plan {
	p := &Plan{
		EvalID:         e.ID,
		Job:            j,
		NodeUpdate:     make(map[string][]*Allocation),
		NodeAllocation: make(map[string][]*Allocation),
	}
	return p
}

// NextRollingEval creates an evaluation to followup this eval for rolling updates
func (e *Evaluation) NextRollingEval(wait time.Duration) *Evaluation {
	return &Evaluation{
		ID:             GenerateUUID(),
		Type:           e.Type,
		TriggeredBy:    EvalTriggerRollingUpdate,
		JobID:          e.JobID,
		JobModifyIndex: e.JobModifyIndex,
		Status:         EvalStatusPending,
		Wait:           wait,
		PreviousEval:   e.ID,
	}
}

// CreateBlockedEval creates a blocked evaluation to followup this eval to place any
// failed allocations. It takes the classes marked explicitly eligible or
// ineligible and whether the job has escaped computed node classes.
func (e *Evaluation) CreateBlockedEval(classEligibility map[string]bool, escaped bool) *Evaluation {
	return &Evaluation{
		ID:                   GenerateUUID(),
		Type:                 e.Type,
		TriggeredBy:          e.TriggeredBy,
		JobID:                e.JobID,
		JobModifyIndex:       e.JobModifyIndex,
		Status:               EvalStatusBlocked,
		PreviousEval:         e.ID,
		ClassEligibility:     classEligibility,
		EscapedComputedClass: escaped,
	}
}

// EvalUpdateRequest is used for upserting evaluations.
type EvalUpdateRequest struct {
	Evals     []*Evaluation
	EvalToken string
	WriteRequest
}

// EvalDeleteRequest is used for deleting an evaluation.
type EvalDeleteRequest struct {
	Evals  []string
	Allocs []string
	WriteRequest
}

// EvalSpecificRequest is used when we just need to specify a target evaluation
type EvalSpecificRequest struct {
	EvalID string
	QueryOptions
}

// EvalAckRequest is used to Ack/Nack a specific evaluation
type EvalAckRequest struct {
	EvalID string
	Token  string
	WriteRequest
}

// EvalDequeueRequest is used when we want to dequeue an evaluation
type EvalDequeueRequest struct {
	Schedulers       []string
	Timeout          time.Duration
	SchedulerVersion uint16
	WriteRequest
}

// EvalListRequest is used to list the evaluations
type EvalListRequest struct {
	QueryOptions
}

// JobEvaluationsResponse is used to return the evaluations for a job
type JobEvaluationsResponse struct {
	Evaluations []*Evaluation
	QueryMeta
}

// SingleEvalResponse is used to return a single evaluation
type SingleEvalResponse struct {
	Eval *Evaluation
	QueryMeta
}

// EvalDequeueResponse is used to return from a dequeue
type EvalDequeueResponse struct {
	Eval  *Evaluation
	Token string
	QueryMeta
}

// EvalListResponse is used for a list request
type EvalListResponse struct {
	Evaluations []*Evaluation
	QueryMeta
}

// EvalAllocationsResponse is used to return the allocations for an evaluation
type EvalAllocationsResponse struct {
	Allocations []*AllocListStub
	QueryMeta
}
