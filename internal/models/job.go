/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package models

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hashicorp/go-multierror"

	"github.com/actiontech/dts/internal"
)

const (
	JobTypeSync = "synchronous"
)

const (
	JobStatusPause    = "pause"    // Pause means the job is pause
	JobStatusPending  = "pending"  // Pending means the job is waiting on scheduling
	JobStatusRunning  = "running"  // Running means the job has non-terminal allocations
	JobStatusDead     = "dead"     // Dead means all evaluation's and allocations are terminal
	JobStatusComplete = "complete" // Complete means all evaluation's and allocations are terminal
)

func ValidJobStatus(status string) bool {
	switch status {
	case JobStatusPending, JobStatusRunning, JobStatusPause, JobStatusDead, JobStatusComplete:
		return true
	default:
		return false
	}
}

// Job is the scope of a scheduling request to Udup. It is the largest
// scoped object, and is a named collection of tasks. Each task
// is further composed of tasks. A task (T) is the unit of scheduling
// however.
type Job struct {
	// Region is the Udup region that handles scheduling this job
	Region string

	// ID is a unique identifier for the job per region. It can be
	// specified hierarchically like LineOfBiz/OrgName/Team/Project
	ID string

	Orders []string

	// Name is the logical name of the job used to refer to it. This is unique
	// per region, but not unique globally.
	Name string

	Failover bool

	// Type is used to control various behaviors about the job. Most jobs
	// are service jobs, meaning they are expected to be long lived.
	// Some jobs are batch oriented meaning they run and then terminate.
	// This can be extended in the future to support custom schedulers.
	Type string

	// Datacenters contains all the datacenters this job is allowed to span
	Datacenters []string

	// Constraints can be specified at a job level and apply to
	// all the tasks.
	Constraints []*Constraint

	// Tasks are the collections of tasks that this job needs
	// to run. Each task is an atomic unit of scheduling and placement.
	Tasks []*Task

	// Job status
	Status string
	// job typs
	WorkType string
	// StatusDescription is meant to provide more human useful information
	StatusDescription string

	EnforceIndex bool

	// Raft Indexes
	CreateIndex    uint64
	ModifyIndex    uint64
	JobModifyIndex uint64
}

// Canonicalize is used to canonicalize fields in the Job. This should be called
// when registering a Job.
func (j *Job) Canonicalize() {
	for _, t := range j.Tasks {
		t.Canonicalize(j)
	}
}

// Copy returns a deep copy of the Job. It is expected that callers use recover.
// This job can panic if the deep copy failed as it uses reflection.
func (j *Job) Copy() *Job {
	if j == nil {
		return nil
	}
	nj := new(Job)
	*nj = *j
	nj.Datacenters = internal.CopySliceString(nj.Datacenters)
	nj.Constraints = CopySliceConstraints(nj.Constraints)

	if j.Tasks != nil {
		ts := make([]*Task, len(nj.Tasks))
		for i, t := range nj.Tasks {
			ts[i] = t.Copy()
		}
		nj.Tasks = ts
	}

	return nj
}

// Validate is used to sanity check a job input
func (j *Job) Validate() error {
	var mErr multierror.Error

	if j.Region == "" {
		mErr.Errors = append(mErr.Errors, errors.New("Missing job region"))
	}
	if j.ID == "" {
		mErr.Errors = append(mErr.Errors, errors.New("Missing job ID"))
	} else if strings.Contains(j.ID, " ") {
		mErr.Errors = append(mErr.Errors, errors.New("Job ID contains a space"))
	}
	if j.Name == "" {
		mErr.Errors = append(mErr.Errors, errors.New("Missing job name"))
	}
	if j.Type == "" {
		mErr.Errors = append(mErr.Errors, errors.New("Missing job type"))
	}
	if len(j.Datacenters) == 0 {
		mErr.Errors = append(mErr.Errors, errors.New("Missing job datacenters"))
	}
	if len(j.Tasks) == 0 {
		mErr.Errors = append(mErr.Errors, errors.New("Missing job tasks"))
	}
	for idx, constr := range j.Constraints {
		if err := constr.Validate(); err != nil {
			outer := fmt.Errorf("Constraint %d validation failed: %s", idx+1, err)
			mErr.Errors = append(mErr.Errors, outer)
		}
	}

	// Check for duplicate tasks
	tasks := make(map[string]int)
	for idx, t := range j.Tasks {
		if t.Type == "" {
			mErr.Errors = append(mErr.Errors, fmt.Errorf("Job task %d missing type", idx+1))
		} else if existing, ok := tasks[t.Type]; ok {
			mErr.Errors = append(mErr.Errors, fmt.Errorf("Job task %d redefines '%s' from task %d", idx+1, t.Type, existing+1))
		} else {
			tasks[t.Type] = idx
		}
	}

	// Validate the task
	for _, t := range j.Tasks {
		if err := t.Validate(); err != nil {
			outer := fmt.Errorf("Task %s validation failed: %v", t.Type, err)
			mErr.Errors = append(mErr.Errors, outer)
		}
	}

	return mErr.ErrorOrNil()
}

// LookupTask finds a task by name
func (j *Job) LookupTask(tp string) *Task {
	for _, t := range j.Tasks {
		if t.Type == tp {
			return t
		}
	}
	return nil
}

// Stub is used to return a summary of the job
func (j *Job) Stub(job *Job) *JobListStub {
	return &JobListStub{
		ID:                j.ID,
		Name:              j.Name,
		Type:              j.Type,
		WorkType:          j.WorkType,
		Status:            j.Status,
		StatusDescription: j.StatusDescription,
		CreateIndex:       j.CreateIndex,
		ModifyIndex:       j.ModifyIndex,
		JobModifyIndex:    j.JobModifyIndex,
		JobSummary:        job,
	}
}

// JobListStub is used to return a subset of job information
// for the job list
type JobListStub struct {
	ID                string
	Name              string
	Type              string
	WorkType          string
	Status            string
	StatusDescription string
	JobSummary        *Job
	CreateIndex       uint64
	ModifyIndex       uint64
	JobModifyIndex    uint64
}

type JobResponse struct {
	Success bool
	QueryMeta
}

// SingleJobResponse is used to return a single job
type SingleJobResponse struct {
	Job *Job
	QueryMeta
}

// JobListResponse is used for a list request
type JobListResponse struct {
	Jobs []*JobListStub
	QueryMeta
}

type JobUpdateStatusRequest struct {
	JobID  string
	Status string
	WriteRequest
}

// JobPlanResponse is used to respond to a job plan request
type JobPlanResponse struct {
	// Annotations stores annotations explaining decisions the scheduler made.
	Annotations *PlanAnnotations

	// FailedTGAllocs is the placement failures per task.
	FailedTGAllocs map[string]*AllocMetric

	// JobModifyIndex is the modification index of the job. The value can be
	// used when running `server run` to ensure that the Job wasn’t modified
	// since the last plan. If the job is being created, the value is zero.
	JobModifyIndex uint64

	// CreatedEvals is the set of evaluations created by the scheduler. The
	// reasons for this can be rolling-updates or blocked evals.
	CreatedEvals []*Evaluation

	WriteMeta
}

// JobRegisterRequest is used for Job.Register endpoint
// to register a job as being a schedulable entity.
type JobRegisterRequest struct {
	Job *Job

	// If EnforceIndex is set then the job will only be registered if the passed
	// JobModifyIndex matches the current Jobs index. If the index is zero, the
	// register only occurs if the job is new.
	EnforceIndex   bool
	JobModifyIndex uint64

	WriteRequest
}

type JobRenewalRequest struct {
	JobID   string
	OrderID string

	WriteRequest
}

type JobUpdateRequest struct {
	// Alloc is the list of new allocations to assign
	JobUpdates []*TaskUpdate

	WriteRequest
}

// JobDeregisterRequest is used for Job.Deregister endpoint
// to deregister a job as being a schedulable entity.
type JobDeregisterRequest struct {
	JobID string
	WriteRequest
}

// JobEvaluateRequest is used when we just need to re-evaluate a target job
type JobEvaluateRequest struct {
	JobID string
	WriteRequest
}

// JobSpecificRequest is used when we just need to specify a target job
type JobSpecificRequest struct {
	JobID     string
	AllAllocs bool
	QueryOptions
}

// JobListRequest is used to parameterize a list request
type JobListRequest struct {
	QueryOptions
}

// JobPlanRequest is used for the Job.Plan endpoint to trigger a dry-run
// evaluation of the Job.
type JobPlanRequest struct {
	Job *Job
	//Diff bool // Toggles an annotated diff
	WriteRequest
}

// JobSummaryRequest is used when we just need to get a specific job summary
type JobSummaryRequest struct {
	JobID string
	QueryOptions
}

// JobValidateRequest is used to validate a job
type JobValidateRequest struct {
	Job *Job
	WriteRequest
}

// JobValidateResponse is the response from validate request
type JobValidateResponse struct {
	// DriverConfigValidated indicates whether the agent validated the driver
	// config
	DriverConfigValidated bool

	// ValidationErrors is a list of validation errors
	ValidationTasks []*TaskValidateResponse

	Error string
}

type TaskValidateResponse struct {
	Type string

	Connection ConnectionValidate

	LogSlaveUpdates LogSlaveUpdatesValidate

	MaxAllowedPacket MaxAllowedPacket

	Privileges PrivilegesValidate

	GtidMode GtidModeValidate

	ServerID ServerIDValidate

	Binlog BinlogValidate
}

type BinlogValidate struct {
	Success bool
	// Error is a string version of any error that may have occured
	Error string
}

type GtidModeValidate struct {
	Success bool
	// Error is a string version of any error that may have occured
	Error string
}

type ServerIDValidate struct {
	Success bool
	// Error is a string version of any error that may have occured
	Error string
}

type PrivilegesValidate struct {
	Success bool
	// Error is a string version of any error that may have occured
	Error string
}

type ConnectionValidate struct {
	Success bool
	// Error is a string version of any error that may have occured
	Error string
}

type LogSlaveUpdatesValidate struct {
	Success bool
	// Error is a string version of any error that may have occured
	Error string
}

type MaxAllowedPacket struct {
	Success bool
	// Error is a string version of any error that may have occured
	Error string
}
