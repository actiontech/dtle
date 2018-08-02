/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package server

import (
	"fmt"
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-memdb"

	"udup/internal/client/driver"
	"udup/internal/models"
	"udup/internal/server/scheduler"
	"udup/internal/server/store"

	"github.com/mitchellh/copystructure"
)

const (
	// RegisterEnforceIndexErrPrefix is the prefix to use in errors caused by
	// enforcing the job modify index during registers.
	RegisterEnforceIndexErrPrefix = "Enforcing job modify index"
)

// Job endpoint is used for job interactions
type Job struct {
	srv *Server
}

// Register is used to upsert a job for scheduling
func (j *Job) Register(args *models.JobRegisterRequest, reply *models.JobResponse) error {
	if done, err := j.srv.forward("Job.Register", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "job", "register"}, time.Now())

	// Validate the arguments
	if args.Job == nil {
		reply.Success = false
		return fmt.Errorf("missing job for registration")
	}

	// Initialize the job fields (sets defaults and any necessary init work).
	args.Job.Canonicalize()

	// Validate the job.
	/*if err := validateJob(args.Job); err != nil {
		reply.Success = false
		return err
	}*/

	if args.EnforceIndex {
		// Lookup the job
		snap, err := j.srv.fsm.State().Snapshot()
		if err != nil {
			reply.Success = false
			return err
		}
		ws := memdb.NewWatchSet()
		job, err := snap.JobByID(ws, args.Job.ID)
		if err != nil {
			reply.Success = false
			return err
		}
		jmi := args.JobModifyIndex
		if job != nil {
			if jmi == 0 {
				reply.Success = false
				return fmt.Errorf("%s 0: job already exists", RegisterEnforceIndexErrPrefix)
			} else if jmi != job.JobModifyIndex {
				reply.Success = false
				return fmt.Errorf("%s %d: job exists with conflicting job modify index: %d",
					RegisterEnforceIndexErrPrefix, jmi, job.JobModifyIndex)
			}
		} else if jmi != 0 {
			reply.Success = false
			return fmt.Errorf("%s %d: job does not exist", RegisterEnforceIndexErrPrefix, jmi)
		}
	}

	// Commit this update via Raft
	_, index, err := j.srv.raftApply(models.JobRegisterRequestType, args)
	if err != nil {
		j.srv.logger.Errorf("server.job: Register failed: %v", err)
		reply.Success = false
		return err
	}

	// Create a new evaluation
	eval := &models.Evaluation{
		ID:             models.GenerateUUID(),
		Type:           args.Job.Type,
		TriggeredBy:    models.EvalTriggerJobRegister,
		JobID:          args.Job.ID,
		JobModifyIndex: index,
		Status:         models.EvalStatusPending,
	}
	update := &models.EvalUpdateRequest{
		Evals:        []*models.Evaluation{eval},
		WriteRequest: models.WriteRequest{Region: args.Region},
	}

	// Commit this evaluation via Raft
	// XXX: There is a risk of partial failure where the JobRegister succeeds
	// but that the EvalUpdate does not.
	_, evalIndex, err := j.srv.raftApply(models.EvalUpdateRequestType, update)
	if err != nil {
		j.srv.logger.Errorf("server.job: Eval create failed: %v", err)
		reply.Success = false
		return err
	}

	// Populate the reply with eval information
	reply.Success = true
	reply.Index = evalIndex
	return nil
}

func (j *Job) Renewal(args *models.JobRenewalRequest, reply *models.JobResponse) error {
	if done, err := j.srv.forward("Job.Renewal", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "job", "renewal"}, time.Now())

	// Lookup the job
	snap, err := j.srv.fsm.State().Snapshot()
	if err != nil {
		reply.Success = false
		return err
	}
	ws := memdb.NewWatchSet()
	job, err := snap.JobByID(ws, args.JobID)
	if err != nil {
		reply.Success = false
		return err
	}
	if job == nil {
		reply.Success = false
		return fmt.Errorf("%s : job does not exist", RegisterEnforceIndexErrPrefix)
	}

	// Commit this update via Raft
	_, index, err := j.srv.raftApply(models.JobRenewalRequestType, args)
	if err != nil {
		j.srv.logger.Errorf("server.job: Renewal failed: %v", err)
		reply.Success = false
		return err
	}

	// Populate the reply with eval information
	reply.Success = true
	reply.Index = index
	return nil
}

// UpdateStatus is used to update the status of a client node
func (j *Job) UpdateStatus(args *models.JobUpdateStatusRequest, reply *models.JobResponse) error {
	if done, err := j.srv.forward("Job.UpdateStatus", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "job", "update_status"}, time.Now())

	// Verify the arguments
	if args.JobID == "" {
		reply.Success = false
		return fmt.Errorf("missing job ID for client status update")
	}
	if !models.ValidJobStatus(args.Status) {
		reply.Success = false
		return fmt.Errorf("invalid status for job")
	}

	// Look for the job
	snap, err := j.srv.fsm.State().Snapshot()
	if err != nil {
		reply.Success = false
		return err
	}

	ws := memdb.NewWatchSet()
	job, err := snap.JobByID(ws, args.JobID)
	if err != nil {
		reply.Success = false
		return err
	}
	if job == nil {
		reply.Success = false
		return fmt.Errorf("job not found")
	}
	// Commit this update via Raft
	if job.Status != args.Status {
		_, index, err := j.srv.raftApply(models.JobUpdateStatusRequestType, args)
		if err != nil {
			j.srv.logger.Errorf("server.job: status update failed: %v", err)
			reply.Success = false
			return err
		}
		var triggeredBy string
		if args.Status == models.JobStatusPause {
			triggeredBy = models.EvalTriggerJobPause
		} else {
			triggeredBy = models.EvalTriggerJobResume
		}
		// Create a new evaluation
		eval := &models.Evaluation{
			ID:             models.GenerateUUID(),
			Type:           job.Type,
			TriggeredBy:    triggeredBy,
			JobID:          args.JobID,
			JobModifyIndex: index,
			Status:         models.EvalStatusPending,
		}
		update := &models.EvalUpdateRequest{
			Evals:        []*models.Evaluation{eval},
			WriteRequest: models.WriteRequest{Region: args.Region},
		}

		// Commit this evaluation via Raft
		// XXX: There is a risk of partial failure where the JobRegister succeeds
		// but that the EvalUpdate does not.
		_, evalIndex, err := j.srv.raftApply(models.EvalUpdateRequestType, update)
		if err != nil {
			j.srv.logger.Errorf("server.job: Eval create failed: %v", err)
			reply.Success = false
			return err
		}

		// Populate the reply with eval information
		reply.Success = true
		reply.Index = evalIndex
	}

	return nil
}

// Validate validates a job
func (j *Job) Validate(args *models.JobValidateRequest,
	reply *models.JobValidateResponse) error {
	if done, err := j.srv.forward("Job.Validate", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"udup", "job", "validate"}, time.Now())

	// validateJob validates a Job and task drivers and returns an error if there is
	// a validation problem or if the Job is of a type a user is not allowed to
	// submit.
	if err := args.Job.Validate(); err != nil {
		return err
	}

	// Validate the driver configurations.
	for _, task := range args.Job.Tasks {
		d, err := driver.NewDriver(
			task.Driver,
			driver.NewEmptyDriverContext(),
		)
		if err != nil {
			msg := "failed to create driver for task %q for validation: %v"
			return fmt.Errorf(msg, task.Type, err)
		}

		rep, err := d.Validate(task)
		if err != nil {
			return fmt.Errorf("task %q -> config: %v", task.Type, err)
		}
		rep.Type = task.Type
		reply.ValidationTasks = append(reply.ValidationTasks, rep)
	}
	reply.DriverConfigValidated = true
	return nil
}

// Evaluate is used to force a job for re-evaluation
func (j *Job) Evaluate(args *models.JobEvaluateRequest, reply *models.JobResponse) error {
	if done, err := j.srv.forward("Job.Evaluate", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "job", "evaluate"}, time.Now())

	// Validate the arguments
	if args.JobID == "" {
		reply.Success = false
		return fmt.Errorf("missing job ID for evaluation")
	}

	// Lookup the job
	snap, err := j.srv.fsm.State().Snapshot()
	if err != nil {
		reply.Success = false
		return err
	}
	ws := memdb.NewWatchSet()
	job, err := snap.JobByID(ws, args.JobID)
	if err != nil {
		reply.Success = false
		return err
	}
	if job == nil {
		reply.Success = false
		return fmt.Errorf("job not found")
	}

	// Create a new evaluation
	eval := &models.Evaluation{
		ID:             models.GenerateUUID(),
		Type:           job.Type,
		TriggeredBy:    models.EvalTriggerJobRegister,
		JobID:          job.ID,
		JobModifyIndex: job.ModifyIndex,
		Status:         models.EvalStatusPending,
	}
	update := &models.EvalUpdateRequest{
		Evals:        []*models.Evaluation{eval},
		WriteRequest: models.WriteRequest{Region: args.Region},
	}

	// Commit this evaluation via Raft
	_, evalIndex, err := j.srv.raftApply(models.EvalUpdateRequestType, update)
	if err != nil {
		j.srv.logger.Errorf("server.job: Eval create failed: %v", err)
		reply.Success = false
		return err
	}

	// Setup the reply
	reply.Success = true
	reply.Index = evalIndex
	return nil
}

// Deregister is used to remove a job the cluster.
func (j *Job) Deregister(args *models.JobDeregisterRequest, reply *models.JobResponse) error {
	if done, err := j.srv.forward("Job.Deregister", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "job", "deregister"}, time.Now())

	// Validate the arguments
	if args.JobID == "" {
		reply.Success = false
		return fmt.Errorf("missing job ID for evaluation")
	}

	// Commit this update via Raft
	_, index, err := j.srv.raftApply(models.JobDeregisterRequestType, args)
	if err != nil {
		j.srv.logger.Errorf("server.job: Deregister failed: %v", err)
		reply.Success = false
		return err
	}

	// Create a new evaluation
	// XXX: The job priority / type is strange for this, since it's not a high
	// priority even if the job was. The scheduler itself also doesn't matter,
	// since all should be able to handle deregistration in the same way.
	eval := &models.Evaluation{
		ID:             models.GenerateUUID(),
		Type:           models.JobTypeSync,
		TriggeredBy:    models.EvalTriggerJobDeregister,
		JobID:          args.JobID,
		JobModifyIndex: index,
		Status:         models.EvalStatusPending,
	}
	update := &models.EvalUpdateRequest{
		Evals:        []*models.Evaluation{eval},
		WriteRequest: models.WriteRequest{Region: args.Region},
	}

	// Commit this evaluation via Raft
	_, evalIndex, err := j.srv.raftApply(models.EvalUpdateRequestType, update)
	if err != nil {
		j.srv.logger.Errorf("server.job: Eval create failed: %v", err)
		reply.Success = false
		return err
	}

	// Populate the reply with eval information
	reply.Success = true
	reply.Index = evalIndex
	return nil
}

// GetJob is used to request information about a specific job
func (j *Job) GetJob(args *models.JobSpecificRequest,
	reply *models.SingleJobResponse) error {
	if done, err := j.srv.forward("Job.GetJob", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "job", "get_job"}, time.Now())

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Look for the job
			out, err := state.JobByID(ws, args.JobID)
			if err != nil {
				return err
			}

			// Setup the output
			reply.Job = out
			if out != nil {
				reply.Index = out.ModifyIndex
			} else {
				// Use the last index that affected the nodes table
				index, err := state.Index("jobs")
				if err != nil {
					return err
				}
				reply.Index = index
			}

			// Set the query response
			j.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return j.srv.blockingRPC(&opts)
}

// List is used to list the jobs registered in the system
func (j *Job) List(args *models.JobListRequest,
	reply *models.JobListResponse) error {
	if done, err := j.srv.forward("Job.List", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "job", "list"}, time.Now())

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Capture all the jobs
			var err error
			var iter memdb.ResultIterator
			if prefix := args.QueryOptions.Prefix; prefix != "" {
				iter, err = state.JobsByIDPrefix(ws, prefix)
			} else {
				iter, err = state.Jobs(ws)
			}
			if err != nil {
				return err
			}

			var jobs []*models.JobListStub
			for {
				raw := iter.Next()
				if raw == nil {
					break
				}
				job := raw.(*models.Job)
				jobCopy0, err := copystructure.Copy(job)
				if err != nil {
					return err
				}
				jobCopy, ok := jobCopy0.(*models.Job)
				if !ok {
					return fmt.Errorf("failed to deep copy job")
				}
				//fmt.Printf("**** mask password\n")
				for _, t := range jobCopy.Tasks {
					if connCfg, ok := t.Config["ConnectionConfig"]; ok {
						if connCfgMap, ok := connCfg.(map[string]interface{}); ok {
							connCfgMap["Password"] = "*"
						}
					}
				}
				jobs = append(jobs, job.Stub(jobCopy))
			}
			reply.Jobs = jobs

			// Use the last index that affected the jobs table
			index, err := state.Index("jobs")
			if err != nil {
				return err
			}
			reply.Index = index

			// Set the query response
			j.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return j.srv.blockingRPC(&opts)
}

// Allocations is used to list the allocations for a job
func (j *Job) Allocations(args *models.JobSpecificRequest,
	reply *models.JobAllocationsResponse) error {
	if done, err := j.srv.forward("Job.Allocations", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "job", "allocations"}, time.Now())

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Capture the allocations
			allocs, err := state.AllocsByJob(ws, args.JobID, args.AllAllocs)
			if err != nil {
				return err
			}

			// Convert to stubs
			if len(allocs) > 0 {
				reply.Allocations = make([]*models.AllocListStub, 0, len(allocs))
				for _, alloc := range allocs {
					reply.Allocations = append(reply.Allocations, alloc.Stub())
				}
			}

			// Use the last index that affected the allocs table
			index, err := state.Index("allocs")
			if err != nil {
				return err
			}
			reply.Index = index

			// Set the query response
			j.srv.setQueryMeta(&reply.QueryMeta)
			return nil

		}}
	return j.srv.blockingRPC(&opts)
}

// Evaluations is used to list the evaluations for a job
func (j *Job) Evaluations(args *models.JobSpecificRequest,
	reply *models.JobEvaluationsResponse) error {
	if done, err := j.srv.forward("Job.Evaluations", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "job", "evaluations"}, time.Now())

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Capture the evals
			var err error
			reply.Evaluations, err = state.EvalsByJob(ws, args.JobID)
			if err != nil {
				return err
			}

			// Use the last index that affected the evals table
			index, err := state.Index("evals")
			if err != nil {
				return err
			}
			reply.Index = index

			// Set the query response
			j.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}

	return j.srv.blockingRPC(&opts)
}

// Plan is used to cause a dry-run evaluation of the Job and return the results
// with a potential diff containing annotations.
func (j *Job) Plan(args *models.JobPlanRequest, reply *models.JobPlanResponse) error {
	if done, err := j.srv.forward("Job.Plan", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "job", "plan"}, time.Now())

	// Validate the arguments
	if args.Job == nil {
		return fmt.Errorf("Job required for plan")
	}

	// Initialize the job fields (sets defaults and any necessary init work).
	args.Job.Canonicalize()

	// Validate the job.
	/*if err := validateJob(args.Job); err != nil {
		return err
	}*/

	// Acquire a snapshot of the store
	snap, err := j.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}

	// Get the original job
	ws := memdb.NewWatchSet()
	oldJob, err := snap.JobByID(ws, args.Job.ID)
	if err != nil {
		return err
	}

	var index uint64
	var updatedIndex uint64
	if oldJob != nil {
		index = oldJob.JobModifyIndex
		updatedIndex = oldJob.JobModifyIndex + 1
	}

	// Insert the updated Job into the snapshot
	snap.UpsertJob(updatedIndex, args.Job)

	// Create an eval and mark it as requiring annotations and insert that as well
	eval := &models.Evaluation{
		ID:             models.GenerateUUID(),
		Type:           args.Job.Type,
		TriggeredBy:    models.EvalTriggerJobRegister,
		JobID:          args.Job.ID,
		JobModifyIndex: updatedIndex,
		Status:         models.EvalStatusPending,
		AnnotatePlan:   true,
	}

	// Create an in-memory Planner that returns no errors and stores the
	// submitted plan and created evals.
	planner := &scheduler.Harness{
		State: &snap.StateStore,
	}

	// Create the scheduler and run it
	sched, err := scheduler.NewScheduler(eval.Type, j.srv.logger, snap, planner)
	if err != nil {
		return err
	}

	if err := sched.Process(eval); err != nil {
		return err
	}

	// Annotate and store the diff
	if plans := len(planner.Plans); plans != 1 {
		return fmt.Errorf("scheduler resulted in an unexpected number of plans: %v", plans)
	}
	annotations := planner.Plans[0].Annotations

	// Grab the failures
	if len(planner.Evals) != 1 {
		return fmt.Errorf("scheduler resulted in an unexpected number of eval updates: %v", planner.Evals)
	}
	updatedEval := planner.Evals[0]

	reply.FailedTGAllocs = updatedEval.FailedTGAllocs
	reply.JobModifyIndex = index
	reply.Annotations = annotations
	reply.CreatedEvals = planner.CreateEvals
	reply.Index = index
	return nil
}
