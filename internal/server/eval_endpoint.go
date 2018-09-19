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

	"github.com/actiontech/udup/internal/models"
	"github.com/actiontech/udup/internal/server/scheduler"
	"github.com/actiontech/udup/internal/server/store"
)

const (
	// DefaultDequeueTimeout is used if no dequeue timeout is provided
	DefaultDequeueTimeout = time.Second
)

// Eval endpoint is used for eval interactions
type Eval struct {
	srv *Server
}

// GetEval is used to request information about a specific evaluation
func (e *Eval) GetEval(args *models.EvalSpecificRequest,
	reply *models.SingleEvalResponse) error {
	if done, err := e.srv.forward("Eval.GetEval", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "eval", "get_eval"}, time.Now())

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Look for the job
			out, err := state.EvalByID(ws, args.EvalID)
			if err != nil {
				return err
			}

			// Setup the output
			reply.Eval = out
			if out != nil {
				reply.Index = out.ModifyIndex
			} else {
				// Use the last index that affected the nodes table
				index, err := state.Index("evals")
				if err != nil {
					return err
				}
				reply.Index = index
			}

			// Set the query response
			e.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return e.srv.blockingRPC(&opts)
}

// Dequeue is used to dequeue a pending evaluation
func (e *Eval) Dequeue(args *models.EvalDequeueRequest,
	reply *models.EvalDequeueResponse) error {
	if done, err := e.srv.forward("Eval.Dequeue", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "eval", "dequeue"}, time.Now())

	// Ensure there is at least one scheduler
	if len(args.Schedulers) == 0 {
		return fmt.Errorf("dequeue requires at least one scheduler type")
	}

	// Check that there isn't a scheduler version mismatch
	if args.SchedulerVersion != scheduler.SchedulerVersion {
		return fmt.Errorf("dequeue disallowed: calling scheduler version is %d; leader version is %d",
			args.SchedulerVersion, scheduler.SchedulerVersion)
	}

	// Ensure there is a default timeout
	if args.Timeout <= 0 {
		args.Timeout = DefaultDequeueTimeout
	}

	// Attempt the dequeue
	eval, token, err := e.srv.evalBroker.Dequeue(args.Schedulers, args.Timeout)
	if err != nil {
		return err
	}

	// Provide the output if any
	if eval != nil {
		reply.Eval = eval
		reply.Token = token
	}

	// Set the query response
	e.srv.setQueryMeta(&reply.QueryMeta)
	return nil
}

// Ack is used to acknowledge completion of a dequeued evaluation
func (e *Eval) Ack(args *models.EvalAckRequest,
	reply *models.GenericResponse) error {
	if done, err := e.srv.forward("Eval.Ack", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "eval", "ack"}, time.Now())

	// Ack the EvalID
	if err := e.srv.evalBroker.Ack(args.EvalID, args.Token); err != nil {
		return err
	}
	return nil
}

// NAck is used to negative acknowledge completion of a dequeued evaluation
func (e *Eval) Nack(args *models.EvalAckRequest,
	reply *models.GenericResponse) error {
	if done, err := e.srv.forward("Eval.Nack", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "eval", "nack"}, time.Now())

	// Nack the EvalID
	if err := e.srv.evalBroker.Nack(args.EvalID, args.Token); err != nil {
		return err
	}
	return nil
}

// Update is used to perform an update of an Eval if it is outstanding.
func (e *Eval) Update(args *models.EvalUpdateRequest,
	reply *models.GenericResponse) error {
	if done, err := e.srv.forward("Eval.Update", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "eval", "update"}, time.Now())

	// Ensure there is only a single update with token
	if len(args.Evals) != 1 {
		return fmt.Errorf("only a single eval can be updated")
	}
	eval := args.Evals[0]

	// Verify the evaluation is outstanding, and that the tokens match.
	if err := e.srv.evalBroker.OutstandingReset(eval.ID, args.EvalToken); err != nil {
		return err
	}

	// Update via Raft
	_, index, err := e.srv.raftApply(models.EvalUpdateRequestType, args)
	if err != nil {
		return err
	}

	// Update the index
	reply.Index = index
	return nil
}

// Create is used to make a new evaluation
func (e *Eval) Create(args *models.EvalUpdateRequest,
	reply *models.GenericResponse) error {
	if done, err := e.srv.forward("Eval.Create", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "eval", "create"}, time.Now())

	// Ensure there is only a single update with token
	if len(args.Evals) != 1 {
		return fmt.Errorf("only a single eval can be created")
	}
	eval := args.Evals[0]

	// Verify the parent evaluation is outstanding, and that the tokens match.
	if err := e.srv.evalBroker.OutstandingReset(eval.PreviousEval, args.EvalToken); err != nil {
		return err
	}

	// Look for the eval
	snap, err := e.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}

	ws := memdb.NewWatchSet()
	out, err := snap.EvalByID(ws, eval.ID)
	if err != nil {
		return err
	}
	if out != nil {
		return fmt.Errorf("evaluation already exists")
	}

	// Update via Raft
	_, index, err := e.srv.raftApply(models.EvalUpdateRequestType, args)
	if err != nil {
		return err
	}

	// Update the index
	reply.Index = index
	return nil
}

// Reblock is used to reinsert an existing blocked evaluation into the blocked
// evaluation tracker.
func (e *Eval) Reblock(args *models.EvalUpdateRequest, reply *models.GenericResponse) error {
	if done, err := e.srv.forward("Eval.Reblock", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "eval", "reblock"}, time.Now())

	// Ensure there is only a single update with token
	if len(args.Evals) != 1 {
		return fmt.Errorf("only a single eval can be reblocked")
	}
	eval := args.Evals[0]

	// Verify the evaluation is outstanding, and that the tokens match.
	if err := e.srv.evalBroker.OutstandingReset(eval.ID, args.EvalToken); err != nil {
		return err
	}

	// Look for the eval
	snap, err := e.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}

	ws := memdb.NewWatchSet()
	out, err := snap.EvalByID(ws, eval.ID)
	if err != nil {
		return err
	}
	if out == nil {
		return fmt.Errorf("evaluation does not exist")
	}
	if out.Status != models.EvalStatusBlocked {
		return fmt.Errorf("evaluation not blocked")
	}

	// Reblock the eval
	e.srv.blockedEvals.Reblock(eval, args.EvalToken)
	return nil
}

// Reap is used to cleanup dead evaluations and allocations
func (e *Eval) Reap(args *models.EvalDeleteRequest,
	reply *models.GenericResponse) error {
	if done, err := e.srv.forward("Eval.Reap", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "eval", "reap"}, time.Now())

	// Update via Raft
	_, index, err := e.srv.raftApply(models.EvalDeleteRequestType, args)
	if err != nil {
		return err
	}

	// Update the index
	reply.Index = index
	return nil
}

// List is used to get a list of the evaluations in the system
func (e *Eval) List(args *models.EvalListRequest,
	reply *models.EvalListResponse) error {
	if done, err := e.srv.forward("Eval.List", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "eval", "list"}, time.Now())

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Scan all the evaluations
			var err error
			var iter memdb.ResultIterator
			if prefix := args.QueryOptions.Prefix; prefix != "" {
				iter, err = state.EvalsByIDPrefix(ws, prefix)
			} else {
				iter, err = state.Evals(ws)
			}
			if err != nil {
				return err
			}

			var evals []*models.Evaluation
			for {
				raw := iter.Next()
				if raw == nil {
					break
				}
				eval := raw.(*models.Evaluation)
				evals = append(evals, eval)
			}
			reply.Evaluations = evals

			// Use the last index that affected the jobs table
			index, err := state.Index("evals")
			if err != nil {
				return err
			}
			reply.Index = index

			// Set the query response
			e.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return e.srv.blockingRPC(&opts)
}

// Allocations is used to list the allocations for an evaluation
func (e *Eval) Allocations(args *models.EvalSpecificRequest,
	reply *models.EvalAllocationsResponse) error {
	if done, err := e.srv.forward("Eval.Allocations", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "eval", "allocations"}, time.Now())

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Capture the allocations
			allocs, err := state.AllocsByEval(ws, args.EvalID)
			if err != nil {
				return err
			}

			// Convert to a stub
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
			e.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return e.srv.blockingRPC(&opts)
}
