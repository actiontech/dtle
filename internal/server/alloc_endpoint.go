/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package server

import (
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-memdb"

	"github.com/actiontech/udup/internal/models"
	"github.com/actiontech/udup/internal/server/store"
)

// Alloc endpoint is used for manipulating allocations
type Alloc struct {
	srv *Server
}

// List is used to list the allocations in the system
func (a *Alloc) List(args *models.AllocListRequest, reply *models.AllocListResponse) error {
	if done, err := a.srv.forward("Alloc.List", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "alloc", "list"}, time.Now())

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Capture all the allocations
			var err error
			var iter memdb.ResultIterator
			if prefix := args.QueryOptions.Prefix; prefix != "" {
				iter, err = state.AllocsByIDPrefix(ws, prefix)
			} else {
				iter, err = state.Allocs(ws)
			}
			if err != nil {
				return err
			}

			var allocs []*models.AllocListStub
			for {
				raw := iter.Next()
				if raw == nil {
					break
				}
				alloc := raw.(*models.Allocation)
				allocs = append(allocs, alloc.Stub())
			}
			reply.Allocations = allocs

			// Use the last index that affected the jobs table
			index, err := state.Index("allocs")
			if err != nil {
				return err
			}
			reply.Index = index

			// Set the query response
			a.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return a.srv.blockingRPC(&opts)
}

// GetAlloc is used to lookup a particular allocation
func (a *Alloc) GetAlloc(args *models.AllocSpecificRequest,
	reply *models.SingleAllocResponse) error {
	if done, err := a.srv.forward("Alloc.GetAlloc", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "alloc", "get_alloc"}, time.Now())

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Lookup the allocation
			out, err := state.AllocByID(ws, args.AllocID)
			if err != nil {
				return err
			}

			// Setup the output
			reply.Alloc = out
			if out != nil {
				reply.Index = out.ModifyIndex
			} else {
				// Use the last index that affected the allocs table
				index, err := state.Index("allocs")
				if err != nil {
					return err
				}
				reply.Index = index
			}

			// Set the query response
			a.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return a.srv.blockingRPC(&opts)
}

// GetAllocs is used to lookup a set of allocations
func (a *Alloc) GetAllocs(args *models.AllocsGetRequest,
	reply *models.AllocsGetResponse) error {
	if done, err := a.srv.forward("Alloc.GetAllocs", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "alloc", "get_allocs"}, time.Now())

	allocs := make([]*models.Allocation, len(args.AllocIDs))

	// Setup the blocking query. We wait for at least one of the requested
	// allocations to be above the min query index. This guarantees that the
	// server has received that index.
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Lookup the allocation
			thresholdMet := false
			maxIndex := uint64(0)
			for i, alloc := range args.AllocIDs {
				out, err := state.AllocByID(ws, alloc)
				if err != nil {
					return err
				}
				if out == nil {
					// We don't have the alloc yet
					thresholdMet = false
					break
				}

				// Store the pointer
				allocs[i] = out

				// Check if we have passed the minimum index
				if out.ModifyIndex > args.QueryOptions.MinQueryIndex {
					thresholdMet = true
				}

				if maxIndex < out.ModifyIndex {
					maxIndex = out.ModifyIndex
				}
			}

			// Setup the output
			if thresholdMet {
				reply.Allocs = allocs
				reply.Index = maxIndex
			} else {
				// Use the last index that affected the nodes table
				index, err := state.Index("allocs")
				if err != nil {
					return err
				}
				reply.Index = index
			}

			// Set the query response
			a.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		},
	}
	return a.srv.blockingRPC(&opts)
}
