/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package server

import (
	"github.com/hashicorp/go-memdb"

	"github.com/actiontech/udup/internal/models"
	"github.com/actiontech/udup/internal/server/store"
)

// Internal endpoint is used to query the miscellaneous info that
// does not necessarily fit into the other systems. It is also
// used to hold undocumented APIs that users should not rely on.
type Internal struct {
	srv *Server
}

// NodeInfo is used to retrieve information about a specific node.
func (m *Internal) NodeInfo(args *models.NodeSpecificRequest,
	reply *models.IndexedNodeDump) error {
	if done, err := m.srv.forward("Internal.NodeInfo", args, args, reply); done {
		return err
	}

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Capture all the nodes
			var err error
			dump, err := state.NodeInfo(args.NodeID)
			if err != nil {
				return err
			}

			reply.Dump = dump
			// Use the last index that affected the jobs table
			index, err := state.Index("nodes")
			if err != nil {
				return err
			}
			reply.Index = index

			// Set the query response
			m.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return m.srv.blockingRPC(&opts)
}

// NodeDump is used to generate information about all of the nodes.
func (m *Internal) NodeDump(args *models.DCSpecificRequest,
	reply *models.IndexedNodeDump) error {
	if done, err := m.srv.forward("Internal.NodeDump", args, args, reply); done {
		return err
	}
	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Capture all the nodes
			var err error
			dump, err := state.NodeDump()
			if err != nil {
				return err
			}

			reply.Dump = dump
			// Use the last index that affected the jobs table
			index, err := state.Index("nodes")
			if err != nil {
				return err
			}
			reply.Index = index

			// Set the query response
			m.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return m.srv.blockingRPC(&opts)
}
