/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package server

import (
	"fmt"
	"net"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"

	"github.com/actiontech/udup/internal/models"
)

// Operator endpoint is used to perform low-level operator tasks for Udup.
type Operator struct {
	srv *Server
}

// RaftGetConfiguration is used to retrieve the current Raft configuration.
func (op *Operator) RaftGetConfiguration(args *models.GenericRequest, reply *models.RaftConfigurationResponse) error {
	if done, err := op.srv.forward("Operator.RaftGetConfiguration", args, args, reply); done {
		return err
	}

	// We can't fetch the leader and the configuration atomically with
	// the current Raft API.
	future := op.srv.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return err
	}

	// Index the Udup information about the servers.
	serverMap := make(map[raft.ServerAddress]serf.Member)
	for _, member := range op.srv.serf.Members() {
		valid, parts := isUdupServer(member)
		if !valid {
			continue
		}

		addr := (&net.TCPAddr{IP: member.Addr, Port: parts.Port}).String()
		serverMap[raft.ServerAddress(addr)] = member
	}

	// Fill out the reply.
	leader := op.srv.raft.Leader()
	reply.Index = future.Index()
	for _, server := range future.Configuration().Servers {
		node := "(unknown)"
		if member, ok := serverMap[server.Address]; ok {
			node = member.Name
		}

		entry := &models.RaftServer{
			ID:      server.ID,
			Node:    node,
			Address: server.Address,
			Leader:  server.Address == leader,
			Voter:   server.Suffrage == raft.Voter,
		}
		reply.Servers = append(reply.Servers, entry)
	}
	return nil
}

// RaftRemovePeerByAddress is used to kick a stale peer (one that it in the Raft
// quorum but no longer known to Serf or the catalog) by address in the form of
// "IP:port". The reply argument is not used, but it required to fulfill the RPC
// interface.
func (op *Operator) RaftRemovePeerByAddress(args *models.RaftPeerByAddressRequest, reply *struct{}) error {
	if done, err := op.srv.forward("Operator.RaftRemovePeerByAddress", args, args, reply); done {
		return err
	}

	// Since this is an operation designed for humans to use, we will return
	// an error if the supplied address isn't among the peers since it's
	// likely they screwed up.
	{
		future := op.srv.raft.GetConfiguration()
		if err := future.Error(); err != nil {
			return err
		}
		for _, s := range future.Configuration().Servers {
			if s.Address == args.Address {
				goto REMOVE
			}
		}
		return fmt.Errorf("address %q was not found in the Raft configuration",
			args.Address)
	}

REMOVE:
	// The Raft library itself will prevent various forms of foot-shooting,
	// like making a configuration with no voters. Some consideration was
	// given here to adding more checks, but it was decided to make this as
	// low-level and direct as possible. We've got ACL coverage to lock this
	// down, and if you are an operator, it's assumed you know what you are
	// doing if you are calling this. If you remove a peer that's known to
	// Serf, for example, it will come back when the leader does a reconcile
	// pass.
	future := op.srv.raft.RemovePeer(args.Address)
	if err := future.Error(); err != nil {
		op.srv.logger.Printf("[WARN] udup.operator: Failed to remove Raft peer %q: %v",
			args.Address, err)
		return err
	}

	op.srv.logger.Printf("[WARN] udup.operator: Removed Raft peer %q", args.Address)
	return nil
}

// RaftRemovePeerByID is used to kick a stale peer (one that is in the Raft
// quorum but no longer known to Serf or the catalog) by address in the form of
// "IP:port". The reply argument is not used, but is required to fulfill the RPC
// interface.
func (op *Operator) RaftRemovePeerByID(args *models.RaftRemovePeerRequest, reply *struct{}) error {
	if done, err := op.srv.forward("Operator.RaftRemovePeerByID", args, args, reply); done {
		return err
	}

	// Since this is an operation designed for humans to use, we will return
	// an error if the supplied id isn't among the peers since it's
	// likely they screwed up.
	{
		future := op.srv.raft.GetConfiguration()
		if err := future.Error(); err != nil {
			return err
		}
		for _, s := range future.Configuration().Servers {
			if s.ID == args.ID {
				args.Address = s.Address
				goto REMOVE
			}
		}
		return fmt.Errorf("id %q was not found in the Raft configuration",
			args.ID)
	}

REMOVE:
	// The Raft library itself will prevent various forms of foot-shooting,
	// like making a configuration with no voters. Some consideration was
	// given here to adding more checks, but it was decided to make this as
	// low-level and direct as possible. We've got ACL coverage to lock this
	// down, and if you are an operator, it's assumed you know what you are
	// doing if you are calling this. If you remove a peer that's known to
	// Serf, for example, it will come back when the leader does a reconcile
	// pass.
	var future raft.Future
	future = op.srv.raft.RemoveServer(args.ID, 0, 0)
	if err := future.Error(); err != nil {
		op.srv.logger.Printf("[WARN] udup.operator: Failed to remove Raft peer with id %q: %v",
			args.ID, err)
		return err
	}

	op.srv.logger.Printf("[WARN] udup.operator: Removed Raft peer with id %q", args.ID)
	return nil
}
