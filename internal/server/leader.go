/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package server

import (
	"fmt"
	"net"
	"time"

	"github.com/armon/go-metrics"
	memdb "github.com/hashicorp/go-memdb"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"

	"github.com/actiontech/dts/internal/models"
)

const (
	// failedEvalUnblockInterval is the interval at which failed evaluations are
	// unblocked to re-enter the scheduler. A failed evaluation occurs under
	// high contention when the schedulers plan does not make progress.
	failedEvalUnblockInterval = 1 * time.Minute
)

// monitorLeadership is used to monitor if we acquire or lose our role
// as the leader in the Raft cluster. There is some work the leader is
// expected to do, so we must react to changes
func (s *Server) monitorLeadership() {
	var stopCh chan struct{}
	for {
		select {
		case isLeader := <-s.leaderCh:
			if isLeader {
				stopCh = make(chan struct{})
				go s.leaderLoop(stopCh)
				s.logger.Printf("manager: cluster leadership acquired")
			} else if stopCh != nil {
				close(stopCh)
				stopCh = nil
				s.logger.Printf("manager: cluster leadership lost")
			}
		case <-s.shutdownCh:
			return
		}
	}
}

// leaderLoop runs as long as we are the leader to run various
// maintence activities
func (s *Server) leaderLoop(stopCh chan struct{}) {
	// Ensure we revoke leadership on stepdown
	defer s.revokeLeadership()

	var reconcileCh chan serf.Member
	establishedLeader := false

RECONCILE:
	// Setup a reconciliation timer
	reconcileCh = nil
	interval := time.After(s.config.ReconcileInterval)

	// Apply a raft barrier to ensure our FSM is caught up
	start := time.Now()
	barrier := s.raft.Barrier(0)
	if err := barrier.Error(); err != nil {
		s.logger.Errorf("manager: failed to wait for barrier: %v", err)
		goto WAIT
	}
	metrics.MeasureSince([]string{"server", "leader", "barrier"}, start)

	// Check if we need to handle initial leadership actions
	if !establishedLeader {
		if err := s.establishLeadership(stopCh); err != nil {
			s.logger.Errorf("manager: failed to establish leadership: %v",
				err)
			goto WAIT
		}
		establishedLeader = true
	}

	// Reconcile any missing data
	if err := s.reconcile(); err != nil {
		s.logger.Errorf("manager: failed to reconcile: %v", err)
		goto WAIT
	}

	// Initial reconcile worked, now we can process the channel
	// updates
	reconcileCh = s.reconcileCh

WAIT:
	// Wait until leadership is lost
	for {
		select {
		case <-stopCh:
			return
		case <-s.shutdownCh:
			return
		case <-interval:
			goto RECONCILE
		case member := <-reconcileCh:
			s.reconcileMember(member)
		}
	}
}

// establishLeadership is invoked once we become leader and are able
// to invoke an initial barrier. The barrier is used to ensure any
// previously inflight transactions have been committed and that our
// state is up-to-date.
func (s *Server) establishLeadership(stopCh chan struct{}) error {
	// Disable workers to free half the cores for use in the plan queue and
	// evaluation broker
	if numWorkers := len(s.workers); numWorkers > 1 {
		// Disabling 3/4 of the workers frees CPU for raft and the
		// plan applier which uses 1/2 the cores.
		for i := 0; i < (3 * numWorkers / 4); i++ {
			s.workers[i].SetPause(true)
		}
	}

	// Enable the plan queue, since we are now the leader
	s.planQueue.SetEnabled(true)

	// Start the plan evaluator
	go s.planApply()

	// Enable the eval broker, since we are now the leader
	s.evalBroker.SetEnabled(true)

	// Enable the blocked eval tracker, since we are now the leader
	s.blockedEvals.SetEnabled(true)

	// Restore the eval broker state
	if err := s.restoreEvals(); err != nil {
		return err
	}

	// Reap any failed evaluations
	go s.reapFailedEvaluations(stopCh)

	// Reap any duplicate blocked evaluations
	go s.reapDupBlockedEvaluations(stopCh)

	// Periodically unblock failed allocations
	go s.periodicUnblockFailedEvals(stopCh)

	// Setup the heartbeat timers. This is done both when starting up or when
	// a leader fail over happens. Since the timers are maintained by the leader
	// node, effectively this means all the timers are renewed at the time of failover.
	// The TTL contract is that the session will not be expired before the TTL,
	// so expiring it later is allowable.
	//
	// This MUST be done after the initial barrier to ensure the latest Nodes
	// are available to be initialized. Otherwise initialization may use stale
	// data.
	if err := s.initializeHeartbeatTimers(); err != nil {
		s.logger.Errorf("manager: heartbeat timer setup failed: %v", err)
		return err
	}

	return nil
}

// restoreEvals is used to restore pending evaluations into the eval broker and
// blocked evaluations into the blocked eval tracker. The broker and blocked
// eval tracker is maintained only by the leader, so it must be restored anytime
// a leadership transition takes place.
func (s *Server) restoreEvals() error {
	// Get an iterator over every evaluation
	ws := memdb.NewWatchSet()
	iter, err := s.fsm.State().Evals(ws)
	if err != nil {
		return fmt.Errorf("failed to get evaluations: %v", err)
	}

	for {
		raw := iter.Next()
		if raw == nil {
			break
		}
		eval := raw.(*models.Evaluation)

		if eval.ShouldEnqueue() {
			s.evalBroker.Enqueue(eval)
		} else if eval.ShouldBlock() {
			s.blockedEvals.Block(eval)
		}
	}
	return nil
}

// reapFailedEvaluations is used to reap evaluations that
// have reached their delivery limit and should be failed
func (s *Server) reapFailedEvaluations(stopCh chan struct{}) {
	for {
		select {
		case <-stopCh:
			return
		default:
			// Scan for a failed evaluation
			eval, token, err := s.evalBroker.Dequeue([]string{failedQueue}, time.Second)
			if err != nil {
				return
			}
			if eval == nil {
				continue
			}

			// Update the status to failed
			newEval := eval.Copy()
			newEval.Status = models.EvalStatusFailed
			newEval.StatusDescription = fmt.Sprintf("evaluation reached delivery limit (%d)", s.config.EvalDeliveryLimit)
			s.logger.Warnf("manager: eval %#v reached delivery limit, marking as failed", newEval)

			// Update via Raft
			req := models.EvalUpdateRequest{
				Evals: []*models.Evaluation{newEval},
			}
			if _, _, err := s.raftApply(models.EvalUpdateRequestType, &req); err != nil {
				s.logger.Errorf("manager: failed to update failed eval %#v: %v", newEval, err)
				continue
			}

			// Ack completion
			s.evalBroker.Ack(eval.ID, token)
		}
	}
}

// reapDupBlockedEvaluations is used to reap duplicate blocked evaluations and
// should be cancelled.
func (s *Server) reapDupBlockedEvaluations(stopCh chan struct{}) {
	for {
		select {
		case <-stopCh:
			return
		default:
			// Scan for duplicate blocked evals.
			dups := s.blockedEvals.GetDuplicates(time.Second)
			if dups == nil {
				continue
			}

			cancel := make([]*models.Evaluation, len(dups))
			for i, dup := range dups {
				// Update the status to cancelled
				newEval := dup.Copy()
				newEval.Status = models.EvalStatusCancelled
				newEval.StatusDescription = fmt.Sprintf("existing blocked evaluation exists for job %q", newEval.JobID)
				cancel[i] = newEval
			}

			// Update via Raft
			req := models.EvalUpdateRequest{
				Evals: cancel,
			}
			if _, _, err := s.raftApply(models.EvalUpdateRequestType, &req); err != nil {
				s.logger.Errorf("manager: failed to update duplicate evals %#v: %v", cancel, err)
				continue
			}
		}
	}
}

// periodicUnblockFailedEvals periodically unblocks failed, blocked evaluations.
func (s *Server) periodicUnblockFailedEvals(stopCh chan struct{}) {
	ticker := time.NewTicker(failedEvalUnblockInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			// Unblock the failed allocations
			s.blockedEvals.UnblockFailed()
		}
	}
}

// revokeLeadership is invoked once we step down as leader.
// This is used to cleanup any state that may be specific to a leader.
func (s *Server) revokeLeadership() error {
	// Disable the plan queue, since we are no longer leader
	s.planQueue.SetEnabled(false)

	// Disable the eval broker, since it is only useful as a leader
	s.evalBroker.SetEnabled(false)

	// Disable the blocked eval tracker, since it is only useful as a leader
	s.blockedEvals.SetEnabled(false)

	// Clear the heartbeat timers on either shutdown or step down,
	// since we are no longer responsible for TTL expirations.
	if err := s.clearAllHeartbeatTimers(); err != nil {
		s.logger.Errorf("manager: clearing heartbeat timers failed: %v", err)
		return err
	}

	// Unpause our worker if we paused previously
	if len(s.workers) > 1 {
		for i := 0; i < len(s.workers)/2; i++ {
			s.workers[i].SetPause(false)
		}
	}
	return nil
}

// reconcile is used to reconcile the differences between Serf
// membership and what is reflected in our strongly consistent store.
func (s *Server) reconcile() error {
	defer metrics.MeasureSince([]string{"server", "leader", "reconcile"}, time.Now())
	members := s.serf.Members()
	for _, member := range members {
		if err := s.reconcileMember(member); err != nil {
			return err
		}
	}
	return nil
}

// reconcileMember is used to do an async reconcile of a single serf member
func (s *Server) reconcileMember(member serf.Member) error {
	// Check if this is a member we should handle
	valid, parts := isUdupServer(member)
	if !valid || parts.Region != s.config.Region {
		return nil
	}
	defer metrics.MeasureSince([]string{"server", "leader", "reconcileMember"}, time.Now())

	// Do not reconcile ourself
	if member.Name == fmt.Sprintf("%s.%s", s.config.NodeName, s.config.Region) {
		return nil
	}

	var err error
	switch member.Status {
	case serf.StatusAlive:
		err = s.addRaftPeer(member, parts)
	case serf.StatusLeft, StatusReap:
		err = s.removeRaftPeer(member, parts)
	}
	if err != nil {
		s.logger.Errorf("manager: failed to reconcile member: %v: %v",
			member, err)
		return err
	}
	return nil
}

// addRaftPeer is used to add a new Raft peer when a Udup server joins
func (s *Server) addRaftPeer(m serf.Member, parts *serverParts) error {
	// Do not join ourselfs
	if m.Name == s.config.NodeName {
		s.logger.Debugf("manager: adding self (%q) as raft peer skipped", m.Name)
		return nil
	}

	// Check for possibility of multiple bootstrap nodes
	if parts.Bootstrap {
		members := s.serf.Members()
		for _, member := range members {
			valid, p := isUdupServer(member)
			if valid && member.Name != m.Name && p.Bootstrap {
				s.logger.Errorf("manager: '%v' and '%v' are both in bootstrap mode. Only one node should be in bootstrap mode, not adding Raft peer.", m.Name, member.Name)
				return nil
			}
		}
	}

	// TODO (alexdadgar) - This will need to be changed once we support node IDs.
	addr := (&net.TCPAddr{IP: m.Addr, Port: parts.Port}).String()

	// See if it's already in the configuration. It's harmless to re-add it
	// but we want to avoid doing that if possible to prevent useless Raft
	// log entries.
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Errorf("manager: failed to get raft configuration: %v", err)
		return err
	}
	for _, server := range configFuture.Configuration().Servers {
		if server.Address == raft.ServerAddress(addr) {
			return nil
		}
	}

	// Attempt to add as a peer
	addFuture := s.raft.AddPeer(raft.ServerAddress(addr))
	if err := addFuture.Error(); err != nil {
		s.logger.Errorf("manager: failed to add raft peer: %v", err)
		return err
	} else if err == nil {
		s.logger.Printf("manager: added raft peer: %v", parts)
	}
	return nil
}

// removeRaftPeer is used to remove a Raft peer when a Udup server leaves
// or is reaped
func (s *Server) removeRaftPeer(m serf.Member, parts *serverParts) error {
	// TODO (alexdadgar) - This will need to be changed once we support node IDs.
	addr := (&net.TCPAddr{IP: m.Addr, Port: parts.Port}).String()

	// See if it's already in the configuration. It's harmless to re-remove it
	// but we want to avoid doing that if possible to prevent useless Raft
	// log entries.
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Errorf("manager: failed to get raft configuration: %v", err)
		return err
	}
	for _, server := range configFuture.Configuration().Servers {
		if server.Address == raft.ServerAddress(addr) {
			goto REMOVE
		}
	}
	return nil

REMOVE:
	// Attempt to remove as a peer.
	future := s.raft.RemovePeer(raft.ServerAddress(addr))
	if err := future.Error(); err != nil {
		s.logger.Errorf("manager: failed to remove raft peer '%v': %v",
			parts, err)
		return err
	}
	return nil
}
