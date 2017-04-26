package server

import (
	"fmt"

	"udup/internal/models"
)

// System endpoint is used to call invoke system tasks.
type System struct {
	srv *Server
}

// ReconcileSummaries reconciles the summaries of all the jobs in the state
// store
func (s *System) ReconcileJobSummaries(args *models.GenericRequest, reply *models.GenericResponse) error {
	if done, err := s.srv.forward("System.ReconcileJobSummaries", args, args, reply); done {
		return err
	}

	_, index, err := s.srv.raftApply(models.ReconcileJobSummariesRequestType, args)
	if err != nil {
		return fmt.Errorf("reconciliation of job summaries failed: %v", err)
	}
	reply.Index = index
	return nil
}
