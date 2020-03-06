package server

import (
	"fmt"
	"time"

	"github.com/actiontech/dtle/internal/models"
	"github.com/actiontech/dtle/internal/server/store"
	metrics "github.com/armon/go-metrics"
	memdb "github.com/hashicorp/go-memdb"
)

type Ser struct {
	srv *Server
}

// Register is used to upsert a user for scheduling
func (o *Ser) Register(args *models.UserRegisterRequest, reply *models.UserResponse) error {
	if done, err := o.srv.forward("User.Register", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "user", "register"}, time.Now())
	// Validate the arguments
	if args.User == nil {
		reply.Success = false
		return fmt.Errorf("missing user for registration")
	}

	// Commit this update via Raft
	_, index, err := o.srv.raftApply(models.UserRegisterRequestType, args)
	if err != nil {
		o.srv.logger.Errorf("server.User: Register failed: %v", err)
		reply.Success = false
		return err
	}

	// Populate the reply with eval information
	reply.Success = true
	reply.Index = index
	return nil
}

// Deregister is used to remove a user the cluster.
func (o *Ser) Deregister(args *models.UserDeregisterRequest, reply *models.UserResponse) error {
	if done, err := o.srv.forward("User.Deregister", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "User", "deregister"}, time.Now())

	// Validate the arguments
	if args.UserID == "" {
		reply.Success = false
		return fmt.Errorf("missing user ID for evaluation")
	}

	// Commit this update via Raft
	_, index, err := o.srv.raftApply(models.UserDeregisterRequestType, args)
	if err != nil {
		o.srv.logger.Errorf("server.User: Deregister failed: %v", err)
		reply.Success = false
		return err
	}

	// Populate the reply with eval information
	reply.Success = true
	reply.Index = index
	return nil
}

// List is used to list the users registered in the system
func (o *Ser) List(args *models.UserListRequest,
	reply *models.UserListResponse) error {
	if done, err := o.srv.forward("User.List", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "User", "list"}, time.Now())

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Capture all the users
			var err error
			var iter memdb.ResultIterator

			iter, err = state.Users(ws)

			if err != nil {
				return err
			}

			var Users []*models.User
			for {
				raw := iter.Next()
				if raw == nil {
					break
				}
				user := raw.(*models.User)
				Users = append(Users, user)
			}
			reply.Users = Users

			// Use the last index that affected the users table
			index, err := state.Index("users")
			if err != nil {
				return err
			}
			reply.Index = index

			// Set the query response
			o.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return o.srv.blockingRPC(&opts)
}
func (j *Ser) GetUser(args *models.UserSpecificRequest,
	reply *models.SingleUserResponse) error {
	if done, err := j.srv.forward("User.GetUser", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "User", "get_User"}, time.Now())

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Look for the job
			out, err := state.UserByID(ws, args.UserID)
			if err != nil {
				return err
			}

			// Setup the output
			reply.User = out
			if out != nil {
				//reply.Index = out.ModifyIndex
			} else {
				// Use the last index that affected the nodes table
				index, err := state.Index("users")
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
