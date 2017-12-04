package server

import (
	"fmt"
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-memdb"

	"udup/internal/models"
	"udup/internal/server/store"
)

type Order struct {
	srv *Server
}

// Register is used to upsert a order for scheduling
func (o *Order) Register(args *models.OrderRegisterRequest, reply *models.OrderResponse) error {
	if done, err := o.srv.forward("Order.Register", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "order", "register"}, time.Now())

	// Validate the arguments
	if args.Order == nil {
		reply.Success = false
		return fmt.Errorf("missing order for registration")
	}

	// Commit this update via Raft
	_, index, err := o.srv.raftApply(models.OrderRegisterRequestType, args)
	if err != nil {
		o.srv.logger.Errorf("server.order: Register failed: %v", err)
		reply.Success = false
		return err
	}

	// Populate the reply with eval information
	reply.Success = true
	reply.Index = index
	return nil
}

// Deregister is used to remove a order the cluster.
func (o *Order) Deregister(args *models.OrderDeregisterRequest, reply *models.OrderResponse) error {
	if done, err := o.srv.forward("Order.Deregister", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "order", "deregister"}, time.Now())

	// Validate the arguments
	if args.OrderID == "" {
		reply.Success = false
		return fmt.Errorf("missing order ID for evaluation")
	}

	// Commit this update via Raft
	_, index, err := o.srv.raftApply(models.OrderDeregisterRequestType, args)
	if err != nil {
		o.srv.logger.Errorf("server.order: Deregister failed: %v", err)
		reply.Success = false
		return err
	}

	// Populate the reply with eval information
	reply.Success = true
	reply.Index = index
	return nil
}

// List is used to list the orders registered in the system
func (o *Order) List(args *models.OrderListRequest,
	reply *models.OrderListResponse) error {
	if done, err := o.srv.forward("Order.List", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "order", "list"}, time.Now())

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *store.StateStore) error {
			// Capture all the orders
			var err error
			var iter memdb.ResultIterator
			if prefix := args.QueryOptions.Prefix; prefix != "" {
				iter, err = state.OrdersByIDPrefix(ws, prefix)
			} else {
				iter, err = state.Orders(ws)
			}
			if err != nil {
				return err
			}

			var orders []*models.Order
			for {
				raw := iter.Next()
				if raw == nil {
					break
				}
				order := raw.(*models.Order)
				orders = append(orders, order)
			}
			reply.Orders = orders

			// Use the last index that affected the orders table
			index, err := state.Index("orders")
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
