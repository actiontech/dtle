package server

import (
	"fmt"
	"time"

	"github.com/armon/go-metrics"
	"udup/internal/server/models"
)

// Client endpoint is used for client interactions
type Client struct {
	srv *Server
}

// Register is used to upsert a client that is available for scheduling
func (c *Client) Register(args *models.RegisterRequest, reply *models.RegisterResponse) error {
	if done, err := c.srv.forward("Client.Register", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"server", "client", "register"}, time.Now())

	// Validate the arguments
	if args.Region == "" {
		return fmt.Errorf("missing region for client registration")
	}
	if args.Datacenter == "" {
		return fmt.Errorf("missing datacenter for client registration")
	}
	if args.Node == "" {
		return fmt.Errorf("missing node name for client registration")
	}
	if _, ok := args.Capabilities[models.CoreCapability]; !ok {
		return fmt.Errorf("missing core capability for client registration")
	}

	// Default the status if none is given
	if args.Status == "" {
		args.Status = models.StatusInit
	}

	// Commit this update via Raft
	_, err := c.srv.raftApply(models.RegisterRequestType, args)
	if err != nil {
		c.srv.logger.Printf("[ERR] server.client: Register failed: %v", err)
		return err
	}
	return nil
}
