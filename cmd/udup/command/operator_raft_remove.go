/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/actiontech/udup/api"
)

type OperatorRaftRemoveCommand struct {
	Meta
}

func (c *OperatorRaftRemoveCommand) Help() string {
	helpText := `
Usage: udup remove-peer [options]

Remove the Udup server with given -peer-address from the Raft configuration.

There are rare cases where a peer may be left behind in the Raft quorum even
though the server is no longer present and known to the cluster. This command
can be used to remove the failed server so that it is no longer affects the Raft
quorum. If the server still shows in the output of the "udup members"
command, it is preferable to clean up by simply running "udup
server-force-leave" instead of this command.

General Options:

  ` + generalOptionsUsage() + `

Remove Peer Options:

  -address="IP:port"
    Remove a Udup server with given address from the Raft configuration.
  -id=""
    Remove a Udup server with given ID from the Raft configuration.
`
	return strings.TrimSpace(helpText)
}

func (c *OperatorRaftRemoveCommand) Synopsis() string {
	return "Remove a Udup server from the Raft configuration"
}

func (c *OperatorRaftRemoveCommand) Run(args []string) int {
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	flags.Usage = func() { c.Ui.Error(c.Help()) }

	var address, id string
	flags.StringVar(&address, "address", "",
		"The address to remove from the Raft configuration.")
	flags.StringVar(&id, "id", "",
		"The ID to remove from the Raft configuration.")

	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		c.Ui.Error(fmt.Sprintf("Failed to parse args: %v", err))
		return 1
	}

	// Set up a client.
	client, err := c.Meta.Client()
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Error initializing client: %s", err))
		return 1
	}

	// Fetch the current configuration.
	if err := raftRemovePeers(address, id, client.Operator()); err != nil {
		c.Ui.Error(fmt.Sprintf("Error removing peer: %v", err))
		return 1
	}
	if address != "" {
		c.Ui.Output(fmt.Sprintf("Removed peer with address %q", address))
	} else {
		c.Ui.Output(fmt.Sprintf("Removed peer with id %q", id))
	}

	return 0
}

func raftRemovePeers(address, id string, operator *api.Operator) error {
	if len(address) == 0 && len(id) == 0 {
		return fmt.Errorf("an address or id is required for the peer to remove")
	}
	if len(address) > 0 && len(id) > 0 {
		return fmt.Errorf("cannot give both an address and id")
	}

	// Try to kick the peer.
	if len(address) > 0 {
		if err := operator.RaftRemovePeerByAddress(address, nil); err != nil {
			return err
		}
	} else {
		if err := operator.RaftRemovePeerByID(id, nil); err != nil {
			return err
		}
	}

	return nil
}
