package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/mitchellh/cli"

	uconf "udup/config"
)

// gracefulTimeout controls how long we wait before forcefully terminating
const gracefulTimeout = 5 * time.Second

type AgentCommand struct {
	Ui         cli.Ui
	ShutdownCh <-chan struct{}

	args  []string
	agent *Agent
}

func (c *AgentCommand) Run(args []string) int {
	//NewAgent...
	config := &uconf.Config{}
	agent, err := NewAgent(config)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Error starting agent: %s", err))
		return 1
	}
	c.agent = agent
	c.Ui.Output("Udup agent started!\n")

	return 0
}

func (a *AgentCommand) Synopsis() string {
	return "Runs a Udup agent"
}

func (a *AgentCommand) Help() string {
	helpText := `
Usage: udup agent [options]

  Starts the Udup agent and runs until an interrupt is received.
  The agent may be a extractor and/or applier.

  The Udup agent's configuration primarily comes from the config
  files used, but a subset of the options may also be passed directly
  as CLI arguments, listed below.

General Options (clients and servers):

  -log-level=<level>
    Specify the verbosity level of Udup's logs. Valid values include
    DEBUG, INFO, and WARN, in decreasing order of verbosity. The
    default is INFO.

Extractor Options:

  -extract
    Enable extract mode for the agent. Extract mode enables a given node to be
    extract for events. If extract mode is not enabled, no work will be
    extract to the agent.

Applier Options:

  -apply
    Enable apply mode for the agent. Apply mode enables a given node to be
    applied for events. If apply mode is not enabled, no work will be
    applied to the agent.
 `
	return strings.TrimSpace(helpText)
}
