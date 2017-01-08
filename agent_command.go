package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"syscall"
	"time"

	"github.com/mitchellh/cli"
	"github.com/ngaut/log"

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

// setupAgent is used to start the agent and various interfaces
func (c *AgentCommand) setupAgent(config *uconf.Config) error {
	agent, err := NewAgent(config)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Error starting agent: %s", err))
		return err
	}
	c.agent = agent

	// Output the header that the server has started
	c.Ui.Output("Udup agent started!\n")

	return nil
}

func (c *AgentCommand) Run(args []string) int {

	// Parse our configs
	c.args = args
	config := c.readConfig()
	if config == nil {
		return 1
	}

	// Log config file
	if len(config.File) > 0 {
		c.Ui.Info(fmt.Sprintf("Loaded configuration from %s", config.File))
	} else {
		c.Ui.Info("No configuration file loaded")
	}

	// Create the agent
	if err := c.setupAgent(config); err != nil {
		return 1
	}

	log.SetLevelByString(config.LogLevel)
	if len(config.LogFile) > 0 {
		log.SetOutputByName(config.LogFile)
		log.SetHighlighting(false)

		if config.LogRotate == "hour" {
			log.SetRotateByHour()
		} else {
			log.SetRotateByDay()
		}
	}

	// Wait for exit
	return c.handleSignals(config)
}

// handleSignals blocks until we get an exit-causing signal
func (c *AgentCommand) handleSignals(config *uconf.Config) int {
	signalCh := make(chan os.Signal, 4)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	// Wait for a signal
WAIT:
	var sig os.Signal
	select {
	case s := <-signalCh:
		sig = s
	case <-c.ShutdownCh:
		sig = os.Interrupt
	}
	c.Ui.Output(fmt.Sprintf("Caught signal: %v", sig))

	// Check if this is a SIGHUP
	if sig == syscall.SIGHUP {
		if conf := c.handleReload(config); conf != nil {
			*config = *conf
		}
		goto WAIT
	}

	// Check if we should do a graceful leave
	graceful := false
	if sig == os.Interrupt {
		graceful = true
	} else if sig == syscall.SIGTERM {
		graceful = true
	}

	// Bail fast if not doing a graceful leave
	if !graceful {
		return 1
	}

	// Attempt a graceful leave
	gracefulCh := make(chan struct{})
	c.Ui.Output("Gracefully shutting down agent...")

	// Wait for leave or another signal
	select {
	case <-signalCh:
		return 1
	case <-time.After(gracefulTimeout):
		return 1
	case <-gracefulCh:
		return 0
	}
}

// handleReload is invoked when we should reload our configs, e.g. SIGHUP
func (c *AgentCommand) handleReload(config *uconf.Config) *uconf.Config {
	c.Ui.Output("Reloading configuration...")
	newConf := c.readConfig()
	if newConf == nil {
		c.Ui.Error(fmt.Sprintf("Failed to reload configs"))
		return config
	}

	// Change the log level
	log.SetLevelByString(newConf.LogLevel)
	return newConf
}

func (a *AgentCommand) readConfig() *uconf.Config {
	// Make a new, empty config.
	cmdConfig := &uconf.Config{}

	flags := flag.NewFlagSet("agent", flag.ContinueOnError)
	flags.Usage = func() { a.Ui.Error(a.Help()) }

	// General options
	flags.StringVar(&cmdConfig.File, "config", "config", "")
	flags.StringVar(&cmdConfig.LogLevel, "log-level", "info", "")

	if err := flags.Parse(a.args); err != nil {
		return nil
	}

	// Load the configuration
	var config *uconf.Config
	config = uconf.DefaultConfig()

	current, err := uconf.LoadConfig(cmdConfig.File)
	if err != nil {
		a.Ui.Error(fmt.Sprintf(
			"Error loading configuration from %s: %s", cmdConfig.File, err))
		return nil
	}

	// The user asked us to load some config here but we didn't find any,
	// so we'll complain but continue.
	if current == nil || reflect.DeepEqual(current, &uconf.Config{}) {
		a.Ui.Info(fmt.Sprintf("No configuration loaded from %s", cmdConfig.File))
	}

	if config == nil {
		config = current
	}

	return config
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
  file used, but a subset of the options may also be passed directly
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
