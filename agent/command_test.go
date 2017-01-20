package agent

import (
	"testing"

	"github.com/mitchellh/cli"
)

func TestCommand_Implements(t *testing.T) {
	var _ cli.Command = &AgentCommand{}
}

func TestCommand_Args(t *testing.T) {
	// Make a new command. We pre-emptively close the shutdownCh
	// so that the command exits immediately instead of blocking.
	ui := new(cli.MockUi)
	shutdownCh := make(chan struct{})
	close(shutdownCh)
	cmd := &AgentCommand{
		Ui:         ui,
		ShutdownCh: shutdownCh,
	}

	args := []string{}
	args = append(args, "-config=../etc/udup.conf")
	args = append(args, "-log-level=debug")
	args = append(args, "agent")
	if code := cmd.Run(args); code != 1 {
		t.Fatalf("args: %v\nexit: %d\n", args, code)
	}
}
