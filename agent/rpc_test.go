package agent

import (
	"testing"
	"time"

	"github.com/hashicorp/serf/testutil"
	"github.com/mitchellh/cli"

	"udup/cmd/udup/command"
)

func TestRPCExecutionDone(t *testing.T) {
	store := NewStore([]string{""}, nil)

	// Cleanup everything
	err := store.Client.DeleteTree("udup")
	if err != nil {
		t.Logf("error cleaning up: %s", err)
	}

	shutdownCh := make(chan struct{})
	defer close(shutdownCh)

	ui := new(cli.MockUi)
	a := &command.AgentCommand{
		Ui:         ui,
		ShutdownCh: shutdownCh,
	}

	aAddr := testutil.GetBindAddr().String()

	args := []string{
		"-bind", aAddr,
		"-node", "test1",
		"-server",
		"-log-level", "debug",
	}

	resultCh := make(chan int)
	go func() {
		resultCh <- a.Run(args)
	}()
	time.Sleep(2 * time.Second)

	testJob := &Job{
		Name:     "test",
		Disabled: true,
	}

	if err := store.UpsertJob(testJob); err != nil {
		t.Fatalf("error creating job: %s", err)
	}

	rc := &RPCClient{
		ServerAddr: a.Agent.getRPCAddr(),
	}

	rc.callExecutionDone(testJob)

	// Test store execution on a deleted job
	store.DeleteJob(testJob.Name)
	rc.callExecutionDone(testJob)
}
