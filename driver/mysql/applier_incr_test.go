package mysql

import (
	"github.com/hashicorp/go-hclog"
	"testing"
	"time"
)

func TestMtsManager(t *testing.T) {
	logger := hclog.Default()
	shutdownCh := make(chan struct{})
	defer close(shutdownCh)

	mm := NewMtsManager(shutdownCh, logger)
	go mm.LcUpdater()

	doneCh := make(chan struct{})
	go func() {
		mm.WaitForExecution0(1, 0)
		logger.Info("wait 1 0")
		mm.Executed0(1)
		mm.WaitForExecution0(2, 1)
		logger.Info("wait 2 1")
		mm.Executed0(2)
		mm.WaitForExecution0(3, 2)
		logger.Info("wait 3 2")
		mm.Executed0(3)

		// BinlogEntry was resent
		mm.WaitForExecution0(2, 1)
		mm.Executed0(2)

		mm.WaitForAllCommitted(logger)
		close(doneCh)
	}()

	tm := time.NewTimer(10 * time.Second)
	select {
	case <-doneCh:
		t.Log("case finished")
	case <-tm.C:
		t.Fatal("might be stuck")
	}
}
