package testdata

import (
	"io"

	hclog "github.com/hashicorp/go-hclog"
)

func badHCLog() {
	l := hclog.L()

	var (
		err            = io.EOF
		numConnections = 5
		ipAddr         = "10.40.40.10"
	)

	// good
	l.Info("ok", "key", "val")
	l.Error("raft request failed", "error", err)
	l.Error("error opening file", "error", err)
	l.Debug("too many connections", "connections", numConnections, "ip", ipAddr)

	// bad
	l.Info("bad", "key")
	l.Error("raft request failed: %v", err)
	l.Error("error opening file", err)
	l.Debug("too many connections", numConnections, "ip", ipAddr)
}
