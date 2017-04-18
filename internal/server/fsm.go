package server

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/raft"
	"udup/internal/server/models"
)

// udupFSM implements a finite state machine that is used
// along with Raft to provide strong consistency. We implement
// this outside the Server to avoid exposing this outside the package.
type udupFSM struct {
	logOutput io.Writer
	logger    *log.Logger
	state     *StateStore
}

// udupSnapshot is used to provide a snapshot of the current
// state in a way that can be accessed concurrently with operations
// that may modify the live state.
type udupSnapshot struct {
	state *StateSnapshot
}

// NewFSMPath is used to construct a new FSM with a blank state
func NewFSM(logOutput io.Writer) (*udupFSM, error) {
	// Create a state store
	state, err := NewStateStore(logOutput)
	if err != nil {
		return nil, err
	}

	fsm := &udupFSM{
		logOutput: logOutput,
		logger:    log.New(logOutput, "", log.LstdFlags),
		state:     state,
	}
	return fsm, nil
}

// Close is used to cleanup resources associated with the FSM
func (n *udupFSM) Close() error {
	return nil
}

// State is used to return a handle to the current state
func (n *udupFSM) State() *StateStore {
	return n.state
}

func (n *udupFSM) Apply(log *raft.Log) interface{} {
	buf := log.Data
	msgType := models.MessageType(buf[0])

	// Check if this message type should be ignored when unknown. This is
	// used so that new commands can be added with developer control if older
	// versions can safely ignore the command, or if they should crash.
	ignoreUnknown := false
	if msgType&models.IgnoreUnknownTypeFlag == models.IgnoreUnknownTypeFlag {
		msgType &= ^models.IgnoreUnknownTypeFlag
		ignoreUnknown = true
	}

	switch msgType {
	case models.NodeRegisterRequestType:
		return n.decodeRegister(buf[1:], log.Index)
	case models.NodeDeregisterRequestType:
		return n.applyDeregister(buf[1:], log.Index)
	case models.NodeUpdateStatusRequestType:
		return n.applyStatusUpdate(buf[1:], log.Index)
	default:
		if ignoreUnknown {
			n.logger.Printf("[WARN] server.fsm: ignoring unknown message type (%d), upgrade to newer version", msgType)
			return nil
		} else {
			panic(fmt.Errorf("failed to apply request: %#v", buf))
		}
	}
}

func (n *udupFSM) decodeRegister(buf []byte, index uint64) interface{} {
	var req models.RegisterRequest
	if err := models.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}
	return n.applyRegister(&req, index)
}

func (n *udupFSM) applyRegister(req *models.RegisterRequest, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"server", "fsm", "register"}, time.Now())
	if err := n.state.RegisterNode(index, req.Node); err != nil {
		n.logger.Printf("[ERR] server.fsm: RegisterNode failed: %v", err)
		return err
	}
	return nil
}

func (n *udupFSM) applyDeregister(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"server", "fsm", "deregister"}, time.Now())
	var req models.DeregisterRequest
	if err := models.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.DeregisterNode(index, req.NodeID); err != nil {
		n.logger.Printf("[ERR] server.fsm: DeregisterNode failed: %v", err)
		return err
	}
	return nil
}

func (n *udupFSM) applyStatusUpdate(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"server", "fsm", "node_status_update"}, time.Now())
	var req models.UpdateStatusRequest
	if err := models.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.UpdateNodeStatus(index, req.NodeID, req.Status); err != nil {
		n.logger.Printf("[ERR] server.fsm: UpdateNodeStatus failed: %v", err)
		return err
	}
	return nil
}

func (n *udupFSM) Snapshot() (raft.FSMSnapshot, error) {
	// Create a new snapshot
	snap, err := n.state.Snapshot()
	if err != nil {
		return nil, err
	}
	return &udupSnapshot{snap}, nil
}

func (n *udupFSM) Restore(old io.ReadCloser) error {
	defer old.Close()
	return nil
}

func (s *udupSnapshot) Persist(sink raft.SnapshotSink) error {
	defer metrics.MeasureSince([]string{"server", "fsm", "persist"}, time.Now())
	return nil
}

func (s *udupSnapshot) Release() {
	return
}
