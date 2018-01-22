package server

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/armon/go-metrics"
	memdb "github.com/hashicorp/go-memdb"
	"github.com/hashicorp/raft"
	"github.com/ugorji/go/codec"

	log "udup/internal/logger"
	"udup/internal/models"
	"udup/internal/server/store"
)

const (
	// timeTableGranularity is the granularity of index to time tracking
	timeTableGranularity = 5 * time.Minute

	// timeTableLimit is the maximum limit of our tracking
	timeTableLimit = 72 * time.Hour
)

// SnapshotType is prefixed to a record in the FSM snapshot
// so that we can determine the type for restore
type SnapshotType byte

const (
	NodeSnapshot SnapshotType = iota
	JobSnapshot
	IndexSnapshot
	EvalSnapshot
	AllocSnapshot
	TimeTableSnapshot
)

// udupFSM implements a finite store machine that is used
// along with Raft to provide strong consistency. We implement
// this outside the Server to avoid exposing this outside the package.
type udupFSM struct {
	evalBroker   *EvalBroker
	blockedEvals *BlockedEvals
	logOutput    io.Writer
	logger       *log.Logger
	state        *store.StateStore
	timetable    *TimeTable

	// stateLock is only used to protect outside callers to State() from
	// racing with Restore(), which is called by Raft (it puts in a totally
	// new store store). Everything internal here is synchronized by the
	// Raft side, so doesn't need to lock this.
	stateLock sync.RWMutex
}

// udupSnapshot is used to provide a snapshot of the current
// store in a way that can be accessed concurrently with operations
// that may modify the live store.
type udupSnapshot struct {
	snap      *store.StateSnapshot
	timetable *TimeTable
}

// snapshotHeader is the first entry in our snapshot
type snapshotHeader struct {
}

// NewFSMPath is used to construct a new FSM with a blank store
func NewFSM(evalBroker *EvalBroker,
	blocked *BlockedEvals, logOutput io.Writer, logger *log.Logger) (*udupFSM, error) {
	// Create a store store
	state, err := store.NewStateStore(logOutput)
	if err != nil {
		return nil, err
	}

	fsm := &udupFSM{
		evalBroker:   evalBroker,
		blockedEvals: blocked,
		logOutput:    logOutput,
		logger:       logger,
		state:        state,
		timetable:    NewTimeTable(timeTableGranularity, timeTableLimit),
	}
	return fsm, nil
}

// Close is used to cleanup resources associated with the FSM
func (n *udupFSM) Close() error {
	return nil
}

// State is used to return a handle to the current store
func (n *udupFSM) State() *store.StateStore {
	n.stateLock.RLock()
	defer n.stateLock.RUnlock()
	return n.state
}

// TimeTable returns the time table of transactions
func (n *udupFSM) TimeTable() *TimeTable {
	return n.timetable
}

func (n *udupFSM) Apply(log *raft.Log) interface{} {
	buf := log.Data
	msgType := models.MessageType(buf[0])

	// Witness this write
	n.timetable.Witness(log.Index, time.Now().UTC())

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
		return n.applyUpsertNode(buf[1:], log.Index)
	case models.NodeDeregisterRequestType:
		return n.applyDeregisterNode(buf[1:], log.Index)
	case models.NodeUpdateStatusRequestType:
		return n.applyStatusUpdate(buf[1:], log.Index)
	case models.JobUpdateStatusRequestType:
		return n.applyJobStatusUpdate(buf[1:], log.Index)
	case models.JobRegisterRequestType:
		return n.applyUpsertJob(buf[1:], log.Index)
	case models.JobDeregisterRequestType:
		return n.applyDeregisterJob(buf[1:], log.Index)
	case models.JobRenewalRequestType:
		return n.applyRenewalJob(buf[1:], log.Index)
	case models.OrderRegisterRequestType:
		return n.applyUpsertOrder(buf[1:], log.Index)
	case models.OrderDeregisterRequestType:
		return n.applyDeregisterOrder(buf[1:], log.Index)
	case models.JobClientUpdateRequestType:
		return n.applyJobClientUpdate(buf[1:], log.Index)
	case models.EvalUpdateRequestType:
		return n.applyUpdateEval(buf[1:], log.Index)
	case models.EvalDeleteRequestType:
		return n.applyDeleteEval(buf[1:], log.Index)
	case models.AllocUpdateRequestType:
		return n.applyAllocUpdate(buf[1:], log.Index)
	case models.AllocClientUpdateRequestType:
		return n.applyAllocClientUpdate(buf[1:], log.Index)
	default:
		if ignoreUnknown {
			n.logger.Warnf("server.fsm: ignoring unknown message type (%d), upgrade to newer version", msgType)
			return nil
		} else {
			panic(fmt.Errorf("failed to apply request: %#v", buf))
		}
	}
}

func (n *udupFSM) applyUpsertNode(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"server", "fsm", "register_node"}, time.Now())
	var req models.NodeRegisterRequest
	if err := models.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.UpsertNode(index, req.Node); err != nil {
		n.logger.Errorf("server.fsm: UpsertNode failed: %v", err)
		return err
	}

	// Unblock evals for the nodes computed node class if it is in a ready
	// store.
	if req.Node.Status == models.NodeStatusReady {
		n.blockedEvals.Unblock(req.Node.ComputedClass, index)
	}

	return nil
}

func (n *udupFSM) applyDeregisterNode(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"server", "fsm", "deregister_node"}, time.Now())
	var req models.NodeDeregisterRequest
	if err := models.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.DeleteNode(index, req.NodeID); err != nil {
		n.logger.Errorf("server.fsm: DeleteNode failed: %v", err)
		return err
	}
	return nil
}

func (n *udupFSM) applyStatusUpdate(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"server", "fsm", "node_status_update"}, time.Now())
	var req models.NodeUpdateStatusRequest
	if err := models.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.UpdateNodeStatus(index, req.NodeID, req.Status); err != nil {
		n.logger.Errorf("server.fsm: UpdateNodeStatus failed: %v", err)
		return err
	}

	// Unblock evals for the nodes computed node class if it is in a ready
	// store.
	if req.Status == models.NodeStatusReady {
		ws := memdb.NewWatchSet()
		node, err := n.state.NodeByID(ws, req.NodeID)
		if err != nil {
			n.logger.Errorf("server.fsm: looking up node %q failed: %v", req.NodeID, err)
			return err

		}
		n.blockedEvals.Unblock(node.ComputedClass, index)
	}

	if req.Status == models.NodeStatusDown {
		// Get all the jobs
		ws := memdb.NewWatchSet()
		jobs, err := n.state.Jobs(ws)
		if err != nil {
			return err
		}

		for {
			// Get the next item
			raw := jobs.Next()
			if raw == nil {
				break
			}

			// Prepare the request struct
			job := raw.(*models.Job)
			for _, task := range job.Tasks {
				if task.NodeID == req.NodeID {
					if job.Failover {
						// Scan the nodes
						ws := memdb.NewWatchSet()
						var out []*models.Node
						iter, err := n.state.Nodes(ws)
						if err != nil {
							return err
						}
						for {
							raw := iter.Next()
							if raw == nil {
								break
							}

							// Filter on datacenter and status
							node := raw.(*models.Node)
							if node.Status != models.NodeStatusReady {
								continue
							}
							out = append(out, node)
						}
						if len(out) > 0 {
							task.NodeID = out[0].ID
							if task.Type == models.TaskTypeDest {
								for i, t := range job.Tasks {
									t.Config["NatsAddr"] = out[0].NatsAddr
									job.Tasks[i] = t
								}
							}

							if err := n.state.UpsertJob(index, job); err != nil {
								n.logger.Errorf("server.fsm: UpsertJob failed: %v", err)
								return err
							}
						}

						allocs, err := n.state.AllocsByJob(ws, job.ID, true)
						if err != nil {
							return fmt.Errorf("failed to find allocs for '%s': %v", req.NodeID, err)
						}
						for _, alloc := range allocs {
							alloc.Job = job
							nodeID := alloc.NodeID
							node, err := n.state.NodeByID(ws, nodeID)
							if err != nil || node == nil {
								n.logger.Errorf("server.fsm: looking up node %q failed: %v", nodeID, err)
								return err

							}
							if node.Status == models.NodeStatusDown || alloc.Task == models.TaskTypeSrc {
								alloc.TaskStates[alloc.Task].State = models.TaskStateDead
								if len(out) > 0 {
									alloc.NodeID = out[0].ID
								}
								if err := n.state.UpsertAlloc(index, alloc); err != nil {
									n.logger.Errorf("server.fsm: UpsertAlloc failed: %v", err)
									return err
								}
							}
						}
					} else {
						allocs, err := n.state.AllocsByJob(ws, job.ID, true)
						if err != nil {
							return fmt.Errorf("failed to find allocs for '%s': %v", req.NodeID, err)
						}
						for _, alloc := range allocs {
							alloc.Job = job
							nodeID := alloc.NodeID
							node, err := n.state.NodeByID(ws, nodeID)
							if err != nil || node == nil {
								n.logger.Errorf("server.fsm: looking up node %q failed: %v", nodeID, err)
								return err

							}
							if node.Status == models.NodeStatusDown {
								alloc.TaskStates[alloc.Task].State = models.TaskStateDead
								if err := n.state.UpsertAlloc(index, alloc); err != nil {
									n.logger.Errorf("server.fsm: UpsertAlloc failed: %v", err)
									return err
								}
							}
						}
					}
				}
			}
		}
	}

	return nil
}

func (n *udupFSM) applyJobStatusUpdate(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"server", "fsm", "job_status_update"}, time.Now())
	var req models.JobUpdateStatusRequest
	if err := models.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.UpdateJobStatus(index, req.JobID, req.Status); err != nil {
		n.logger.Errorf("server.fsm: UpdateJobStatus failed: %v", err)
		return err
	}

	return nil
}

func (n *udupFSM) applyUpsertJob(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"server", "fsm", "register_job"}, time.Now())
	var req models.JobRegisterRequest
	if err := models.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	req.Job.Canonicalize()

	if err := n.state.UpsertJob(index, req.Job); err != nil {
		n.logger.Errorf("server.fsm: UpsertJob failed: %v", err)
		return err
	}

	return nil
}

func (n *udupFSM) applyRenewalJob(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"server", "fsm", "renewal_job"}, time.Now())
	var req models.JobRenewalRequest
	if err := models.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.RenewalJob(index, req.JobID, req.OrderID); err != nil {
		n.logger.Errorf("server.fsm: RenewalJob failed: %v", err)
		return err
	}

	return nil
}

func (n *udupFSM) applyDeregisterJob(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"server", "fsm", "deregister_job"}, time.Now())
	var req models.JobDeregisterRequest
	if err := models.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.DeleteJob(index, req.JobID); err != nil {
		n.logger.Errorf("server.fsm: DeleteJob failed: %v", err)
		return err
	}

	return nil
}

func (n *udupFSM) applyUpsertOrder(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"server", "fsm", "register_order"}, time.Now())
	var req models.OrderRegisterRequest
	if err := models.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.UpsertOrder(index, req.Order); err != nil {
		n.logger.Errorf("server.fsm: UpsertOrder failed: %v", err)
		return err
	}

	return nil
}

func (n *udupFSM) applyDeregisterOrder(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"server", "fsm", "deregister_order"}, time.Now())
	var req models.OrderDeregisterRequest
	if err := models.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.DeleteOrder(index, req.OrderID); err != nil {
		n.logger.Errorf("server.fsm: DeleteOrder failed: %v", err)
		return err
	}

	return nil
}

func (n *udupFSM) applyUpdateEval(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"server", "fsm", "update_eval"}, time.Now())
	var req models.EvalUpdateRequest
	if err := models.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.UpsertEvals(index, req.Evals); err != nil {
		n.logger.Errorf("server.fsm: UpsertEvals failed: %v", err)
		return err
	}

	for _, eval := range req.Evals {
		if eval.ShouldEnqueue() {
			n.evalBroker.Enqueue(eval)
		} else if eval.ShouldBlock() {
			n.blockedEvals.Block(eval)
		} else if eval.Status == models.EvalStatusComplete &&
			len(eval.FailedTGAllocs) == 0 {
			// If we have a successful evaluation for a node, untrack any
			// blocked evaluation
			n.blockedEvals.Untrack(eval.JobID)
		}
	}
	return nil
}

func (n *udupFSM) applyDeleteEval(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"server", "fsm", "delete_eval"}, time.Now())
	var req models.EvalDeleteRequest
	if err := models.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.DeleteEval(index, req.Evals, req.Allocs); err != nil {
		n.logger.Errorf("server.fsm: DeleteEval failed: %v", err)
		return err
	}
	return nil
}

func (n *udupFSM) applyAllocUpdate(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"server", "fsm", "alloc_update"}, time.Now())
	var req models.AllocUpdateRequest
	if err := models.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	// Attach the job to all the allocations. It is pulled out in the
	// payload to avoid the redundancy of encoding, but should be denormalized
	// prior to being inserted into MemDB.
	if j := req.Job; j != nil {
		for _, alloc := range req.Alloc {
			if alloc.Job == nil && !alloc.ClientTerminalStatus() {
				alloc.Job = j
			}
		}
	}

	if err := n.state.UpsertAllocs(index, req.Alloc); err != nil {
		n.logger.Errorf("server.fsm: UpsertAllocs failed: %v", err)
		return err
	}
	return nil
}

func (n *udupFSM) applyJobClientUpdate(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"server", "fsm", "job_client_update"}, time.Now())
	var req models.JobUpdateRequest
	if err := models.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}
	if len(req.JobUpdates) == 0 {
		return nil
	}

	// Create a watch set
	ws := memdb.NewWatchSet()

	// Updating the allocs with the job id and task name
	for _, ju := range req.JobUpdates {
		// Check if the job already exists
		if existing, _ := n.state.JobByID(ws, ju.JobID); existing != nil {
			if ju.Gtid != "" {
				existing.CreateIndex = existing.CreateIndex
				existing.ModifyIndex = index
				existing.JobModifyIndex = index
				for _, t := range existing.Tasks {
					t.Config["Gtid"] = ju.Gtid
					//t.Config["NatsAddr"] = ju.NatsAddr
				}
				// Update all the client allocations
				if err := n.state.UpdateJobFromClient(index, existing); err != nil {
					n.logger.Errorf("server.fsm: UpdateJobFromClient failed: %v", err)
					return err
				}
			} else {
				existing.CreateIndex = existing.CreateIndex
				existing.ModifyIndex = index
				existing.JobModifyIndex = index
				/*for _, t := range existing.Tasks {
					t.Config["NatsAddr"] = ju.NatsAddr
				}*/
				// Update all the client allocations
				if err := n.state.UpdateJobFromClient(index, existing); err != nil {
					n.logger.Errorf("server.fsm: UpdateJobFromClient failed: %v", err)
					return err
				}
			}
		}
	}

	return nil
}

func (n *udupFSM) applyAllocClientUpdate(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"server", "fsm", "alloc_client_update"}, time.Now())
	var req models.AllocUpdateRequest
	if err := models.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}
	if len(req.Alloc) == 0 {
		return nil
	}

	// Create a watch set
	ws := memdb.NewWatchSet()

	// Updating the allocs with the job id and task name
	for _, alloc := range req.Alloc {
		if existing, _ := n.state.AllocByID(ws, alloc.ID); existing != nil {
			alloc.JobID = existing.JobID
			alloc.Task = existing.Task
		}
	}

	// Update all the client allocations
	if err := n.state.UpdateAllocsFromClient(index, req.Alloc); err != nil {
		n.logger.Errorf("server.fsm: UpdateAllocFromClient failed: %v", err)
		return err
	}

	// Unblock evals for the nodes computed node class if the client has
	// finished running an allocation.
	for _, alloc := range req.Alloc {
		if alloc.ClientStatus == models.AllocClientStatusComplete ||
			alloc.ClientStatus == models.AllocClientStatusFailed {
			nodeID := alloc.NodeID
			node, err := n.state.NodeByID(ws, nodeID)
			if err != nil || node == nil {
				n.logger.Errorf("server.fsm: looking up node %q failed: %v", nodeID, err)
				return err

			}
			n.blockedEvals.Unblock(node.ComputedClass, index)
		}
	}

	return nil
}

func (n *udupFSM) Snapshot() (raft.FSMSnapshot, error) {
	// Create a new snapshot
	snap, err := n.state.Snapshot()
	if err != nil {
		return nil, err
	}

	ns := &udupSnapshot{
		snap:      snap,
		timetable: n.timetable,
	}
	return ns, nil
}

func (n *udupFSM) Restore(old io.ReadCloser) error {
	defer old.Close()

	// Create a new store store
	newState, err := store.NewStateStore(n.logOutput)
	if err != nil {
		return err
	}

	// Start the store restore
	restore, err := newState.Restore()
	if err != nil {
		return err
	}
	defer restore.Abort()

	// Create a decoder
	dec := codec.NewDecoder(old, models.MsgpackHandle)

	// Read in the header
	var header snapshotHeader
	if err := dec.Decode(&header); err != nil {
		return err
	}

	// Populate the new store
	msgType := make([]byte, 1)
	for {
		// Read the message type
		_, err := old.Read(msgType)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		// Decode
		switch SnapshotType(msgType[0]) {
		case TimeTableSnapshot:
			if err := n.timetable.Deserialize(dec); err != nil {
				return fmt.Errorf("time table deserialize failed: %v", err)
			}

		case NodeSnapshot:
			node := new(models.Node)
			if err := dec.Decode(node); err != nil {
				return err
			}
			if err := restore.NodeRestore(node); err != nil {
				return err
			}

		case JobSnapshot:
			job := new(models.Job)
			if err := dec.Decode(job); err != nil {
				return err
			}

			job.Canonicalize()

			if err := restore.JobRestore(job); err != nil {
				return err
			}

		case EvalSnapshot:
			eval := new(models.Evaluation)
			if err := dec.Decode(eval); err != nil {
				return err
			}
			if err := restore.EvalRestore(eval); err != nil {
				return err
			}

		case AllocSnapshot:
			alloc := new(models.Allocation)
			if err := dec.Decode(alloc); err != nil {
				return err
			}
			if err := restore.AllocRestore(alloc); err != nil {
				return err
			}

		case IndexSnapshot:
			idx := new(store.IndexEntry)
			if err := dec.Decode(idx); err != nil {
				return err
			}
			if err := restore.IndexRestore(idx); err != nil {
				return err
			}

		default:
			return fmt.Errorf("Unrecognized snapshot type: %v", msgType)
		}
	}

	restore.Commit()

	// External code might be calling State(), so we need to synchronize
	// here to make sure we swap in the new store store atomically.
	n.stateLock.Lock()
	stateOld := n.state
	n.state = newState
	n.stateLock.Unlock()

	// Signal that the old store store has been abandoned. This is required
	// because we don't operate on it any more, we just throw it away, so
	// blocking queries won't see any changes and need to be woken up.
	stateOld.Abandon()

	return nil
}

func (s *udupSnapshot) Persist(sink raft.SnapshotSink) error {
	defer metrics.MeasureSince([]string{"server", "fsm", "persist"}, time.Now())
	// Register the nodes
	encoder := codec.NewEncoder(sink, models.MsgpackHandle)

	// Write the header
	header := snapshotHeader{}
	if err := encoder.Encode(&header); err != nil {
		sink.Cancel()
		return err
	}

	// Write the time table
	sink.Write([]byte{byte(TimeTableSnapshot)})
	if err := s.timetable.Serialize(encoder); err != nil {
		sink.Cancel()
		return err
	}

	// Write all the data out
	if err := s.persistIndexes(sink, encoder); err != nil {
		sink.Cancel()
		return err
	}
	if err := s.persistNodes(sink, encoder); err != nil {
		sink.Cancel()
		return err
	}
	if err := s.persistJobs(sink, encoder); err != nil {
		sink.Cancel()
		return err
	}
	if err := s.persistEvals(sink, encoder); err != nil {
		sink.Cancel()
		return err
	}
	if err := s.persistAllocs(sink, encoder); err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (s *udupSnapshot) persistIndexes(sink raft.SnapshotSink,
	encoder *codec.Encoder) error {
	// Get all the indexes
	iter, err := s.snap.Indexes()
	if err != nil {
		return err
	}

	for {
		// Get the next item
		raw := iter.Next()
		if raw == nil {
			break
		}

		// Prepare the request struct
		idx := raw.(*store.IndexEntry)

		// Write out a node registration
		sink.Write([]byte{byte(IndexSnapshot)})
		if err := encoder.Encode(idx); err != nil {
			return err
		}
	}
	return nil
}

func (s *udupSnapshot) persistNodes(sink raft.SnapshotSink,
	encoder *codec.Encoder) error {
	// Get all the nodes
	ws := memdb.NewWatchSet()
	nodes, err := s.snap.Nodes(ws)
	if err != nil {
		return err
	}

	for {
		// Get the next item
		raw := nodes.Next()
		if raw == nil {
			break
		}

		// Prepare the request struct
		node := raw.(*models.Node)

		// Write out a node registration
		sink.Write([]byte{byte(NodeSnapshot)})
		if err := encoder.Encode(node); err != nil {
			return err
		}
	}
	return nil
}

func (s *udupSnapshot) persistJobs(sink raft.SnapshotSink,
	encoder *codec.Encoder) error {
	// Get all the jobs
	ws := memdb.NewWatchSet()
	jobs, err := s.snap.Jobs(ws)
	if err != nil {
		return err
	}

	for {
		// Get the next item
		raw := jobs.Next()
		if raw == nil {
			break
		}

		// Prepare the request struct
		job := raw.(*models.Job)

		// Write out a job registration
		sink.Write([]byte{byte(JobSnapshot)})
		if err := encoder.Encode(job); err != nil {
			return err
		}
	}
	return nil
}

func (s *udupSnapshot) persistEvals(sink raft.SnapshotSink,
	encoder *codec.Encoder) error {
	// Get all the evaluations
	ws := memdb.NewWatchSet()
	evals, err := s.snap.Evals(ws)
	if err != nil {
		return err
	}

	for {
		// Get the next item
		raw := evals.Next()
		if raw == nil {
			break
		}

		// Prepare the request struct
		eval := raw.(*models.Evaluation)

		// Write out the evaluation
		sink.Write([]byte{byte(EvalSnapshot)})
		if err := encoder.Encode(eval); err != nil {
			return err
		}
	}
	return nil
}

func (s *udupSnapshot) persistAllocs(sink raft.SnapshotSink,
	encoder *codec.Encoder) error {
	// Get all the allocations
	ws := memdb.NewWatchSet()
	allocs, err := s.snap.Allocs(ws)
	if err != nil {
		return err
	}

	for {
		// Get the next item
		raw := allocs.Next()
		if raw == nil {
			break
		}

		// Prepare the request struct
		alloc := raw.(*models.Allocation)

		// Write out the evaluation
		sink.Write([]byte{byte(AllocSnapshot)})
		if err := encoder.Encode(alloc); err != nil {
			return err
		}
	}
	return nil
}

// Release is a no-op, as we just need to GC the pointer
// to the store store snapshot. There is nothing to explicitly
// cleanup.
func (s *udupSnapshot) Release() {}
