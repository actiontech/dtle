package server

import (
	"fmt"
	"io"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/armon/go-metrics"
	memdb "github.com/hashicorp/go-memdb"
	"github.com/hashicorp/raft"
	"github.com/ugorji/go/codec"

	"udup/internal/models"
	"udup/internal/server/scheduler"
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
	JobSummarySnapshot
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
	blocked *BlockedEvals, logOutput io.Writer) (*udupFSM, error) {
	// Create a store store
	state, err := store.NewStateStore(logOutput)
	if err != nil {
		return nil, err
	}

	fsm := &udupFSM{
		evalBroker:   evalBroker,
		blockedEvals: blocked,
		logOutput:    logOutput,
		logger:       log.New(logOutput, "", log.LstdFlags),
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
	case models.EvalUpdateRequestType:
		return n.applyUpdateEval(buf[1:], log.Index)
	case models.EvalDeleteRequestType:
		return n.applyDeleteEval(buf[1:], log.Index)
	case models.AllocUpdateRequestType:
		return n.applyAllocUpdate(buf[1:], log.Index)
	case models.AllocClientUpdateRequestType:
		return n.applyAllocClientUpdate(buf[1:], log.Index)
	case models.ReconcileJobSummariesRequestType:
		return n.applyReconcileSummaries(buf[1:], log.Index)
	default:
		if ignoreUnknown {
			n.logger.Printf("[WARN] server.fsm: ignoring unknown message type (%d), upgrade to newer version", msgType)
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
		n.logger.Printf("[ERR] server.fsm: UpsertNode failed: %v", err)
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
		n.logger.Printf("[ERR] server.fsm: DeleteNode failed: %v", err)
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
		n.logger.Printf("[ERR] server.fsm: UpdateNodeStatus failed: %v", err)
		return err
	}

	// Unblock evals for the nodes computed node class if it is in a ready
	// store.
	if req.Status == models.NodeStatusReady {
		ws := memdb.NewWatchSet()
		node, err := n.state.NodeByID(ws, req.NodeID)
		if err != nil {
			n.logger.Printf("[ERR] server.fsm: looking up node %q failed: %v", req.NodeID, err)
			return err

		}
		n.blockedEvals.Unblock(node.ComputedClass, index)
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
		n.logger.Printf("[ERR] server.fsm: UpdateJobStatus failed: %v", err)
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
		n.logger.Printf("[ERR] server.fsm: UpsertJob failed: %v", err)
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
		n.logger.Printf("[ERR] server.fsm: DeleteJob failed: %v", err)
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
		n.logger.Printf("[ERR] server.fsm: UpsertEvals failed: %v", err)
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
		n.logger.Printf("[ERR] server.fsm: DeleteEval failed: %v", err)
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
		n.logger.Printf("[ERR] server.fsm: UpsertAllocs failed: %v", err)
		return err
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
		n.logger.Printf("[ERR] server.fsm: UpdateAllocFromClient failed: %v", err)
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
				n.logger.Printf("[ERR] server.fsm: looking up node %q failed: %v", nodeID, err)
				return err

			}
			n.blockedEvals.Unblock(node.ComputedClass, index)
		}
	}

	return nil
}

// applyReconcileSummaries reconciles summaries for all the jobs
func (n *udupFSM) applyReconcileSummaries(buf []byte, index uint64) interface{} {
	if err := n.state.ReconcileJobSummaries(index); err != nil {
		return err
	}
	return n.reconcileQueuedAllocations(index)
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

		case JobSummarySnapshot:
			summary := new(models.JobSummary)
			if err := dec.Decode(summary); err != nil {
				return err
			}
			if err := restore.JobSummaryRestore(summary); err != nil {
				return err
			}

		default:
			return fmt.Errorf("Unrecognized snapshot type: %v", msgType)
		}
	}

	restore.Commit()

	// Create Job Summaries
	// COMPAT 0.4 -> 0.4.1
	// We can remove this in 0.5. This exists so that the server creates job
	// summaries if they were not present previously. When users upgrade to 0.5
	// from 0.4.1, the snapshot will contain job summaries so it will be safe to
	// remove this block.
	index, err := newState.Index("job_summary")
	if err != nil {
		return fmt.Errorf("couldn't fetch index of job summary table: %v", err)
	}

	// If the index is 0 that means there is no job summary in the snapshot so
	// we will have to create them
	if index == 0 {
		// query the latest index
		latestIndex, err := newState.LatestIndex()
		if err != nil {
			return fmt.Errorf("unable to query latest index: %v", index)
		}
		if err := newState.ReconcileJobSummaries(latestIndex); err != nil {
			return fmt.Errorf("error reconciling summaries: %v", err)
		}
	}

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

// reconcileSummaries re-calculates the queued allocations for every job that we
// created a Job Summary during the snap shot restore
func (n *udupFSM) reconcileQueuedAllocations(index uint64) error {
	// Get all the jobs
	ws := memdb.NewWatchSet()
	iter, err := n.state.Jobs(ws)
	if err != nil {
		return err
	}

	snap, err := n.state.Snapshot()
	if err != nil {
		return fmt.Errorf("unable to create snapshot: %v", err)
	}

	// Invoking the scheduler for every job so that we can populate the number
	// of queued allocations for every job
	for {
		rawJob := iter.Next()
		if rawJob == nil {
			break
		}
		job := rawJob.(*models.Job)
		planner := &scheduler.Harness{
			State: &snap.StateStore,
		}
		// Create an eval and mark it as requiring annotations and insert that as well
		eval := &models.Evaluation{
			ID:             models.GenerateUUID(),
			Type:           job.Type,
			TriggeredBy:    models.EvalTriggerJobRegister,
			JobID:          job.ID,
			JobModifyIndex: job.JobModifyIndex + 1,
			Status:         models.EvalStatusPending,
			AnnotatePlan:   true,
		}

		// Create the scheduler and run it
		sched, err := scheduler.NewScheduler(eval.Type, n.logger, snap, planner)
		if err != nil {
			return err
		}

		if err := sched.Process(eval); err != nil {
			return err
		}

		// Get the job summary from the fsm store store
		originalSummary, err := n.state.JobSummaryByID(ws, job.ID)
		if err != nil {
			return err
		}
		summary := originalSummary.Copy()

		// Add the allocations scheduler has made to queued since these
		// allocations are never getting placed until the scheduler is invoked
		// with a real planner
		if l := len(planner.Plans); l != 1 {
			return fmt.Errorf("unexpected number of plans during restore %d. Please file an issue including the logs", l)
		}
		for _, allocations := range planner.Plans[0].NodeAllocation {
			for _, allocation := range allocations {
				tgSummary, ok := summary.Tasks[allocation.Task]
				if !ok {
					return fmt.Errorf("task %q not found while updating queued count", allocation.Task)
				}
				tgSummary.Status = models.TaskStateQueued
				summary.Tasks[allocation.Task] = tgSummary
			}
		}

		// Add the queued allocations attached to the evaluation to the queued
		// counter of the job summary
		if l := len(planner.Evals); l != 1 {
			return fmt.Errorf("unexpected number of evals during restore %d. Please file an issue including the logs", l)
		}
		for t, _ := range planner.Evals[0].QueuedAllocations {
			taskSummary, ok := summary.Tasks[t]
			if !ok {
				return fmt.Errorf("task %q not found while updating queued count", t)
			}

			// We add instead of setting here because we want to take into
			// consideration what the scheduler with a mock planner thinks it
			// placed. Those should be counted as queued as well
			taskSummary.Status = models.TaskStateQueued
			summary.Tasks[t] = taskSummary
		}

		if !reflect.DeepEqual(summary, originalSummary) {
			summary.ModifyIndex = index
			if err := n.state.UpsertJobSummary(index, summary); err != nil {
				return err
			}
		}
	}
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
	if err := s.persistJobSummaries(sink, encoder); err != nil {
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

func (s *udupSnapshot) persistJobSummaries(sink raft.SnapshotSink,
	encoder *codec.Encoder) error {

	ws := memdb.NewWatchSet()
	summaries, err := s.snap.JobSummaries(ws)
	if err != nil {
		return err
	}

	for {
		raw := summaries.Next()
		if raw == nil {
			break
		}

		jobSummary := raw.(*models.JobSummary)

		sink.Write([]byte{byte(JobSummarySnapshot)})
		if err := encoder.Encode(jobSummary); err != nil {
			return err
		}
	}
	return nil
}

// Release is a no-op, as we just need to GC the pointer
// to the store store snapshot. There is nothing to explicitly
// cleanup.
func (s *udupSnapshot) Release() {}
