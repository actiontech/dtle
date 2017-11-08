package store

import (
	"fmt"
	"io"
	"log"

	"github.com/hashicorp/go-memdb"

	"udup/internal/models"
)

// IndexEntry is used with the "index" table
// for managing the latest Raft index affecting a table.
type IndexEntry struct {
	Key   string
	Value uint64
}

// The StateStore is responsible for maintaining all the Udup
// state. It is manipulated by the FSM which maintains consistency
// through the use of Raft. The goals of the StateStore are to provide
// high concurrency for read operations without blocking writes, and
// to provide write availability in the face of reads. EVERY object
// returned as a result of a read against the state store should be
// considered a constant and NEVER modified in place.
type StateStore struct {
	logger *log.Logger
	db     *memdb.MemDB

	// abandonCh is used to signal watchers that this state store has been
	// abandoned (usually during a restore). This is only ever closed.
	abandonCh chan struct{}
}

// NewStateStore is used to create a new state store
func NewStateStore(logOutput io.Writer) (*StateStore, error) {
	// Create the MemDB
	db, err := memdb.NewMemDB(stateStoreSchema())
	if err != nil {
		return nil, fmt.Errorf("state store setup failed: %v", err)
	}

	// Create the state store
	s := &StateStore{
		logger:    log.New(logOutput, "", log.LstdFlags|log.Lmicroseconds),
		db:        db,
		abandonCh: make(chan struct{}),
	}
	return s, nil
}

// Snapshot is used to create a point in time snapshot. Because
// we use MemDB, we just need to snapshot the state of the underlying
// database.
func (s *StateStore) Snapshot() (*StateSnapshot, error) {
	snap := &StateSnapshot{
		StateStore: StateStore{
			logger: s.logger,
			db:     s.db.Snapshot(),
		},
	}
	return snap, nil
}

// Restore is used to optimize the efficiency of rebuilding
// state by minimizing the number of transactions and checking
// overhead.
func (s *StateStore) Restore() (*StateRestore, error) {
	txn := s.db.Txn(true)
	r := &StateRestore{
		txn: txn,
	}
	return r, nil
}

// AbandonCh returns a channel you can wait on to know if the state store was
// abandoned.
func (s *StateStore) AbandonCh() <-chan struct{} {
	return s.abandonCh
}

// Abandon is used to signal that the given state store has been abandoned.
// Calling this more than one time will panic.
func (s *StateStore) Abandon() {
	close(s.abandonCh)
}

// UpsertNode is used to register a node or update a node definition
// This is assumed to be triggered by the client, so we retain the value
// of drain which is set by the scheduler.
func (s *StateStore) UpsertNode(index uint64, node *models.Node) error {
	txn := s.db.Txn(true)
	defer txn.Abort()

	// Check if the node already exists
	existing, err := txn.First("nodes", "id", node.ID)
	if err != nil {
		return fmt.Errorf("node lookup failed: %v", err)
	}

	// Setup the indexes correctly
	if existing != nil {
		exist := existing.(*models.Node)
		node.CreateIndex = exist.CreateIndex
		node.ModifyIndex = index
	} else {
		node.CreateIndex = index
		node.ModifyIndex = index
	}

	// Insert the node
	if err := txn.Insert("nodes", node); err != nil {
		return fmt.Errorf("node insert failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"nodes", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	txn.Commit()
	return nil
}

// DeleteNode is used to deregister a node
func (s *StateStore) DeleteNode(index uint64, nodeID string) error {
	txn := s.db.Txn(true)
	defer txn.Abort()

	// Lookup the node
	existing, err := txn.First("nodes", "id", nodeID)
	if err != nil {
		return fmt.Errorf("node lookup failed: %v", err)
	}
	if existing == nil {
		return fmt.Errorf("node not found")
	}

	// Delete the node
	if err := txn.Delete("nodes", existing); err != nil {
		return fmt.Errorf("node delete failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"nodes", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	txn.Commit()
	return nil
}

func (s *StateStore) UpdateJobStatus(index uint64, jobID, status string) error {
	txn := s.db.Txn(true)
	defer txn.Abort()

	// Check if the job already exists
	existing, err := txn.First("jobs", "id", jobID)
	if err != nil {
		return fmt.Errorf("job lookup failed: %v", err)
	}

	if existing == nil {
		return fmt.Errorf("job not found")
	}

	// Copy the existing job
	existingJob := existing.(*models.Job)
	copyJob := new(models.Job)
	*copyJob = *existingJob

	// Update the status in the copy
	copyJob.Status = status
	copyJob.ModifyIndex = index
	copyJob.JobModifyIndex = index

	// Insert the job
	if err := txn.Insert("jobs", copyJob); err != nil {
		return fmt.Errorf("job insert failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"jobs", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	txn.Commit()
	return nil
}

// UpdateNodeStatus is used to update the status of a node
func (s *StateStore) UpdateNodeStatus(index uint64, nodeID, status string) error {
	txn := s.db.Txn(true)
	defer txn.Abort()

	// Lookup the node
	existing, err := txn.First("nodes", "id", nodeID)
	if err != nil {
		return fmt.Errorf("node lookup failed: %v", err)
	}
	if existing == nil {
		return fmt.Errorf("node not found")
	}

	// Copy the existing node
	existingNode := existing.(*models.Node)
	copyNode := new(models.Node)
	*copyNode = *existingNode

	// Update the status in the copy
	copyNode.Status = status
	copyNode.ModifyIndex = index

	// Insert the node
	if err := txn.Insert("nodes", copyNode); err != nil {
		return fmt.Errorf("node update failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"nodes", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	txn.Commit()
	return nil
}

// NodeByID is used to lookup a node by ID
func (s *StateStore) NodeByID(ws memdb.WatchSet, nodeID string) (*models.Node, error) {
	txn := s.db.Txn(false)

	watchCh, existing, err := txn.FirstWatch("nodes", "id", nodeID)
	if err != nil {
		return nil, fmt.Errorf("node lookup failed: %v", err)
	}
	ws.Add(watchCh)

	if existing != nil {
		return existing.(*models.Node), nil
	}
	return nil, nil
}

// NodesByIDPrefix is used to lookup nodes by prefix
func (s *StateStore) NodesByIDPrefix(ws memdb.WatchSet, nodeID string) (memdb.ResultIterator, error) {
	txn := s.db.Txn(false)

	iter, err := txn.Get("nodes", "id_prefix", nodeID)
	if err != nil {
		return nil, fmt.Errorf("node lookup failed: %v", err)
	}
	ws.Add(iter.WatchCh())

	return iter, nil
}

// parseNodes takes an iterator over a set of nodes and returns a struct
// containing the nodes along with all of their associated services
// and/or health checks.
func (s *StateStore) parseNodes(tx *memdb.Txn, iter memdb.ResultIterator) (models.NodeDump, error) {
	var results models.NodeDump
	for n := iter.Next(); n != nil; n = iter.Next() {
		node := n.(*models.Node)

		// Create the wrapped node
		dump := &models.NodeInfo{
			Node:    node.Name,
			Address: node.HTTPAddr,
		}

		// Query the node services
		/*services, err := tx.Get("services", "node", node.Name)
		if err != nil {
			return nil, fmt.Errorf("failed services lookup: %s", err)
		}
		for service := services.Next(); service != nil; service = services.Next() {
			ns := service.(*models.ServiceNode).ToNodeService()
			dump.Services = append(dump.Services, ns)
		}

		// Query the node checks
		checks, err := tx.Get("checks", "node", node.Name)
		if err != nil {
			return nil, fmt.Errorf("failed node lookup: %s", err)
		}
		for check := checks.Next(); check != nil; check = checks.Next() {
			hc := check.(*models.HealthCheck)
			dump.Checks = append(dump.Checks, hc)
		}*/

		// Add the result to the slice
		results = append(results, dump)
	}
	return results, nil
}

// Coordinates queries for all nodes with coordinates.
func (s *StateStore) Coordinates() (models.Coordinates, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	// Pull all the coordinates.
	coords, err := tx.Get("coordinates", "id")
	if err != nil {
		return nil, fmt.Errorf("failed coordinate lookup: %s", err)
	}
	var results models.Coordinates
	for coord := coords.Next(); coord != nil; coord = coords.Next() {
		results = append(results, coord.(*models.Coordinate))
	}
	return results, nil
}

// NodeInfo is used to generate a dump of a single node. The dump includes
// all services and checks which are registered against the node.
func (s *StateStore) NodeInfo(node string) (models.NodeDump, error) {
	txn := s.db.Txn(false)
	defer txn.Abort()

	// Query the node by the passed node
	iter, err := txn.Get("nodes", "id", node)
	if err != nil {
		return nil, fmt.Errorf("failed node lookup: %s", err)
	}
	return s.parseNodes(txn, iter)
}

// NodeDump is used to generate a dump of all nodes. This call is expensive
// as it has to query every node, service, and check. The response can also
// be quite large since there is currently no filtering applied.
func (s *StateStore) NodeDump() (models.NodeDump, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	// Fetch all of the registered nodes
	nodes, err := tx.Get("nodes", "id")
	if err != nil {
		return nil, fmt.Errorf("failed node lookup: %s", err)
	}
	return s.parseNodes(tx, nodes)
}

// Nodes returns an iterator over all the nodes
func (s *StateStore) Nodes(ws memdb.WatchSet) (memdb.ResultIterator, error) {
	txn := s.db.Txn(false)

	// Walk the entire nodes table
	iter, err := txn.Get("nodes", "id")
	if err != nil {
		return nil, err
	}
	ws.Add(iter.WatchCh())
	return iter, nil
}

// UpsertJob is used to register a job or update a job definition
func (s *StateStore) UpsertJob(index uint64, job *models.Job) error {
	txn := s.db.Txn(true)
	defer txn.Abort()

	// Check if the job already exists
	existing, err := txn.First("jobs", "id", job.ID)
	if err != nil {
		return fmt.Errorf("job lookup failed: %v", err)
	}

	// Setup the indexes correctly
	if existing != nil {
		if existing.(*models.Job).Status == models.JobStatusRunning {
			return nil
		}
		job.CreateIndex = existing.(*models.Job).CreateIndex
		job.ModifyIndex = index
		job.JobModifyIndex = index
		for _, t1 := range existing.(*models.Job).Tasks {
			for i, t2 := range job.Tasks {
				if t1.Type == t2.Type && t2.Config["NatsAddr"] == nil {
					t2.Config["NatsAddr"] = t1.Config["NatsAddr"]
					job.Tasks[i] = t2
				}
			}
		}

		// Compute the job status
		var err error
		job.Status, err = s.getJobStatus(txn, job, false)
		if err != nil {
			return fmt.Errorf("setting job status for %q failed: %v", job.ID, err)
		}
	} else {
		job.CreateIndex = index
		job.ModifyIndex = index
		job.JobModifyIndex = index

		if err := s.setJobStatus(index, txn, job, false, ""); err != nil {
			return fmt.Errorf("setting job status for %q failed: %v", job.ID, err)
		}

		// Have to get the job again since it could have been updated
		updated, err := txn.First("jobs", "id", job.ID)
		if err != nil {
			return fmt.Errorf("job lookup failed: %v", err)
		}
		if updated != nil {
			job = updated.(*models.Job)
		}
	}

	// Insert the job
	if err := txn.Insert("jobs", job); err != nil {
		return fmt.Errorf("job insert failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"jobs", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	txn.Commit()
	return nil
}

// DeleteJob is used to deregister a job
func (s *StateStore) DeleteJob(index uint64, jobID string) error {
	txn := s.db.Txn(true)
	defer txn.Abort()

	eval, err := txn.Get("evals", "job", jobID, models.EvalStatusComplete)
	if err != nil {
		return fmt.Errorf("failed to get blocked evals for job %q: %v", jobID, err)
	}

	for {
		raw := eval.Next()
		if raw == nil {
			break
		}
		existing, err := txn.First("evals", "id", raw.(*models.Evaluation).ID)
		if err != nil {
			return fmt.Errorf("eval lookup failed: %v", err)
		}
		if existing == nil {
			continue
		}
		if err := txn.Delete("evals", existing); err != nil {
			return fmt.Errorf("eval delete failed: %v", err)
		}
	}

	alloc, err := txn.Get("allocs", "job", jobID)
	if err != nil {
		return fmt.Errorf("failed to get blocked allocs for job %q: %v", jobID, err)
	}

	for {
		raw := alloc.Next()
		if raw == nil {
			break
		}
		existing, err := txn.First("allocs", "id", raw.(*models.Allocation).ID)
		if err != nil {
			return fmt.Errorf("alloc lookup failed: %v", err)
		}
		if existing == nil {
			continue
		}
		if err := txn.Delete("allocs", existing); err != nil {
			return fmt.Errorf("alloc delete failed: %v", err)
		}
	}

	// Update the indexes
	if err := txn.Insert("index", &IndexEntry{"evals", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"allocs", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	// Lookup the node
	existing, err := txn.First("jobs", "id", jobID)
	if err != nil {
		return fmt.Errorf("job lookup failed: %v", err)
	}
	if existing == nil {
		return fmt.Errorf("job not found")
	}

	// Delete the job
	job := existing.(*models.Job)

	if err := txn.Delete("jobs", job); err != nil {
		return fmt.Errorf("job delete failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"jobs", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	txn.Commit()
	return nil
}

// JobByID is used to lookup a job by its ID
func (s *StateStore) JobByID(ws memdb.WatchSet, id string) (*models.Job, error) {
	txn := s.db.Txn(false)

	watchCh, existing, err := txn.FirstWatch("jobs", "id", id)
	if err != nil {
		return nil, fmt.Errorf("job lookup failed: %v", err)
	}
	ws.Add(watchCh)

	if existing != nil {
		return existing.(*models.Job), nil
	}
	return nil, nil
}

// JobsByIDPrefix is used to lookup a job by prefix
func (s *StateStore) JobsByIDPrefix(ws memdb.WatchSet, id string) (memdb.ResultIterator, error) {
	txn := s.db.Txn(false)

	iter, err := txn.Get("jobs", "id_prefix", id)
	if err != nil {
		return nil, fmt.Errorf("job lookup failed: %v", err)
	}

	ws.Add(iter.WatchCh())

	return iter, nil
}

// Jobs returns an iterator over all the jobs
func (s *StateStore) Jobs(ws memdb.WatchSet) (memdb.ResultIterator, error) {
	txn := s.db.Txn(false)

	// Walk the entire jobs table
	iter, err := txn.Get("jobs", "id")
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	return iter, nil
}

// JobsByScheduler returns an iterator over all the jobs with the specific
// scheduler type.
func (s *StateStore) JobsByScheduler(ws memdb.WatchSet, schedulerType string) (memdb.ResultIterator, error) {
	txn := s.db.Txn(false)

	// Return an iterator for jobs with the specific type.
	iter, err := txn.Get("jobs", "type", schedulerType)
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	return iter, nil
}

// UpsertEvals is used to upsert a set of evaluations
func (s *StateStore) UpsertEvals(index uint64, evals []*models.Evaluation) error {
	txn := s.db.Txn(true)
	defer txn.Abort()

	// Do a nested upsert
	jobs := make(map[string]string, len(evals))
	for _, eval := range evals {
		if err := s.nestedUpsertEval(txn, index, eval); err != nil {
			return err
		}

		jobs[eval.JobID] = ""
	}

	// Set the job's status
	if err := s.setJobStatuses(index, txn, jobs, false); err != nil {
		return fmt.Errorf("setting job status failed: %v", err)
	}

	txn.Commit()
	return nil
}

// nestedUpsertEvaluation is used to nest an evaluation upsert within a transaction
func (s *StateStore) nestedUpsertEval(txn *memdb.Txn, index uint64, eval *models.Evaluation) error {
	// Lookup the evaluation
	existing, err := txn.First("evals", "id", eval.ID)
	if err != nil {
		return fmt.Errorf("eval lookup failed: %v", err)
	}

	// Update the indexes
	if existing != nil {
		eval.CreateIndex = existing.(*models.Evaluation).CreateIndex
		eval.ModifyIndex = index
	} else {
		eval.CreateIndex = index
		eval.ModifyIndex = index
	}

	// Check if the job has any blocked evaluations and cancel them
	if eval.Status == models.EvalStatusComplete && len(eval.FailedTGAllocs) == 0 {
		// Get the blocked evaluation for a job if it exists
		iter, err := txn.Get("evals", "job", eval.JobID, models.EvalStatusBlocked)
		if err != nil {
			return fmt.Errorf("failed to get blocked evals for job %q: %v", eval.JobID, err)
		}

		var blocked []*models.Evaluation
		for {
			raw := iter.Next()
			if raw == nil {
				break
			}
			blocked = append(blocked, raw.(*models.Evaluation))
		}

		// Go through and update the evals
		for _, eval := range blocked {
			newEval := eval.Copy()
			newEval.Status = models.EvalStatusCancelled
			newEval.StatusDescription = fmt.Sprintf("evaluation %q successful", newEval.ID)
			newEval.ModifyIndex = index
			if err := txn.Insert("evals", newEval); err != nil {
				return fmt.Errorf("eval insert failed: %v", err)
			}
		}
	}

	// Insert the eval
	if err := txn.Insert("evals", eval); err != nil {
		return fmt.Errorf("eval insert failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"evals", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}
	return nil
}

// DeleteEval is used to delete an evaluation
func (s *StateStore) DeleteEval(index uint64, evals []string, allocs []string) error {
	txn := s.db.Txn(true)
	defer txn.Abort()

	jobs := make(map[string]string, len(evals))
	for _, eval := range evals {
		existing, err := txn.First("evals", "id", eval)
		if err != nil {
			return fmt.Errorf("eval lookup failed: %v", err)
		}
		if existing == nil {
			continue
		}
		if err := txn.Delete("evals", existing); err != nil {
			return fmt.Errorf("eval delete failed: %v", err)
		}
		jobID := existing.(*models.Evaluation).JobID
		jobs[jobID] = ""
	}

	for _, alloc := range allocs {
		existing, err := txn.First("allocs", "id", alloc)
		if err != nil {
			return fmt.Errorf("alloc lookup failed: %v", err)
		}
		if existing == nil {
			continue
		}
		if err := txn.Delete("allocs", existing); err != nil {
			return fmt.Errorf("alloc delete failed: %v", err)
		}
	}

	// Update the indexes
	if err := txn.Insert("index", &IndexEntry{"evals", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"allocs", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	// Set the job's status
	if err := s.setJobStatuses(index, txn, jobs, true); err != nil {
		return fmt.Errorf("setting job status failed: %v", err)
	}

	txn.Commit()
	return nil
}

// EvalByID is used to lookup an eval by its ID
func (s *StateStore) EvalByID(ws memdb.WatchSet, id string) (*models.Evaluation, error) {
	txn := s.db.Txn(false)

	watchCh, existing, err := txn.FirstWatch("evals", "id", id)
	if err != nil {
		return nil, fmt.Errorf("eval lookup failed: %v", err)
	}

	ws.Add(watchCh)

	if existing != nil {
		return existing.(*models.Evaluation), nil
	}
	return nil, nil
}

// EvalsByIDPrefix is used to lookup evaluations by prefix
func (s *StateStore) EvalsByIDPrefix(ws memdb.WatchSet, id string) (memdb.ResultIterator, error) {
	txn := s.db.Txn(false)

	iter, err := txn.Get("evals", "id_prefix", id)
	if err != nil {
		return nil, fmt.Errorf("eval lookup failed: %v", err)
	}

	ws.Add(iter.WatchCh())

	return iter, nil
}

// EvalsByJob returns all the evaluations by job id
func (s *StateStore) EvalsByJob(ws memdb.WatchSet, jobID string) ([]*models.Evaluation, error) {
	txn := s.db.Txn(false)

	// Get an iterator over the node allocations
	iter, err := txn.Get("evals", "job_prefix", jobID)
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	var out []*models.Evaluation
	for {
		raw := iter.Next()
		if raw == nil {
			break
		}

		e := raw.(*models.Evaluation)

		// Filter non-exact matches
		if e.JobID != jobID {
			continue
		}

		out = append(out, e)
	}
	return out, nil
}

// Evals returns an iterator over all the evaluations
func (s *StateStore) Evals(ws memdb.WatchSet) (memdb.ResultIterator, error) {
	txn := s.db.Txn(false)

	// Walk the entire table
	iter, err := txn.Get("evals", "id")
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	return iter, nil
}

func (s *StateStore) UpdateJobFromClient(index uint64, job *models.Job) error {
	txn := s.db.Txn(true)
	defer txn.Abort()

	// Insert the job
	if err := txn.Insert("jobs", job); err != nil {
		return fmt.Errorf("job insert failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"jobs", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	txn.Commit()
	return nil
}

// UpdateAllocsFromClient is used to update an allocation based on input
// from a client. While the schedulers are the authority on the allocation for
// most things, some updates are authoritative from the client. Specifically,
// the desired store comes from the schedulers, while the actual store comes
// from clients.
func (s *StateStore) UpdateAllocsFromClient(index uint64, allocs []*models.Allocation) error {
	txn := s.db.Txn(true)
	defer txn.Abort()

	// Handle each of the updated allocations
	for _, alloc := range allocs {
		if err := s.nestedUpdateAllocFromClient(txn, index, alloc); err != nil {
			return err
		}
	}

	// Update the indexes
	if err := txn.Insert("index", &IndexEntry{"allocs", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	txn.Commit()
	return nil
}

// nestedUpdateAllocFromClient is used to nest an update of an allocation with client status
func (s *StateStore) nestedUpdateAllocFromClient(txn *memdb.Txn, index uint64, alloc *models.Allocation) error {
	// Look for existing alloc
	existing, err := txn.First("allocs", "id", alloc.ID)
	if err != nil {
		return fmt.Errorf("alloc lookup failed: %v", err)
	}

	// Nothing to do if this does not exist
	if existing == nil {
		return nil
	}
	exist := existing.(*models.Allocation)

	// Copy everything from the existing allocation
	copyAlloc := exist.Copy()

	// Pull in anything the client is the authority on
	//if exist.DesiredStatus != models.AllocDesiredStatusPause {
	copyAlloc.ClientStatus = alloc.ClientStatus
	copyAlloc.ClientDescription = alloc.ClientDescription
	copyAlloc.TaskStates = alloc.TaskStates
	//}

	// Update the modify index
	copyAlloc.ModifyIndex = index

	// Update the allocation
	if err := txn.Insert("allocs", copyAlloc); err != nil {
		return fmt.Errorf("alloc insert failed: %v", err)
	}

	// Set the job's status
	forceStatus := ""
	if !copyAlloc.ClientTerminalStatus() {
		switch copyAlloc.ClientStatus {
		case models.AllocClientStatusPending:
			forceStatus = models.JobStatusPending
		default:
			forceStatus = models.JobStatusRunning
		}
	} else {
		forceStatus = models.JobStatusDead
	}
	jobs := map[string]string{exist.JobID: forceStatus}
	if err := s.setJobStatuses(index, txn, jobs, false); err != nil {
		return fmt.Errorf("setting job status failed: %v", err)
	}
	return nil
}

// UpsertAllocs is used to evict a set of allocations
// and allocate new ones at the same time.
func (s *StateStore) UpsertAllocs(index uint64, allocs []*models.Allocation) error {
	txn := s.db.Txn(true)
	defer txn.Abort()

	// Handle the allocations
	jobs := make(map[string]string, 1)
	for _, alloc := range allocs {
		existing, err := txn.First("allocs", "id", alloc.ID)
		if err != nil {
			return fmt.Errorf("alloc lookup failed: %v", err)
		}
		exist, _ := existing.(*models.Allocation)

		if exist == nil {
			alloc.CreateIndex = index
			alloc.ModifyIndex = index
			alloc.AllocModifyIndex = index
		} else {
			alloc.CreateIndex = exist.CreateIndex
			alloc.ModifyIndex = index
			alloc.AllocModifyIndex = index

			// If the scheduler is marking this allocation as lost we do not
			// want to reuse the status of the existing allocation.
			if alloc.ClientStatus != models.AllocClientStatusLost {
				alloc.ClientStatus = exist.ClientStatus
				alloc.ClientDescription = exist.ClientDescription
			}

			// The job has been denormalized so re-attach the original job
			if alloc.Job == nil {
				alloc.Job = exist.Job
			}
		}

		if err := txn.Insert("allocs", alloc); err != nil {
			return fmt.Errorf("alloc insert failed: %v", err)
		}

		// If the allocation is running, force the job to running status.
		forceStatus := ""
		if !alloc.ClientTerminalStatus() {
			forceStatus = models.JobStatusRunning
		}
		jobs[alloc.JobID] = forceStatus
	}

	// Update the indexes
	if err := txn.Insert("index", &IndexEntry{"allocs", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	// Set the job's status
	if err := s.setJobStatuses(index, txn, jobs, false); err != nil {
		return fmt.Errorf("setting job status failed: %v", err)
	}

	txn.Commit()
	return nil
}

// AllocByID is used to lookup an allocation by its ID
func (s *StateStore) AllocByID(ws memdb.WatchSet, id string) (*models.Allocation, error) {
	txn := s.db.Txn(false)

	watchCh, existing, err := txn.FirstWatch("allocs", "id", id)
	if err != nil {
		return nil, fmt.Errorf("alloc lookup failed: %v", err)
	}

	ws.Add(watchCh)

	if existing != nil {
		return existing.(*models.Allocation), nil
	}

	return nil, nil
}

// AllocsByIDPrefix is used to lookup allocs by prefix
func (s *StateStore) AllocsByIDPrefix(ws memdb.WatchSet, id string) (memdb.ResultIterator, error) {
	txn := s.db.Txn(false)

	iter, err := txn.Get("allocs", "id_prefix", id)
	if err != nil {
		return nil, fmt.Errorf("alloc lookup failed: %v", err)
	}

	ws.Add(iter.WatchCh())

	return iter, nil
}

// AllocsByNode returns all the allocations by node
func (s *StateStore) AllocsByNode(ws memdb.WatchSet, node string) ([]*models.Allocation, error) {
	txn := s.db.Txn(false)

	// Get an iterator over the node allocations, using only the
	// node prefix which ignores the terminal status
	iter, err := txn.Get("allocs", "node_prefix", node)
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	var out []*models.Allocation
	for {
		raw := iter.Next()
		if raw == nil {
			break
		}
		out = append(out, raw.(*models.Allocation))
	}
	return out, nil
}

// AllocsByNode returns all the allocations by node and terminal status
func (s *StateStore) AllocsByNodeTerminal(ws memdb.WatchSet, node string, terminal bool) ([]*models.Allocation, error) {
	txn := s.db.Txn(false)

	// Get an iterator over the node allocations
	iter, err := txn.Get("allocs", "node", node, terminal)
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	var out []*models.Allocation
	for {
		raw := iter.Next()
		if raw == nil {
			break
		}
		out = append(out, raw.(*models.Allocation))
	}
	return out, nil
}

// AllocsByJob returns all the allocations by job id
func (s *StateStore) AllocsByJob(ws memdb.WatchSet, jobID string, all bool) ([]*models.Allocation, error) {
	txn := s.db.Txn(false)

	// Get the job
	var job *models.Job
	rawJob, err := txn.First("jobs", "id", jobID)
	if err != nil {
		return nil, err
	}
	if rawJob != nil {
		job = rawJob.(*models.Job)
	}

	// Get an iterator over the node allocations
	iter, err := txn.Get("allocs", "job", jobID)
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	var out []*models.Allocation
	for {
		raw := iter.Next()
		if raw == nil {
			break
		}

		alloc := raw.(*models.Allocation)
		// If the allocation belongs to a job with the same ID but a different
		// create index and we are not getting all the allocations whose Jobs
		// matches the same Job ID then we skip it
		if !all && job != nil && alloc.Job.CreateIndex != job.CreateIndex {
			continue
		}
		out = append(out, raw.(*models.Allocation))
	}
	return out, nil
}

// AllocsByEval returns all the allocations by eval id
func (s *StateStore) AllocsByEval(ws memdb.WatchSet, evalID string) ([]*models.Allocation, error) {
	txn := s.db.Txn(false)

	// Get an iterator over the eval allocations
	iter, err := txn.Get("allocs", "eval", evalID)
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	var out []*models.Allocation
	for {
		raw := iter.Next()
		if raw == nil {
			break
		}
		out = append(out, raw.(*models.Allocation))
	}
	return out, nil
}

// Allocs returns an iterator over all the evaluations
func (s *StateStore) Allocs(ws memdb.WatchSet) (memdb.ResultIterator, error) {
	txn := s.db.Txn(false)

	// Walk the entire table
	iter, err := txn.Get("allocs", "id")
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	return iter, nil
}

// LastIndex returns the greatest index value for all indexes
func (s *StateStore) LatestIndex() (uint64, error) {
	indexes, err := s.Indexes()
	if err != nil {
		return 0, err
	}

	var max uint64 = 0
	for {
		raw := indexes.Next()
		if raw == nil {
			break
		}

		// Prepare the request struct
		idx := raw.(*IndexEntry)

		// Determine the max
		if idx.Value > max {
			max = idx.Value
		}
	}

	return max, nil
}

// Index finds the matching index value
func (s *StateStore) Index(name string) (uint64, error) {
	txn := s.db.Txn(false)

	// Lookup the first matching index
	out, err := txn.First("index", "id", name)
	if err != nil {
		return 0, err
	}
	if out == nil {
		return 0, nil
	}
	return out.(*IndexEntry).Value, nil
}

// RemoveIndex is a helper method to remove an index for testing purposes
func (s *StateStore) RemoveIndex(name string) error {
	txn := s.db.Txn(true)
	defer txn.Abort()

	if _, err := txn.DeleteAll("index", "id", name); err != nil {
		return err
	}

	txn.Commit()
	return nil
}

// Indexes returns an iterator over all the indexes
func (s *StateStore) Indexes() (memdb.ResultIterator, error) {
	txn := s.db.Txn(false)

	// Walk the entire nodes table
	iter, err := txn.Get("index", "id")
	if err != nil {
		return nil, err
	}
	return iter, nil
}

// setJobStatuses is a helper for calling setJobStatus on multiple jobs by ID.
// It takes a map of job IDs to an optional forceStatus string. It returns an
// error if the job doesn't exist or setJobStatus fails.
func (s *StateStore) setJobStatuses(index uint64, txn *memdb.Txn,
	jobs map[string]string, evalDelete bool) error {
	for job, forceStatus := range jobs {
		existing, err := txn.First("jobs", "id", job)
		if err != nil {
			return fmt.Errorf("job lookup failed: %v", err)
		}

		if existing == nil {
			continue
		}

		exist := existing.(*models.Job)
		if exist.Status == models.JobStatusPause || exist.Status == models.JobStatusDead {
			continue
		}

		if err := s.setJobStatus(index, txn, existing.(*models.Job), evalDelete, forceStatus); err != nil {
			return err
		}
	}

	return nil
}

// setJobStatus sets the status of the job by looking up associated evaluations
// and allocations. evalDelete should be set to true if setJobStatus is being
// called because an evaluation is being deleted (potentially because of garbage
// collection). If forceStatus is non-empty, the job's status will be set to the
// passed status.
func (s *StateStore) setJobStatus(index uint64, txn *memdb.Txn,
	job *models.Job, evalDelete bool, forceStatus string) error {

	// Capture the current status so we can check if there is a change
	oldStatus := job.Status
	if index == job.CreateIndex {
		oldStatus = ""
	}
	newStatus := forceStatus

	// If forceStatus is not set, compute the jobs status.
	if forceStatus == "" {
		var err error
		newStatus, err = s.getJobStatus(txn, job, evalDelete)
		if err != nil {
			return err
		}
	}

	// Fast-path if nothing has changed.
	if oldStatus == newStatus {
		return nil
	}

	// Copy and update the existing job
	updated := job.Copy()
	updated.Status = newStatus
	updated.ModifyIndex = index

	// Insert the job
	if err := txn.Insert("jobs", updated); err != nil {
		return fmt.Errorf("job insert failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"jobs", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	return nil
}

func (s *StateStore) getJobStatus(txn *memdb.Txn, job *models.Job, evalDelete bool) (string, error) {
	allocs, err := txn.Get("allocs", "job", job.ID)
	if err != nil {
		return "", err
	}

	// If there is a non-terminal allocation, the job is running.
	hasAlloc := false
	for alloc := allocs.Next(); alloc != nil; alloc = allocs.Next() {
		hasAlloc = true
		if !alloc.(*models.Allocation).TerminalStatus() {
			return models.JobStatusRunning, nil
		}
	}

	evals, err := txn.Get("evals", "job_prefix", job.ID)
	if err != nil {
		return "", err
	}

	hasEval := false
	for raw := evals.Next(); raw != nil; raw = evals.Next() {
		e := raw.(*models.Evaluation)

		// Filter non-exact matches
		if e.JobID != job.ID {
			continue
		}

		hasEval = true
		if !e.TerminalStatus() {
			return models.JobStatusPending, nil
		}
	}

	// The job is dead if all the allocations and evals are terminal or if there
	// are no evals because of garbage collection.
	if evalDelete || hasEval || hasAlloc {
		return models.JobStatusComplete, nil
	}

	return models.JobStatusPending, nil
}

// StateSnapshot is used to provide a point-in-time snapshot
type StateSnapshot struct {
	StateStore
}

// StateRestore is used to optimize the performance when
// restoring state by only using a single large transaction
// instead of thousands of sub transactions
type StateRestore struct {
	txn *memdb.Txn
}

// Abort is used to abort the restore operation
func (s *StateRestore) Abort() {
	s.txn.Abort()
}

// Commit is used to commit the restore operation
func (s *StateRestore) Commit() {
	s.txn.Commit()
}

// NodeRestore is used to restore a node
func (r *StateRestore) NodeRestore(node *models.Node) error {
	if err := r.txn.Insert("nodes", node); err != nil {
		return fmt.Errorf("node insert failed: %v", err)
	}
	return nil
}

// JobRestore is used to restore a job
func (r *StateRestore) JobRestore(job *models.Job) error {
	if err := r.txn.Insert("jobs", job); err != nil {
		return fmt.Errorf("job insert failed: %v", err)
	}
	return nil
}

// EvalRestore is used to restore an evaluation
func (r *StateRestore) EvalRestore(eval *models.Evaluation) error {
	if err := r.txn.Insert("evals", eval); err != nil {
		return fmt.Errorf("eval insert failed: %v", err)
	}
	return nil
}

// AllocRestore is used to restore an allocation
func (r *StateRestore) AllocRestore(alloc *models.Allocation) error {
	if err := r.txn.Insert("allocs", alloc); err != nil {
		return fmt.Errorf("alloc insert failed: %v", err)
	}
	return nil
}

// IndexRestore is used to restore an index
func (r *StateRestore) IndexRestore(idx *IndexEntry) error {
	if err := r.txn.Insert("index", idx); err != nil {
		return fmt.Errorf("index insert failed: %v", err)
	}
	return nil
}
