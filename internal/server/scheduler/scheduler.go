package scheduler

import (
	"fmt"

	"github.com/hashicorp/go-memdb"

	ulog "udup/internal/logger"
	"udup/internal/models"
)

const (
	// SchedulerVersion is the version of the scheduler. Changes to the
	// scheduler that are incompatible with prior schedulers will increment this
	// version. It is used to disallow dequeueing when the versions do not match
	// across the leader and the dequeueing scheduler.
	SchedulerVersion uint16 = 1
)

// BuiltinSchedulers contains the built in registered schedulers
// which are available
var BuiltinSchedulers = map[string]Factory{
	models.JobTypeSync: NewGenericScheduler,
	models.JobTypeMig:  NewGenericScheduler,
	//models.JobTypeSub:   NewGenericScheduler,
}

// NewScheduler is used to instantiate and return a new scheduler
// given the scheduler name, initial store, and planner.
func NewScheduler(name string, logger *ulog.Logger, state State, planner Planner) (Scheduler, error) {
	// Lookup the factory function
	factory, ok := BuiltinSchedulers[name]
	if !ok {
		return nil, fmt.Errorf("unknown scheduler '%s'", name)
	}

	// Instantiate the scheduler
	sched := factory(logger, state, planner)
	return sched, nil
}

// Factory is used to instantiate a new Scheduler
type Factory func(*ulog.Logger, State, Planner) Scheduler

// Scheduler is the top level instance for a scheduler. A scheduler is
// meant to only encapsulate business logic, pushing the various plumbing
// into Udup itself. They are invoked to process a single evaluation at
// a time. The evaluation may result in task allocations which are computed
// optimistically, as there are many concurrent evaluations being processed.
// The task allocations are submitted as a plan, and the current leader will
// coordinate the commmits to prevent oversubscription or improper allocations
// based on stale store.
type Scheduler interface {
	// Process is used to handle a new evaluation. The scheduler is free to
	// apply any logic necessary to make the task placements. The store and
	// planner will be provided prior to any invocations of process.
	Process(*models.Evaluation) error
}

// State is an immutable view of the global store. This allows schedulers
// to make intelligent decisions based on allocations of other schedulers
// and to enforce complex constraints that require more information than
// is available to a local store scheduler.
type State interface {
	// Nodes returns an iterator over all the nodes.
	// The type of each result is *models.Node
	Nodes(ws memdb.WatchSet) (memdb.ResultIterator, error)

	// AllocsByJob returns the allocations by JobID
	AllocsByJob(ws memdb.WatchSet, jobID string, all bool) ([]*models.Allocation, error)

	// AllocsByNode returns all the allocations by node
	AllocsByNode(ws memdb.WatchSet, node string) ([]*models.Allocation, error)

	// AllocsByNodeTerminal returns all the allocations by node filtering by terminal status
	AllocsByNodeTerminal(ws memdb.WatchSet, node string, terminal bool) ([]*models.Allocation, error)

	// GetNodeByID is used to lookup a node by ID
	NodeByID(ws memdb.WatchSet, nodeID string) (*models.Node, error)

	// GetJobByID is used to lookup a job by ID
	JobByID(ws memdb.WatchSet, id string) (*models.Job, error)
}

// Planner interface is used to submit a task allocation plan.
type Planner interface {
	// SubmitPlan is used to submit a plan for consideration.
	// This will return a PlanResult or an error. It is possible
	// that this will result in a store refresh as well.
	SubmitPlan(*models.Plan) (*models.PlanResult, State, error)

	// UpdateEval is used to update an evaluation. This should update
	// a copy of the input evaluation since that should be immutable.
	UpdateEval(*models.Evaluation) error

	// CreateEval is used to create an evaluation. This should set the
	// PreviousEval to that of the current evaluation.
	CreateEval(*models.Evaluation) error

	// ReblockEval takes a blocked evaluation and re-inserts it into the blocked
	// evaluation tracker. This update occurs only in-memory on the leader. The
	// evaluation must exist in a blocked store prior to this being called such
	// that on leader changes, the evaluation will be reblocked properly.
	ReblockEval(*models.Evaluation) error
}
