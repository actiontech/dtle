package route

import "time"

const (
	JobTypeSync = "synchronous"
)

// Job is used to serialize a job.
type Job struct {
	Region            *string
	ID                *string
	Orders            []string
	Name              *string
	Failover          bool
	Type              *string
	Datacenters       []string
	Tasks             []*Task
	Status            *string
	StatusDescription *string
	EnforceIndex      bool
	CreateIndex       *uint64
	ModifyIndex       *uint64
	JobModifyIndex    *uint64
}

func (j *Job) Canonicalize() {
	if j.ID == nil {
		j.ID = StringToPtr(GenerateUUID())
	}
	if j.Name == nil {
		j.Name = StringToPtr(*j.ID)
	}
	if j.Region == nil {
		j.Region = StringToPtr("global")
	}
	if len(j.Datacenters) == 0 {
		j.Datacenters = []string{"dc1"}
	}
	if j.Type == nil {
		j.Type = StringToPtr(JobTypeSync)
	}
	if j.Status == nil {
		j.Status = StringToPtr("")
	}
	if j.StatusDescription == nil {
		j.StatusDescription = StringToPtr("")
	}
	if j.CreateIndex == nil {
		j.CreateIndex = Uint64ToPtr(0)
	}
	if j.ModifyIndex == nil {
		j.ModifyIndex = Uint64ToPtr(0)
	}
	if j.JobModifyIndex == nil {
		j.JobModifyIndex = Uint64ToPtr(0)
	}
}

// JobListStub is used to return a subset of information about
// jobs during list operations.
type JobListStub struct {
	ID                string
	Name              string
	Type              string
	Status            string
	StatusDescription string
	JobSummary        *Job
	CreateIndex       uint64
	ModifyIndex       uint64
	JobModifyIndex    uint64
}

// JobIDSort is used to sort jobs by their job ID's.
type JobIDSort []*JobListStub

func (j JobIDSort) Len() int {
	return len(j)
}

func (j JobIDSort) Less(a, b int) bool {
	return j[a].ID < j[b].ID
}

func (j JobIDSort) Swap(a, b int) {
	j[a], j[b] = j[b], j[a]
}

// AddDatacenter is used to add a datacenter to a job.
func (j *Job) AddDatacenter(dc string) *Job {
	j.Datacenters = append(j.Datacenters, dc)
	return j
}

// AddTask adds a task to an existing job.
func (j *Job) AddTask(t *Task) *Job {
	j.Tasks = append(j.Tasks, t)
	return j
}

type WriteRequest struct {
	// The target region for this write
	Region string
}

// JobValidateRequest is used to validate a job
type JobValidateRequest struct {
	Job *Job
	WriteRequest
}

// JobValidateResponse is the response from validate request
type JobValidateResponse struct {
	// DriverConfigValidated indicates whether the agent validated the driver
	// config
	DriverConfigValidated bool

	// ValidationErrors is a list of validation errors
	ValidationErrors []string

	// Error is a string version of any error that may have occured
	Error string
}

// JobUpdateRequest is used to update a job
type JobRegisterRequest struct {
	Job *Job
	// If EnforceIndex is set then the job will only be registered if the passed
	// JobModifyIndex matches the current Jobs index. If the index is zero, the
	// register only occurs if the job is new.
	EnforceIndex   bool
	JobModifyIndex uint64

	WriteRequest
}

type JobUpdateStatusRequest struct {
	JobID  string
	Status string
	WriteRequest
}

// JobUpdateResponse is used to respond to a job registration
/*type JobUpdateResponse struct {
	EvalID          string
	EvalCreateIndex uint64
	JobModifyIndex  uint64
	QueryMeta
}
*/
// RegisterJobRequest is used to serialize a job registration
type RegisterJobRequest struct {
	Job            *Job
	EnforceIndex   bool   `json:",omitempty"`
	JobModifyIndex uint64 `json:",omitempty"`
}

type RenewalJobRequest struct {
	Region  *string
	JobID   string
	OrderID string
}

// registerJobResponse is used to deserialize a job response
type registerJobResponse struct {
	EvalID string
}

// deregisterJobResponse is used to decode a deregister response
type deregisterJobResponse struct {
	EvalID string
}

type JobPlanRequest struct {
	Job  *Job
	Diff bool
	WriteRequest
}

/*type JobPlanResponse struct {
	JobModifyIndex uint64
	CreatedEvals   []*Evaluation
	Diff           *JobDiff
	Annotations    *PlanAnnotations
	FailedTGAllocs map[string]*AllocationMetric
}
*/
type JobDiff struct {
	Type    string
	ID      string
	Fields  []*FieldDiff
	Objects []*ObjectDiff
	Tasks   []*TaskDiff
}

type TaskDiff struct {
	Type        string
	Name        string
	Fields      []*FieldDiff
	Objects     []*ObjectDiff
	Annotations []string
	Updates     map[string]uint64
}

type FieldDiff struct {
	Type        string
	Name        string
	Old, New    string
	Annotations []string
}

type ObjectDiff struct {
	Type    string
	Name    string
	Fields  []*FieldDiff
	Objects []*ObjectDiff
}



type NoamdNodeListStub struct {
	Address               string
	ID                    string
	Datacenter            string
	Name                  string
	NodeClass             string
	Version               string
	Drain                 bool
	SchedulingEligibility string
	Status                string
	StatusDescription     string
	Drivers               map[string]*DriverInfo
	CreateIndex           uint64
	ModifyIndex           uint64
}
// regularly as driver health changes on the node.
type DriverInfo struct {
	Attributes        map[string]string
	Detected          bool
	Healthy           bool
	HealthDescription string
	UpdateTime        time.Time
}

type NodeListStub struct {
	ID                string
	Datacenter        string
	Name              string
	HTTPAddr          string
	Status            string
	StatusDescription string
	CreateIndex       uint64
	ModifyIndex       uint64
}
