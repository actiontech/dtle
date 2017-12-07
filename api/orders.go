package api

import (
	"sort"

	"udup/internal"
	"udup/internal/models"
)

type Orders struct {
	client *Client
}

// Jobs returns a handle on the jobs endpoints.
func (c *Client) Orders() *Orders {
	return &Orders{client: c}
}

// Register is used to register a new job. It returns the ID
// of the evaluation, along with any errors encountered.
func (j *Orders) Register(job *Job, q *WriteOptions) (string, *WriteMeta, error) {

	var resp registerJobResponse

	req := &RegisterJobRequest{Job: job}
	wm, err := j.client.write("/v1/orders", req, &resp, q)
	if err != nil {
		return "", nil, err
	}
	return resp.EvalID, wm, nil
}

// EnforceRegister is used to register a job enforcing its job modify index.
func (j *Orders) EnforceRegister(job *Job, modifyIndex uint64, q *WriteOptions) (string, *WriteMeta, error) {

	var resp registerJobResponse

	req := &RegisterJobRequest{
		Job:            job,
		EnforceIndex:   true,
		JobModifyIndex: modifyIndex,
	}
	wm, err := j.client.write("/v1/orders", req, &resp, q)
	if err != nil {
		return "", nil, err
	}
	return resp.EvalID, wm, nil
}

// List is used to list all of the existing jobs.
func (j *Orders) List(q *QueryOptions) ([]*JobListStub, *QueryMeta, error) {
	var resp []*JobListStub
	qm, err := j.client.query("/v1/orders", &resp, q)
	if err != nil {
		return nil, qm, err
	}
	sort.Sort(JobIDSort(resp))
	return resp, qm, nil
}

// PrefixList is used to list all existing jobs that match the prefix.
func (j *Orders) PrefixList(prefix string) ([]*JobListStub, *QueryMeta, error) {
	return j.List(&QueryOptions{Prefix: prefix})
}

// Deregister is used to remove an existing job.
func (j *Orders) Deregister(jobID string, q *WriteOptions) (string, *WriteMeta, error) {
	var resp deregisterJobResponse
	wm, err := j.client.delete("/v1/order/"+jobID, &resp, q)
	if err != nil {
		return "", nil, err
	}
	return resp.EvalID, wm, nil
}

type Order struct {
	Region           *string
	ID               *string
	Name             *string
	TrafficLimit     *uint64
	EnforceIndex     bool
	CreateIndex      *uint64
	ModifyIndex      *uint64
	OrderModifyIndex *uint64
}

func (j *Order) Canonicalize() {
	if j.ID == nil {
		j.ID = internal.StringToPtr(models.GenerateUUID())
	}
	if j.Name == nil {
		j.Name = internal.StringToPtr(*j.ID)
	}
	if j.Region == nil {
		j.Region = internal.StringToPtr("global")
	}
	if j.CreateIndex == nil {
		j.CreateIndex = internal.Uint64ToPtr(0)
	}
	if j.ModifyIndex == nil {
		j.ModifyIndex = internal.Uint64ToPtr(0)
	}
	if j.OrderModifyIndex == nil {
		j.OrderModifyIndex = internal.Uint64ToPtr(0)
	}
}

// JobIDSort is used to sort jobs by their job ID's.
type OrderIDSort []*JobListStub

func (j OrderIDSort) Len() int {
	return len(j)
}

func (j OrderIDSort) Less(a, b int) bool {
	return j[a].ID < j[b].ID
}

func (j OrderIDSort) Swap(a, b int) {
	j[a], j[b] = j[b], j[a]
}

// JobUpdateRequest is used to update a job
type OrderRegisterRequest struct {
	Order *Order
	// If EnforceIndex is set then the job will only be registered if the passed
	// JobModifyIndex matches the current Jobs index. If the index is zero, the
	// register only occurs if the job is new.
	EnforceIndex   bool
	JobModifyIndex uint64

	WriteRequest
}

// RegisterJobRequest is used to serialize a job registration
type RegisterOrderRequest struct {
	Order          *Order
	EnforceIndex   bool   `json:",omitempty"`
	JobModifyIndex uint64 `json:",omitempty"`
}

// registerJobResponse is used to deserialize a job response
type registerOrderResponse struct {
	EvalID string
}

// deregisterJobResponse is used to decode a deregister response
type deregisterOrderResponse struct {
	EvalID string
}

type OrderPlanRequest struct {
	Order *Order
	Diff  bool
	WriteRequest
}

type OrderPlanResponse struct {
	JobModifyIndex uint64
	CreatedEvals   []*Evaluation
	Diff           *JobDiff
	Annotations    *PlanAnnotations
	FailedTGAllocs map[string]*AllocationMetric
}

type OrderDiff struct {
	Type    string
	ID      string
	Fields  []*FieldDiff
	Objects []*ObjectDiff
	Tasks   []*TaskDiff
}
