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
func (j *Orders) Deregister(orderID string, q *WriteOptions) (string, *WriteMeta, error) {
	var resp deregisterJobResponse
	wm, err := j.client.delete("/v1/order/"+orderID, &resp, q)
	if err != nil {
		return "", nil, err
	}
	return resp.EvalID, wm, nil
}

type Order struct {
	Region                *string
	JobID                 string
	ID                    string
	SkuId                 string
	TrafficAgainstLimits  *uint64
	TotalTransferredBytes *uint64
	Status                *string
	EnforceIndex          bool
	CreateIndex           *uint64
	ModifyIndex           *uint64
	OrderModifyIndex      *uint64
}

func (o *Order) Canonicalize() {
	if o.ID == "" {
		o.ID = *internal.StringToPtr(models.GenerateUUID())
	}
	if o.Region == nil {
		o.Region = internal.StringToPtr("global")
	}
	if o.Status == nil {
		o.Status = internal.StringToPtr(models.OrderStatusPending)
	}
	if o.TrafficAgainstLimits == nil {
		o.TrafficAgainstLimits = internal.Uint64ToPtr(0)
	}
	if o.TotalTransferredBytes == nil {
		o.TotalTransferredBytes = internal.Uint64ToPtr(0)
	}
	if o.CreateIndex == nil {
		o.CreateIndex = internal.Uint64ToPtr(0)
	}
	if o.ModifyIndex == nil {
		o.ModifyIndex = internal.Uint64ToPtr(0)
	}
	if o.OrderModifyIndex == nil {
		o.OrderModifyIndex = internal.Uint64ToPtr(0)
	}
}

// JobIDSort is used to sort jobs by their job ID's.
type OrderIDSort []*JobListStub

func (o OrderIDSort) Len() int {
	return len(o)
}

func (o OrderIDSort) Less(a, b int) bool {
	return o[a].ID < o[b].ID
}

func (o OrderIDSort) Swap(a, b int) {
	o[a], o[b] = o[b], o[a]
}

// RegisterJobRequest is used to serialize a job registration
type RegisterOrderRequest struct {
	Order          *Order
	EnforceIndex   bool   `json:",omitempty"`
	JobModifyIndex uint64 `json:",omitempty"`
}
