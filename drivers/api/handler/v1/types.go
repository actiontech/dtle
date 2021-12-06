package v1

const (
	JobTypeSync = "synchronous"
)

// Job is used to serialize a job.
type OldJob struct {
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

type Task struct {
	Type string
	NodeID string
	NodeName string
	Driver string
	Config map[string]interface{}
}
