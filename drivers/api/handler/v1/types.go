package v1

import (
	"github.com/actiontech/dtle/g"
	uuid "github.com/satori/go.uuid"
)

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

func (j *OldJob) Canonicalize() {
	if j.ID == nil {
		j.ID = g.StringToPtr(uuid.NewV4().String())
	}
	if j.Name == nil {
		j.Name = g.StringToPtr(*j.ID)
	}
	if j.Region == nil {
		j.Region = g.StringToPtr("global")
	}
	if len(j.Datacenters) == 0 {
		j.Datacenters = []string{"dc1"}
	}
	if j.Type == nil {
		j.Type = g.StringToPtr(JobTypeSync)
	}
	if j.Status == nil {
		j.Status = g.StringToPtr("")
	}
	if j.StatusDescription == nil {
		j.StatusDescription = g.StringToPtr("")
	}
	if j.CreateIndex == nil {
		j.CreateIndex = g.Uint64ToPtr(0)
	}
	if j.ModifyIndex == nil {
		j.ModifyIndex = g.Uint64ToPtr(0)
	}
	if j.JobModifyIndex == nil {
		j.JobModifyIndex = g.Uint64ToPtr(0)
	}
}

type Task struct {
	Type string
	NodeID string
	NodeName string
	Driver string
	Config map[string]interface{}
}
