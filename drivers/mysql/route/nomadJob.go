package route

import (
	"github.com/hashicorp/nomad/api"
)

// JobUpdateRequest is used to update a job
type NomadJobRegisterRequest struct {
	Job *api.Job
	// If EnforceIndex is set then the job will only be registered if the passed
	// JobModifyIndex matches the current Jobs index. If the index is zero, the
	// register only occurs if the job is new.
	EnforceIndex   bool
	JobModifyIndex uint64
	PolicyOverride bool

	WriteRequest
}
