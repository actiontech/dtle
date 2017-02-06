package agent

import "sync"

type Job struct {
	// Job name. Must be unique, acts as the id.
	Name string `json:"name"`

	Driver string

	Type string

	// Pointer to the calling agent.
	Agent *Agent `json:"-"`

	running sync.Mutex
}

// Run the job
func (j *Job) Run() {

}
