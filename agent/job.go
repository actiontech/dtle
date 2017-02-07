package agent

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/docker/libkv/store"
	"github.com/ngaut/log"
)

const (
	Success = iota
	Running
	Failed

	ConcurrencyAllow  = "allow"
	ConcurrencyForbid = "forbid"
)

var (
	ErrNoAgent           = errors.New("No agent defined")
)

type Job struct {
	// Job name. Must be unique, acts as the id.
	Name string `json:"name"`

	Driver string `json:"driver"`

	Type string `json:"type"`

	// Last time this job executed succesful.
	LastSuccess time.Time `json:"last_success"`

	// Last time this job failed.
	LastError time.Time `json:"last_error"`

	// Is this job disabled?
	Disabled bool `json:"disabled"`

	// Pointer to the calling agent.
	Agent *Agent `json:"-"`

	// Number of times to retry a job that failed an execution.
	Retries uint `json:"retries"`

	running sync.Mutex

	// Jobs that are dependent upon this one will be run after this job runs.
	DependentJobs []string `json:"dependent_jobs"`

	// Job id of job that this job is dependent upon.
	ParentJob string `json:"parent_job"`

	lock store.Locker

	// If this execution executed succesfully.
	Success bool `json:"success,omitempty"`

	// Concurrency policy for this job (allow, forbid)
	Concurrency string `json:"concurrency"`
}

// Run the job
func (j *Job) Run() {
	j.running.Lock()
	defer j.running.Unlock()

	if j.Agent != nil && j.Disabled == false {
		// Check if it's runnable
		if j.isRunnable() {
			log.Infof("job:%v,scheduler: Run job", j.Name)
			j.Agent.RunQuery(j)
		}
	}
}

// Return the status of a job
// Wherever it's running, succeded or failed
func (j *Job) Status() int {
	if j.Agent == nil {
		return -1
	}
	var status int
	job, _ := j.Agent.store.JobByName(j.Name)
	if job.Success{
		status = Success
	} else{
		status = Failed
	}
	return status
}

// Get the parent job of a job
func (j *Job) GetParent() (*Job, error) {
	if j.Agent == nil {
		return nil, ErrNoAgent
	}

	if j.Name == j.ParentJob {
		return nil, fmt.Errorf("The job can not have itself as parent")
	}

	if j.ParentJob == "" {
		return nil, fmt.Errorf("The job doens't have a parent job set")
	}

	parentJob, err := j.Agent.store.JobByName(j.ParentJob)
	if err != nil {
		if err == store.ErrKeyNotFound {
			return nil, fmt.Errorf("Specified parent job not found")
		} else {
			return nil, err
		}
	}

	return parentJob, nil
}

// Lock the job in store
func (j *Job) Lock() error {
	if j.Agent == nil {
		return ErrNoAgent
	}

	lockKey := fmt.Sprintf("%s/job_locks/%s", keyspace, j.Name)
	// TODO: LockOptions empty is a temporary fix until https://github.com/docker/libkv/pull/99 is fixed
	l, err := j.Agent.store.Client.NewLock(lockKey, &store.LockOptions{RenewLock: make(chan (struct{}))})
	if err != nil {
		return err
	}
	j.lock = l

	_, err = j.lock.Lock(nil)
	if err != nil {
		return err
	}

	return nil
}

// Unlock the job in store
func (j *Job) Unlock() error {
	if j.Agent == nil {
		return ErrNoAgent
	}

	if err := j.lock.Unlock(); err != nil {
		return err
	}

	return nil
}

func (j *Job) isRunnable() bool {
	status := j.Status()

	if status == Running {
		if j.Concurrency == ConcurrencyAllow {
			return true
		} else if j.Concurrency == ConcurrencyForbid {
			log.Infof("job:%v,concurrency:%v,job_status:%v,scheduler: Skipping execution", j.Name, j.Concurrency, status)
			return false
		}
	}

	return true
}
