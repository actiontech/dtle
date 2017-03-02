package agent

import (
	"errors"
	"fmt"
	"sync"

	"github.com/docker/libkv/store"
	"github.com/ngaut/log"

	uconf "udup/config"
)

const (
	Success = iota
	Running
	Failed
	PartialyFailed

	ConcurrencyAllow  = "allow"
	ConcurrencyForbid = "forbid"
)

var (
	ErrParentJobNotFound = errors.New("Specified parent job not found")
	ErrNoAgent           = errors.New("No agent defined")
	ErrSameParent        = errors.New("The job can not have itself as parent")
	ErrNoParent          = errors.New("The job doens't have a parent job set")
	ErrWrongConcurrency  = errors.New("Wrong concurrency policy value, use: allow/forbid")
)

type Job struct {
	// Job name. Must be unique, acts as the id.
	Name string `json:"name"`

	// Node name of the node that run this job.
	NodeName string `json:"node_name,omitempty"`

	// Is this job disabled?
	Disabled bool `json:"disabled"`

	// Pointer to the calling agent.
	Agent *Agent `json:"-"`

	running sync.Mutex

	// Jobs that are dependent upon this one will be run after this job runs.
	DependentJobs []string `json:"dependent_jobs"`

	// Job id of job that this job is dependent upon.
	ParentJob string `json:"parent_job"`

	lock store.Locker

	// If this execution executed succesfully.
	Success bool `json:"success,omitempty"`

	// Processors to use for this job
	Processors map[string]*uconf.DriverConfig `json:"processors"`

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
			log.Infof("Run job:%v", j.Name)
			j.Agent.StartJobQuery(j)
		}
	}
}

func (j *Job) listenOnPanicAbort(cfg *uconf.DriverConfig) {
	err := <-cfg.ErrCh
	log.Errorf("Run failed: %v", err)
	j.Lock()
}

func (j *Job) listenOnGtid(cfg *uconf.DriverConfig) {
	for gtid := range cfg.GtidCh {
		if gtid != "" {
			j.Processors["extract"].Gtid = gtid
			err := j.Agent.store.UpsertJob(j)
			if err != nil {
				log.Errorf(err.Error())
			}
		}
	}
}

// Return the status of a job
// Wherever it's running, succeded or failed
func (j *Job) Status() int {
	// Maybe we are testing
	if j.Agent == nil {
		return -1
	}

	job, _ := j.Agent.store.GetJob(j.Name)
	success := 0
	failed := 0

	var status int
	if job.Success {
		success = success + 1
	} else {
		failed = failed + 1
	}

	if failed == 0 {
		status = Success
	} else if failed > 0 && success == 0 {
		status = Failed
	} else if failed > 0 && success > 0 {
		status = PartialyFailed
	}

	return status
}

// Get the parent job of a job
func (j *Job) GetParent() (*Job, error) {
	// Maybe we are testing
	if j.Agent == nil {
		return nil, ErrNoAgent
	}

	if j.Name == j.ParentJob {
		return nil, ErrSameParent
	}

	if j.ParentJob == "" {
		return nil, ErrNoParent
	}

	parentJob, err := j.Agent.store.GetJob(j.ParentJob)
	if err != nil {
		if err == store.ErrKeyNotFound {
			return nil, ErrParentJobNotFound
		} else {
			return nil, err
		}
	}

	return parentJob, nil
}

// Lock the job in store
func (j *Job) Lock() error {
	// Maybe we are testing
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
	// Maybe we are testing
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
			log.Infof("Skipping execution, job:%v; concurrency:%v; job_status:%v", j.Name, j.Concurrency, status)
			return false
		}
	}

	return true
}
