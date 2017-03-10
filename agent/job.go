package agent

import (
	"errors"
	"fmt"
	"sync"

	"github.com/docker/libkv/store"
	"github.com/ngaut/log"

	uconf "udup/config"
)

var (
	ErrParentJobNotFound = errors.New("Specified parent job not found")
	ErrNoAgent           = errors.New("No agent defined")
	ErrSameParent        = errors.New("The job can not have itself as parent")
	ErrNoParent          = errors.New("The job doens't have a parent job set")
)
// JobStatus is the status of the Job.
type JobStatus int

const (
	Running JobStatus = iota
	Queued
	Stopped
	Failed
)

func (s JobStatus) String() string {
	switch s {
	case Running:
		return "running"
	case Queued:
		return "queued"
	case Stopped:
		return "stopped"
	case Failed:
		return "failed"
	default:
		return "unknown"
	}
}

type Job struct {
	// Job name. Must be unique, acts as the id.
	Name string `json:"name"`

	// Node name of the node that run this job.
	NodeName string `json:"node_name,omitempty"`

	// Job status
	Status JobStatus `json:"status"`

	// Pointer to the calling agent.
	Agent *Agent `json:"-"`

	running sync.Mutex

	// Jobs that are dependent upon this one will be run after this job runs.
	DependentJobs []string `json:"dependent_jobs"`

	// Job id of job that this job is dependent upon.
	ParentJob string `json:"parent_job"`

	lock store.Locker

	// Processors to use for this job
	Processors map[string]*uconf.DriverConfig `json:"processors"`
}

// Start the job
func (j *Job) Start(restart bool) {
	j.running.Lock()
	defer j.running.Unlock()

	if j.Agent != nil {
		if j.Status == Stopped || restart{
			log.Infof("Start job:%v", j.Name)
			j.Agent.StartJobQuery(j)
		}
	}
}

// Stop the job
func (j *Job) Stop() {
	j.running.Lock()
	defer j.running.Unlock()

	if j.Agent != nil && j.Status == Running {
		log.Infof("Stop job:%v", j.Name)
		j.Agent.StopJobQuery(j)
	}
}

func (j *Job) listenOnPanicAbort(cfg *uconf.DriverConfig) {
	err := <-cfg.ErrCh
	log.Errorf("Run failed: %v", err)
	j.Stop()
}

func (j *Job) listenOnGtid(cfg *uconf.DriverConfig) {
	for gtid := range cfg.GtidCh {
		j.Processors["apply"].Gtid = gtid
		err := j.Agent.store.UpsertJob(j)
		if err != nil {
			log.Errorf(err.Error())
		}
	}
}

// Get the parent job of a job
func (j *Job) GetParent() (*Job, error) {
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
