package agent

import (
	"errors"
	"fmt"
	"sync"

	"github.com/docker/libkv/store"
	"github.com/hashicorp/serf/serf"
	"github.com/ngaut/log"

	uconf "udup/config"
)

var (
	ErrNoAgent = errors.New("No agent defined")
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

	// Job status
	Status JobStatus `json:"status"`

	// Pointer to the calling agent.
	Agent *Agent `json:"-"`

	running sync.Mutex

	lock store.Locker

	// Processors to use for this job
	Processors map[string]*uconf.DriverConfig `json:"processors"`
}

// Start the job
func (j *Job) Start() {
	j.running.Lock()
	defer j.running.Unlock()

	if j.Agent != nil {
		if j.Status != Running {
			for _, m := range j.Agent.serf.Members() {
				for k, v := range j.Processors {
					if m.Name == v.NodeName && m.Status != serf.StatusAlive {
						j.Status = Queued
						j.Processors[k].Running = false
						if err := j.Agent.store.UpsertJob(j); err != nil {
							log.Fatal(err)
						}
						return
					}
				}
			}
			log.Infof("Start job:%v", j.Name)
			for k, v := range j.Processors {
				if v.Running != true {
					go j.Agent.StartJobQuery(j, k)
				}
			}
		}
	}
}

// Stop the job
func (j *Job) Stop() {
	j.running.Lock()
	defer j.running.Unlock()

	if j.Agent != nil && j.Status == Running {
		log.Infof("Stop job:%v", j.Name)
		for k, _ := range j.Processors {
			go j.Agent.StopJobQuery(j, k)
		}
	}
}

// Enqueue the job
func (j *Job) Enqueue() {
	j.running.Lock()
	defer j.running.Unlock()

	if j.Agent != nil && j.Status == Running {
		log.Infof("Enqueue job:%v", j.Name)
		j.Agent.EnqueueJobQuery(j)
	}
}

func (j *Job) listenOnPanicAbort(cfg *uconf.DriverConfig) {
	err := <-cfg.ErrCh
	log.Errorf("Run failed: %v", err)
	j.Enqueue()
}

func (j *Job) listenOnGtid(a *Agent, cfg *uconf.DriverConfig) {
	for gtid := range cfg.GtidCh {
		j.Processors["apply"].Gtid = gtid
		err := a.store.UpsertJob(j)
		if err != nil {
			log.Errorf(err.Error())
		}
	}
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
