package agent

import (
	"encoding/json"
	"fmt"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/consul"
	"github.com/ngaut/log"
)

const (
	backend  = "consul"
	keyspace = "udup"
)

type Store struct {
	Client store.Store
	agent  *Agent
}

func init() {
	consul.Register()
}

func NewStore(addrs []string, a *Agent) *Store {
	s, err := libkv.NewStore(store.Backend(backend), addrs, nil)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("store: Backend config: %v", addrs)

	_, err = s.List(keyspace)
	if err != store.ErrKeyNotFound && err != nil {
		log.Infof("store: Store backend not reachable: %v", err)
	}

	return &Store{Client: s, agent: a}
}

// Store a job
func (s *Store) UpsertJob(job *Job) error {
	jobKey := fmt.Sprintf("%s/jobs/%s", keyspace, job.Name)

	// Init the job agent
	job.Agent = s.agent

	if err := s.validateJob(job); err != nil {
		return err
	}

	// Get if the requested job already exist
	ej, err := s.GetJob(job.Name)
	if err != nil && err != store.ErrKeyNotFound {
		return err
	}
	if ej != nil {
		// When the job runs, these status vars are updated
		// otherwise use the ones that are stored
	}

	jobJSON, err := json.Marshal(job)
	if err != nil {
		return err
	}

	log.Debugf("store: Setting job: %v; json: %v", job.Name, string(jobJSON))
	if err := s.Client.Put(jobKey, jobJSON, nil); err != nil {
		return err
	}

	return nil
}

// Set the depencency tree for a job given the job and the previous version
// of the Job or nil if it's new.
func (s *Store) UpsertJobDependencyTree(job *Job, previousJob *Job) error {
	// Existing job that doesn't have parent job set and it's being set
	if previousJob != nil && previousJob.ParentJob == "" && job.ParentJob != "" {
		pj, err := job.GetParent()
		if err != nil {
			return err
		}
		pj.Lock()
		defer pj.Unlock()

		pj.DependentJobs = append(pj.DependentJobs, job.Name)
		if err := s.UpsertJob(pj); err != nil {
			return err
		}
	}

	// Existing job that has parent job set and it's being removed
	if previousJob != nil && previousJob.ParentJob != "" && job.ParentJob == "" {
		pj, err := previousJob.GetParent()
		if err != nil {
			return err
		}
		pj.Lock()
		defer pj.Unlock()

		ndx := 0
		for i, djn := range pj.DependentJobs {
			if djn == job.Name {
				ndx = i
				break
			}
		}
		pj.DependentJobs = append(pj.DependentJobs[:ndx], pj.DependentJobs[ndx+1:]...)
		if err := s.UpsertJob(pj); err != nil {
			return err
		}
	}

	// New job that has parent job set
	if previousJob == nil && job.ParentJob != "" {
		pj, err := job.GetParent()
		if err != nil {
			return err
		}
		pj.Lock()
		defer pj.Unlock()

		pj.DependentJobs = append(pj.DependentJobs, job.Name)
		if err := s.UpsertJob(pj); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) validateJob(job *Job) error {
	if job.ParentJob == job.Name {
		return ErrSameParent
	}

	if job.Concurrency != ConcurrencyAllow && job.Concurrency != ConcurrencyForbid && job.Concurrency != "" {
		return ErrWrongConcurrency
	}

	return nil
}

// GetJobs returns all jobs
func (s *Store) GetJobs() ([]*Job, error) {
	res, err := s.Client.List(keyspace + "/jobs/")
	if err != nil {
		if err == store.ErrKeyNotFound {
			log.Debug("store: No jobs found")
			return []*Job{}, nil
		}
		return nil, err
	}

	jobs := make([]*Job, 0)
	for _, node := range res {
		var job Job
		err := json.Unmarshal([]byte(node.Value), &job)
		if err != nil {
			return nil, err
		}
		job.Agent = s.agent
		jobs = append(jobs, &job)
	}
	return jobs, nil
}

// Get a job
func (s *Store) GetJob(name string) (*Job, error) {
	res, err := s.Client.Get(keyspace + "/jobs/" + name)
	if err != nil {
		return nil, err
	}

	var job Job
	if err = json.Unmarshal([]byte(res.Value), &job); err != nil {
		return nil, err
	}

	job.Agent = s.agent
	return &job, nil
}

func (s *Store) DeleteJob(name string) (*Job, error) {
	job, err := s.GetJob(name)
	if err != nil {
		return nil, err
	}

	if err := s.Client.Delete(keyspace + "/jobs/" + name); err != nil {
		return nil, err
	}

	return job, nil
}

// Retrieve the leader from the store
func (s *Store) GetLeader() []byte {
	res, err := s.Client.Get(s.LeaderKey())
	if err != nil {
		if err == store.ErrNotReachable {
			log.Fatal("store: Store not reachable, be sure you have an existing key-value store running is running and is reachable.")
		} else if err != store.ErrKeyNotFound {
			log.Error(err)
		}
		return nil
	}

	log.Infof("store: Retrieved leader from datastore: %v", string(res.Value))

	return res.Value
}

// Retrieve the leader key used in the KV store to store the leader node
func (s *Store) LeaderKey() string {
	return keyspace + "/leader"
}
