package agent

import (
	"encoding/json"
	"fmt"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/consul"
	"github.com/ngaut/log"

	"udup/plugins"
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

func SetupStore(addrs []string, a *Agent) *Store {
	s, err := libkv.NewStore(store.Backend(backend), addrs, nil)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("Backend config: %v", addrs)

	_, err = s.List(keyspace)
	if err != store.ErrKeyNotFound && err != nil {
		log.Infof("Store backend not reachable: %v", err)
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

	log.Debugf("Setting job: %v; json: %v", job.Name, string(jobJSON))
	if err := s.Client.Put(jobKey, jobJSON, nil); err != nil {
		return err
	}

	return nil
}

func (s *Store) validateJob(job *Job) error {
	/*if job.ParentJob == job.Name {
		return ErrSameParent
	}*/

	return nil
}

// GetJobs returns all jobs
func (s *Store) GetJobs() ([]*Job, error) {
	res, err := s.Client.List(keyspace + "/jobs/")
	if err != nil {
		if err == store.ErrKeyNotFound {
			log.Debug("No jobs found")
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
func (s *Store) GetJobByNode(nodeName string) (*JobResponse, error) {
	res, err := s.Client.List(keyspace + "/jobs/")
	if err != nil {
		if err == store.ErrKeyNotFound {
			log.Debug("No jobs found")
			return &JobResponse{}, nil
		}
		return nil, err
	}

	jobs := &JobResponse{}
	for _, node := range res {
		var job Job
		err := json.Unmarshal([]byte(node.Value), &job)
		if err != nil {
			return nil, err
		}
		if job.Processors[plugins.ProcessorTypeExtract].NodeName != nodeName && job.Processors[plugins.ProcessorTypeApply].NodeName != nodeName {
			continue
		}
		job.Agent = s.agent
		jobs.Payload = append(jobs.Payload, &job)
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
			log.Fatal("Store not reachable, be sure you have an existing key-value store running is running and is reachable.")
		} else if err != store.ErrKeyNotFound {
			log.Error(err)
		}
		return nil
	}

	log.Infof("Retrieved leader from datastore: %v", string(res.Value))

	return res.Value
}

// Retrieve the leader key used in the KV store to store the leader node
func (s *Store) LeaderKey() string {
	return keyspace + "/leader"
}
