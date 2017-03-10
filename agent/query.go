package agent

import (
	"encoding/json"
	"math/rand"

	"github.com/docker/libkv/store"
	"github.com/hashicorp/serf/serf"
	"github.com/ngaut/log"
	"fmt"
)

const (
	QuerySchedulerRestart = "scheduler:restart"
	QueryStartJob         = "start:job"
	QueryStopJob          = "stop:job"
	QueryRPCConfig        = "rpc:config"
)

type RunQueryParam struct {
	Job     *Job   `json:"job"`
	RPCAddr string `json:"rpc_addr"`
}

// Send a serf run query to the cluster, this is used to ask a node or nodes
// to run a Job.
func (a *Agent) StartJobQuery(j *Job) error{
	var params *serf.QueryParam

	job, err := a.store.GetJob(j.Name)

	//Job can be removed and the QuerySchedulerRestart not yet received.
	//In this case, the job will not be found in the store.
	if err == store.ErrKeyNotFound {
		log.Infof("Job not found, cancelling this execution")
		return err
	}

	var statusAlive bool
	for _, m := range a.serf.Members() {
		if m.Name == job.NodeName && m.Status == serf.StatusAlive {
			statusAlive = true
		}
	}
	if !statusAlive {
		return ErrMemberNotFound
	}

	// Jobs that have dependent jobs are a bit more expensive because we need to call the Status() method for every execution.
	// Check first if there's dependent jobs and then check for the job status to begin executiong dependent jobs on success.
	if job.ParentJob != "" {
		pj, err := a.store.GetJob(job.ParentJob)
		if err != nil {
			return fmt.Errorf("failed to get parent job '%s': %v",
				job.ParentJob, err)
		}

		statusAlive = false
		for _, m := range a.serf.Members() {
			if m.Name == pj.NodeName && m.Status == serf.StatusAlive {
				statusAlive = true
			}
		}
		if !statusAlive {
			return ErrMemberNotFound
		}

		pj.Start(false)
	}

	params = &serf.QueryParam{
		FilterNodes: []string{j.NodeName},
		RequestAck:  true,
	}

	rqp := &RunQueryParam{
		Job:     j,
		RPCAddr: a.getRPCAddr(),
	}
	rqpJson, _ := json.Marshal(rqp)

	log.Debug("Sending query:%v; job_name:%v; json:%v", QueryStartJob, job.Name, string(rqpJson))

	qr, err := a.serf.Query(QueryStartJob, rqpJson, params)
	if err != nil {
		log.Errorf("Sending query error:%v; query:%v", err, QueryStartJob)
		return err
	}
	defer qr.Close()

	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				log.Debug("Received ack:%v; from:%v", QueryStartJob, ack)
			}
		case resp, ok := <-respCh:
			if ok {
				log.Debug("Received response:%v; query:%v; from:%v", string(resp.Payload), QueryStartJob, resp.From)
				a.upsertJob(resp.Payload)
			}
		}
	}
	log.Infof("Done receiving acks and responses:%v", QueryStartJob)
	return nil
}

func (a *Agent) StopJobQuery(j *Job) error{
	var params *serf.QueryParam

	job, err := a.store.GetJob(j.Name)

	//Job can be removed and the QuerySchedulerRestart not yet received.
	//In this case, the job will not be found in the store.
	if err == store.ErrKeyNotFound {
		log.Infof("Job not found, cancelling this execution")
		return err
	}

	var statusAlive bool
	for _, m := range a.serf.Members() {
		if m.Name == job.NodeName && m.Status == serf.StatusAlive {
			statusAlive = true
		}
	}
	if !statusAlive {
		// Lock the job while editing
		if err = job.Lock(); err != nil {
			log.Fatal(err)
		}

		job.Status = Queued
		for _, p := range job.Processors {
			p.Running = false
		}
		if err := a.store.UpsertJob(job); err != nil {
			log.Fatal(err)
		}

		// Release the lock
		if err = job.Unlock(); err != nil {
			log.Fatal(err)
		}
		return ErrMemberNotFound
	}

	// Jobs that have dependent jobs are a bit more expensive because we need to call the Status() method for every execution.
	// Check first if there's dependent jobs and then check for the job status to begin executiong dependent jobs on success.
	if len(job.DependentJobs) > 0 {
		for _, djn := range job.DependentJobs {
			dj, err := a.store.GetJob(djn)
			if err != nil {
				return err
			}
			statusAlive = false
			for _, m := range a.serf.Members() {
				if m.Name == dj.NodeName && m.Status == serf.StatusAlive {
					statusAlive = true
				}
			}
			if !statusAlive {
				return ErrMemberNotFound
			}
			dj.Stop()
		}
	}

	params = &serf.QueryParam{
		FilterNodes: []string{j.NodeName},
		RequestAck:  true,
	}

	rqp := &RunQueryParam{
		Job:     j,
		RPCAddr: a.getRPCAddr(),
	}
	rqpJson, _ := json.Marshal(rqp)

	log.Debug("Sending query:%v; job_name:%v; json:%v", QueryStopJob, job.Name, string(rqpJson))

	qr, err := a.serf.Query(QueryStopJob, rqpJson, params)
	if err != nil {
		log.Errorf("Sending query error:%v; query:%v", err, QueryStopJob)
	}
	defer qr.Close()

	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				log.Debug("Received ack:%v; from:%v", QueryStopJob, ack)
			}
		case resp, ok := <-respCh:
			if ok {
				log.Debug("Received response:%v; query:%v; from:%v", string(resp.Payload), QueryStopJob, resp.From)
				a.upsertJob(resp.Payload)
			}
		}
	}
	log.Infof("Done receiving acks and responses:%v", QueryStopJob)
	return nil
}

func (a *Agent) upsertJob(payload []byte) *Job {
	var ex Job
	if err := json.Unmarshal(payload, &ex); err != nil {
		log.Fatal(err)
	}

	// Save the new execution to store
	if err := a.store.UpsertJob(&ex); err != nil {
		log.Fatal(err)
	}

	return &ex
}

// Broadcast a query to get the RPC config of one udup_server, any that could
// attend later RPC calls.
func (a *Agent) queryRPCConfig(nodeName string) ([]byte, error) {
	if nodeName == "" {
		nodeName = a.selectServer().Name
	}

	params := &serf.QueryParam{
		FilterNodes: []string{nodeName},
		FilterTags:  map[string]string{"udup_server": "true"},
		RequestAck:  true,
	}

	qr, err := a.serf.Query(QueryRPCConfig, nil, params)
	if err != nil {
		log.Errorf("Error sending query:%v; query:%v", err, QueryRPCConfig)
		return nil, err
	}
	defer qr.Close()

	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	var rpcAddr []byte
	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				log.Infof("Received ack:%v; from:%v", QueryRPCConfig, ack)
			}
		case resp, ok := <-respCh:
			if ok {
				log.Infof("Received response:%v; from:%v; payload:%v", QueryRPCConfig, resp.From, string(resp.Payload))
				rpcAddr = resp.Payload
			}
		}
	}
	log.Infof("Done receiving acks and responses:%v", QueryRPCConfig)

	return rpcAddr, nil
}

func (a *Agent) selectServer() serf.Member {
	servers := a.listServers()
	server := servers[rand.Intn(len(servers))]

	return server
}
